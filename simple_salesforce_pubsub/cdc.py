"""Change Data Capture helpers

A Change Data Capture event does not name the fields that changed. Its
``ChangeEventHeader`` reports them as bitmaps over the event schema's own field
list, which is only meaningful once expanded against that schema:

``"0x0A"``
    A hexadecimal bitmap of the top level fields, least significant bit first,
    so bit *n* stands for the *n*-th field of the event record.

``"3-0x01"``
    A compound field: the number is the position of the parent field in the
    event record, and the bitmap that follows covers the fields of the nested
    record. Expanded names are dotted, such as ``BillingAddress.Street``.

:func:`expand_change_event_header` rewrites all three bitmap lists of an event
in place; the client applies it for you when
:obj:`~simple_salesforce_pubsub.SalesforcePubSubClient` is created with
``expand_change_event_header=True``.
"""

import logging
from typing import Any

from .exceptions import SchemaError

#: Name of the header field every Change Data Capture event carries
CHANGE_EVENT_HEADER = "ChangeEventHeader"
#: Header entries holding bitmaps rather than field names
BITMAP_FIELDS = ("changedFields", "diffFields", "nulledFields")

LOGGER = logging.getLogger(__name__)


def _resolve(schema: dict[str, Any], type_definition: Any) -> Any:
    """Resolve *type_definition* to a definition, following named references

    ``fastavro`` writes the second use of a named type as its name alone, so
    the definition has to be looked up in the parsed schema.
    """
    if isinstance(type_definition, str):
        named = schema.get("__named_schemas") or {}
        return named.get(type_definition, type_definition)
    return type_definition


def _record_fields(
    schema: dict[str, Any], type_definition: Any
) -> list[dict[str, Any]] | None:
    """Return the fields of the record *type_definition* denotes

    A compound field is usually nullable, so the record is wrapped in a union.

    :return: The record's fields, or ``None`` if this is not a record
    """
    resolved = _resolve(schema, type_definition)
    candidates = resolved if isinstance(resolved, list) else [resolved]
    for candidate in candidates:
        definition = _resolve(schema, candidate)
        if isinstance(definition, dict) and definition.get("type") == "record":
            return definition["fields"]
    return None


def expand_bitmap(fields: list[dict[str, Any]], bitmap: str) -> list[str]:
    """Return the names of the *fields* selected by *bitmap*

    :param fields: The field list of the record the bitmap covers
    :param bitmap: A ``0x``-prefixed hexadecimal bitmap
    :return: The selected field names, in schema order
    :raise SchemaError: If *bitmap* is not valid hexadecimal, or selects a \
    field beyond the end of *fields*
    """
    try:
        bits = int(bitmap, 16)
    except ValueError as error:
        raise SchemaError(f"Malformed change event bitmap: {bitmap!r}") from error
    if bits >> len(fields):
        raise SchemaError(
            f"Change event bitmap {bitmap!r} selects a field beyond the "
            f"{len(fields)} fields of the schema it was decoded with."
        )
    return [field["name"] for index, field in enumerate(fields) if bits >> index & 1]


def expand_bitmap_fields(schema: dict[str, Any], bitmap_fields: list[str]) -> list[str]:
    """Expand one ``ChangeEventHeader`` bitmap list into field names

    :param schema: The parsed event schema, as :func:`fastavro.parse_schema` \
    returns it
    :param bitmap_fields: The raw entries of a header bitmap list
    :return: The field names, with compound fields dotted
    :raise SchemaError: If an entry cannot be expanded against *schema*
    """
    fields = schema["fields"]
    names: list[str] = []
    for entry in bitmap_fields:
        if "-" not in entry:
            names.extend(expand_bitmap(fields, entry))
            continue
        position, _, nested_bitmap = entry.partition("-")
        try:
            parent = fields[int(position)]
        except (ValueError, IndexError) as error:
            raise SchemaError(
                f"Change event bitmap {entry!r} names field {position!r}, which "
                f"is not a position in a schema of {len(fields)} fields."
            ) from error
        nested_fields = _record_fields(schema, parent["type"])
        if nested_fields is None:
            # not a compound field after all, so there is nothing to expand
            LOGGER.debug("Ignoring nested bitmap %r on a non-record field.", entry)
            continue
        names.extend(
            f"{parent['name']}.{name}"
            for name in expand_bitmap(nested_fields, nested_bitmap)
        )
    return names


def expand_change_event_header(schema: dict[str, Any], payload: dict[str, Any]) -> bool:
    """Replace the bitmap lists of *payload* with field names, in place

    Leaves a payload without a ``ChangeEventHeader`` untouched, so it is safe
    to call for events of any topic.

    :param schema: The parsed event schema the payload was decoded with
    :param payload: A decoded event payload
    :return: Whether the payload carried a header to expand
    :raise SchemaError: If a bitmap cannot be expanded against *schema*
    """
    header = payload.get(CHANGE_EVENT_HEADER)
    if not isinstance(header, dict):
        return False
    for name in BITMAP_FIELDS:
        bitmap_fields = header.get(name)
        if bitmap_fields:
            header[name] = expand_bitmap_fields(schema, bitmap_fields)
    return True
