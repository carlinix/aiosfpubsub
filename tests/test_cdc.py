import fastavro
import pytest

from aiosfpubsub.cdc import (
    expand_bitmap,
    expand_bitmap_fields,
    expand_change_event_header,
)
from aiosfpubsub.exceptions import SchemaError

ADDRESS = {
    "type": "record",
    "name": "Address",
    "fields": [
        {"name": "Street", "type": ["null", "string"]},
        {"name": "City", "type": ["null", "string"]},
        {"name": "Country", "type": ["null", "string"]},
    ],
}

# Field positions: 0 ChangeEventHeader, 1 Name, 2 BillingAddress,
# 3 AnnualRevenue, 4 ShippingAddress
ACCOUNT_CHANGE_EVENT = {
    "type": "record",
    "name": "AccountChangeEvent",
    "fields": [
        {
            "name": "ChangeEventHeader",
            "type": {
                "type": "record",
                "name": "ChangeEventHeader",
                "fields": [
                    {"name": "entityName", "type": "string"},
                    {
                        "name": "changedFields",
                        "type": {"type": "array", "items": "string"},
                    },
                    {
                        "name": "diffFields",
                        "type": {"type": "array", "items": "string"},
                    },
                    {
                        "name": "nulledFields",
                        "type": {"type": "array", "items": "string"},
                    },
                ],
            },
        },
        {"name": "Name", "type": ["null", "string"]},
        {"name": "BillingAddress", "type": ["null", ADDRESS]},
        {"name": "AnnualRevenue", "type": ["null", "double"]},
        # a second Address, which fastavro records as a named reference
        {"name": "ShippingAddress", "type": ["null", "Address"]},
    ],
}


@pytest.fixture
def schema():
    return fastavro.parse_schema(ACCOUNT_CHANGE_EVENT)


def header(**bitmaps):
    return {"ChangeEventHeader": {"entityName": "Account", **bitmaps}}


def test_expand_bitmap_selects_fields_least_significant_bit_first():
    fields = [{"name": "a"}, {"name": "b"}, {"name": "c"}, {"name": "d"}]

    assert expand_bitmap(fields, "0x01") == ["a"]
    assert expand_bitmap(fields, "0x0A") == ["b", "d"]
    assert expand_bitmap(fields, "0x0F") == ["a", "b", "c", "d"]
    assert expand_bitmap(fields, "0x00") == []


def test_expand_bitmap_rejects_a_malformed_value():
    with pytest.raises(SchemaError, match="Malformed"):
        expand_bitmap([{"name": "a"}], "not hex")


def test_expand_bitmap_rejects_a_field_beyond_the_schema():
    with pytest.raises(SchemaError, match="beyond the 2 fields"):
        expand_bitmap([{"name": "a"}, {"name": "b"}], "0x04")


def test_expand_top_level_fields(schema):
    # bits 1 and 3 -> Name and AnnualRevenue
    assert expand_bitmap_fields(schema, ["0x0A"]) == ["Name", "AnnualRevenue"]


def test_expand_nested_compound_fields(schema):
    # field 2 is BillingAddress; bits 0 and 2 of Address -> Street and Country
    assert expand_bitmap_fields(schema, ["2-0x05"]) == [
        "BillingAddress.Street",
        "BillingAddress.Country",
    ]


def test_expand_resolves_a_named_type_reference(schema):
    """fastavro writes the second use of a named type as its name alone"""
    assert expand_bitmap_fields(schema, ["4-0x02"]) == ["ShippingAddress.City"]


def test_expand_combines_top_level_and_nested_entries(schema):
    assert expand_bitmap_fields(schema, ["0x02", "2-0x01"]) == [
        "Name",
        "BillingAddress.Street",
    ]


def test_expand_ignores_a_nested_bitmap_on_a_plain_field(schema):
    # field 1 is Name, a string, so there is nothing to expand
    assert expand_bitmap_fields(schema, ["1-0x01"]) == []


def test_expand_rejects_an_unknown_parent_position(schema):
    with pytest.raises(SchemaError, match="not a position"):
        expand_bitmap_fields(schema, ["99-0x01"])


def test_expand_rejects_a_non_numeric_parent_position(schema):
    with pytest.raises(SchemaError, match="not a position"):
        expand_bitmap_fields(schema, ["oops-0x01"])


def test_expand_change_event_header_rewrites_every_bitmap_list(schema):
    payload = header(
        changedFields=["0x02"], diffFields=["0x08"], nulledFields=["2-0x01"]
    )

    assert expand_change_event_header(schema, payload) is True
    assert payload["ChangeEventHeader"] == {
        "entityName": "Account",
        "changedFields": ["Name"],
        "diffFields": ["AnnualRevenue"],
        "nulledFields": ["BillingAddress.Street"],
    }


def test_expand_change_event_header_leaves_empty_lists_alone(schema):
    payload = header(changedFields=[], nulledFields=None)

    assert expand_change_event_header(schema, payload) is True
    assert payload["ChangeEventHeader"]["changedFields"] == []
    assert payload["ChangeEventHeader"]["nulledFields"] is None


def test_expand_change_event_header_skips_a_platform_event(schema):
    """A payload without the header is not a change event"""
    payload = {"Field__c": "value"}

    assert expand_change_event_header(schema, payload) is False
    assert payload == {"Field__c": "value"}


def test_expand_change_event_header_skips_a_non_mapping_header(schema):
    payload = {"ChangeEventHeader": "not a record"}

    assert expand_change_event_header(schema, payload) is False
