Changelog
=========

0.2.0 (2026-09-11)
------------------

- ``PasswordAuthenticator`` accepts a ``domain`` argument naming the host to
  request tokens from: ``login``, ``test``, or an org's My Domain such as
  ``mycompany.my``. It follows the same rules as ``SOAPAuthenticator``,
  whose validation it now shares: without it the ``sandbox`` flag decides
  as before, and an explicit value wins over ``sandbox``. This mirrors
  aiosfstream 1.4.0.

0.1.0 (2026-09-10)
------------------

First release.

- Subscribe to Platform Events and Change Data Capture topics over the
  Salesforce Pub/Sub API, with Avro payloads decoded automatically and
  schemas cached by id.
- Flow control driven by ``pending_num_requested``, so a subscription
  applies backpressure instead of stalling once its budget is spent.
- Automatic re-authentication for both subscription kinds, bounded by
  ``auth_retries``, resuming from the position the subscription was at.
- Client side replay marker storage (:obj:`~aiosfpubsub.ReplayOption`,
  :obj:`~aiosfpubsub.MappingStorage`,
  :obj:`~aiosfpubsub.ConstantReplayId`) and server side tracking
  through managed event subscriptions, both governed by
  :obj:`~aiosfpubsub.ReplayMarkerStoragePolicy`.
- Replay fallback for replay ids outside the retention window.
- Publishing, one request at a time or over a stream, with per-record
  failures reported through :obj:`~aiosfpubsub.PublishError`.
- ``PasswordAuthenticator``, ``RefreshTokenAuthenticator``,
  ``ClientCredentialsAuthenticator``, ``JWTBearerAuthenticator`` and
  ``SOAPAuthenticator``. The JWT Bearer flow signs its assertion with
  PyJWT, which comes with the ``jwt`` extra:
  ``pip install aiosfpubsub[jwt]``. The SOAP flow needs no connected app,
  but Salesforce retires it in Summer '27.
