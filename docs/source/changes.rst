Changelog
=========

0.1.0
-----

First release.

- Subscribe to Platform Events and Change Data Capture topics over the
  Salesforce Pub/Sub API, with Avro payloads decoded automatically and
  schemas cached by id.
- Flow control driven by ``pending_num_requested``, so a subscription
  applies backpressure instead of stalling once its budget is spent.
- Automatic re-authentication for both subscription kinds, bounded by
  ``auth_retries``, resuming from the position the subscription was at.
- Client side replay marker storage (:obj:`~simple_salesforce_pubsub.ReplayOption`,
  :obj:`~simple_salesforce_pubsub.MappingStorage`,
  :obj:`~simple_salesforce_pubsub.ConstantReplayId`) and server side tracking
  through managed event subscriptions, both governed by
  :obj:`~simple_salesforce_pubsub.ReplayMarkerStoragePolicy`.
- Replay fallback for replay ids outside the retention window.
- Publishing, one request at a time or over a stream, with per-record
  failures reported through :obj:`~simple_salesforce_pubsub.PublishError`.
- ``PasswordAuthenticator``, ``RefreshTokenAuthenticator`` and
  ``ClientCredentialsAuthenticator``.
