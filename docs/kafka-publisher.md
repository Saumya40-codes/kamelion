# Kafka publication

`KafkaPublisher::publish` serializes each change and awaits Kafka's delivery
result. A successful return means the broker acknowledged the record according
to `acks=all`. It does not mean a downstream consumer has processed it.

The message key is JSON containing `source`, `table`, and the event's `key`.
This scopes record identity when several tables share a topic. Source values
must uniquely identify the source dataset, rather than just its backend type.
The key excludes the change position so successive changes to the same record
use the same partition key. Preserve the key encoding and topic partition count
when relying on partition ordering. Null record keys are rejected.

The payload is the complete JSON `ChangeEvent`. Deletes retain their operation,
before image, and position in the envelope; they are not Kafka tombstones.

The producer enables idempotence to suppress duplicates caused by its own
retries. This does not deduplicate replay after a service restart and does not
provide end-to-end exactly-once CDC. Checkpoints remain future work.

Queue admission can wait up to five seconds. Once admitted, the native producer
has a thirty-second delivery timeout. Failed delivery returns an error to the
pipeline; a timeout can leave delivery outcome uncertain.

Publication is currently awaited one event at a time. Bounded concurrent sends
can be added later with explicit ordering and checkpoint rules.

The unit tests verify the JSON envelope, record-key scope and stability, and
rejection of null keys. Broker delivery still requires integration testing.
The placeholder Hudi source returns no events, so `cargo run` alone does not
exercise publication.
