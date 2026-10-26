# Compact PostgreSQL storage, version 1

Install `compact-storage.sql` explicitly in an isolated restored database before
setting `PANAKO_PG_COMPACT=TRUE`. This flag is accepted from the environment and
is false by default. The ordinary `panako_fingerprint` table and its hash indexes
are still required; it accepts new writes. Both packed orientations must be
backed up with that table, `panako_resource`, the views and all SQL functions.

Use `panako_compact_partition('panako_fingerprint_pNNN')` only during **offline**
restoration, one full partition at a time. Both packed writes and truncation of
the ordinary source commit together; interruption rolls back. Verify canonical
checksums of `(hash, resource_id, t, f)` before/after conversion. Keep the original
backup. Reverting to an older engine requires its ordinary database restored;
turning the flag off is not a rollback.

For a serving database use bounded `panako_compact_batch(partition, batch_size)`
transactions instead. The service schedules these, honors deployment pauses,
and uses short timeouts. Each successful call is durable and repeating it is
safe. Keep autovacuum enabled and monitor write-buffer growth and compaction
failures. A five-second timeout may need a smaller batch on slower machines.
The advisory lock number is part of the engine/schema contract: all compact
writers take shared lock 179108082; compaction and deletion take it exclusively.
External writers must follow the same protocol. Legacy `migrate` is intentionally
refused in compact mode.

The arrays are ordered and deduplicated by the supplied mutation functions.
Do not write packed arrays directly: capped retrieval depends on this invariant.
The full views are useful for verification; the engine uses `panako_compact_hits`
to stop expanding a very common hash after the requested number of eligible hits.

On a restored 47,359,250-fingerprint partition, packed tables including reverse
lookup/deletion data measured 1,365,114,880 bytes versus 5,851,521,024 originally.
Full checksums matched in both orientations and after dump/restore. A local
480-query benchmark returned identical capped/range/exclusion results, with
packed median/p95 2.06/2.34 ms versus ordinary 0.91/1.08 ms. These are warm local
measurements, not full-library or replacement-host performance claims.
The historical AudioScout audio acceptance gate passed for both engines with
matching identification outcomes; the separate production-regression setting
run retained an existing 8/10 result below the 9/10 +6% vinyl-pitch bar in both.
Scores can change because the previous query did not specify ordering.

For the Java integration check, create an empty local database named
`ear_storage_benchmark_engine_NAME`, set `PANAKO_PG_URL` to its loopback JDBC URL,
`PANAKO_PG_USER` to its user and `PANAKO_PG_COMPACT=true`. Compile
`src/test/java/be/panako/strategy/panako/storage/CompactStorageCheck.java` against
the engine JAR, then run its main class with `sql/compact-storage.sql` as the
argument. The check refuses other database names and exercises real queue APIs,
missing-schema refusal, duplicate/capped/excluded queries, both deletion paths
and clear. No audio download is needed for this storage check.

The original ordinary PostgreSQL and LMDB modes remain available. No extraction,
fingerprint density, hash precision or matching thresholds are changed here.
