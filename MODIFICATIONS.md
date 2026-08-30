# Modifications

This is a modified copy of [Panako](https://github.com/JorenSix/Panako) by Joren
Six, forked at commit `e4b0e1dbb55e340bc66c90bac0ceb82b2cf84211`. Panako is
licensed under the AGPL-3.0; so is this copy, and the modified source is
published here to satisfy that licence.

Changed by AudioScout, 2026:

- **A PostgreSQL fingerprint store** (`PanakoStoragePostgres`), selected with
  `PANAKO_STORAGE=PG`. Fingerprints live in a hash-range partitioned table
  rather than in LMDB, so concurrent `store` processes are not serialised
  behind LMDB's single process-wide write transaction, deletion actually
  removes rows, and the index does not have to fit in the page cache. New
  configuration keys: `PANAKO_PG_URL`, `PANAKO_PG_USER`, `PANAKO_PG_PASSWORD`,
  `PANAKO_PG_PARTITIONS`, `PANAKO_PG_BATCH_SIZE`. The url, user and password
  are also read from environment variables of the same name, so a password need
  not be passed on a command line; an environment variable that is set wins
  over the configuration file, since `PANAKO_PG_URL` has a built-in default
  (a database on localhost) that would otherwise silently take precedence.
  Fingerprints are unique per `(hash, resource_id, t, f)` and inserted with `ON
  CONFLICT DO NOTHING`, so storing the same audio again cannot double its
  fingerprints.
- **Configuration arguments split on the first `=` only** (`be.panako.cli.Panako`),
  so a value containing one — a JDBC url with query parameters — is no longer
  truncated.
- **The `delete` command guard was inverted** (`be.panako.cli.Delete`):
  deletion was skipped exactly when the resource *was* present in the store,
  and attempted when it was not, so nothing was ever deleted.
- **`PanakoStorageKV.processDeleteQueue` read the store queue** instead of the
  delete queue, so queued deletions were dropped.
- `org.postgresql:postgresql` added to the Gradle dependencies.
