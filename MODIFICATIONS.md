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
- **A `migrate` command** (`be.panako.cli.Migrate`,
  `PanakoStorageMigration`) that copies an existing LMDB store into the
  PostgreSQL one. The fingerprints in LMDB are the full prints of every stored
  file, so they are moved as data rather than extracted from audio again. LMDB
  is only read, one bounded batch per read transaction, so no reader is long
  lived enough to pin its free list while the store stays in use. Each batch
  ends on a hash boundary and commits its rows together with that hash, so an
  interrupted run resumes from it and repeats only the batch that was in
  flight. `--verify` compares the number of audio files, the number of
  fingerprints, all meta-data and a sample of individual fingerprints spread
  over the hash space. `--min-duration=SECONDS` leaves the prints of files
  shorter than that behind - a print of thirty seconds of a six minute record
  only matches while those thirty seconds play - and lists them in
  `panako_migration_short_resource` instead, as the files worth fetching in
  full. A duration of zero is unknown rather than short, so such a file is
  copied. A resume point belongs to one store copied under one threshold: both
  are recorded beside it and a run with either changed refuses to continue and
  asks for `--restart`, since a threshold that moved decides different rows.
  What an earlier run copied and this one leaves behind is removed from
  PostgreSQL, and the list of files worth fetching in full is the current
  selection rather than everything ever left behind. Verification holds the two
  stores to the same number of fingerprints rather than a minimum, so a print of
  something the source no longer holds is a difference.
- **A `serve` command** (`be.panako.cli.Serve`) that answers queries read from
  standard input, keeping the index open between them. `query` is a whole
  process per question — a JVM starts, the index is opened, one file is matched,
  everything is discarded — which for a service answering one clip at a time is
  nearly all of the wait: measured against a live index, about a second of
  matching arrived eleven seconds after it was asked for, and four seconds of
  audio took longer than thirty seconds of audio did, a bill that plainly has
  nothing to do with the audio. This command pays it once. Its conversation is
  line based so it needs no library on either side: it prints `READY` when the
  storage is open, then for each `<id>\t<audio file>` line read from standard
  input prints `BEGIN <id>`, the result rows exactly as `query` prints them, and
  `END <id> ok` or `END <id> error <what went wrong>`; `QUIT` or end of input
  stops it. Identifiers are the caller's and are echoed rather than interpreted.
  One query at a time, in the order asked, since a query is CPU and memory
  hungry and the index is memory mapped — concurrency belongs to the caller,
  which runs several of these. Nothing a query can throw stops the process: a
  file that cannot be read or decoded is reported against its own identifier and
  the next question is answered normally, because a service that dies on one bad
  clip hands back the warm index it exists to keep.
- **Configuration arguments split on the first `=` only** (`be.panako.cli.Panako`),
  so a value containing one — a JDBC url with query parameters — is no longer
  truncated.
- **The `delete` command guard was inverted** (`be.panako.cli.Delete`):
  deletion was skipped exactly when the resource *was* present in the store,
  and attempted when it was not, so nothing was ever deleted.
- **`PanakoStorageKV.processDeleteQueue` read the store queue** instead of the
  delete queue, so queued deletions were dropped.
- `org.postgresql:postgresql` added to the Gradle dependencies.
- **The test dataset is an assumption rather than a requirement**
  (`be.panako.tests.TestData`): every test in the suite needs audio downloaded
  from `panako.be`, and a machine with no route to that host reported five
  failures that say nothing about the engine. A file that cannot be fetched now
  aborts the tests that need it as undecided, so the suite is honest on a
  build machine without the dataset and unchanged on one with it.
