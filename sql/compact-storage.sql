-- SPDX-License-Identifier: AGPL-3.0-or-later
-- Copyright (C) 2026 AudioScout
-- Explicit, opt-in storage migration. Install on an isolated restoration first.
-- Audio hashes, resource identifiers, times and frequencies remain unchanged.
-- New writes use the ordinary indexed table; maintenance packs them in batches.
BEGIN;
SELECT pg_advisory_xact_lock(179108082);

CREATE TABLE IF NOT EXISTS panako_fingerprint_compact_meta (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  version integer NOT NULL CHECK (version = 1)
);
INSERT INTO panako_fingerprint_compact_meta VALUES (true, 1) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS panako_fingerprint_hash_packed (
  hash bigint PRIMARY KEY,
  resources integer[] NOT NULL,
  times integer[] NOT NULL,
  freqs integer[] NOT NULL,
  CHECK (cardinality(resources) = cardinality(times) AND cardinality(times) = cardinality(freqs)),
  CHECK (array_position(resources, NULL) IS NULL AND array_position(times, NULL) IS NULL AND array_position(freqs, NULL) IS NULL)
);
CREATE TABLE IF NOT EXISTS panako_fingerprint_resource_packed (
  resource_id integer PRIMARY KEY,
  hashes bigint[] NOT NULL,
  times integer[] NOT NULL,
  freqs integer[] NOT NULL,
  CHECK (cardinality(hashes) = cardinality(times) AND cardinality(times) = cardinality(freqs)),
  CHECK (array_position(hashes, NULL) IS NULL AND array_position(times, NULL) IS NULL AND array_position(freqs, NULL) IS NULL)
);

-- UNION deduplicates a reprint that is present in both the packed store and
-- the ordinary write buffer. A LIMIT must be applied AFTER this deduplication.
CREATE OR REPLACE VIEW panako_fingerprint_search AS
  SELECT hash, resource_id, t, f FROM panako_fingerprint
  UNION
  SELECT h.hash, p.resource_id, p.t, p.f
  FROM panako_fingerprint_hash_packed h
  CROSS JOIN LATERAL unnest(h.resources, h.times, h.freqs) p(resource_id,t,f);

CREATE OR REPLACE VIEW panako_fingerprint_by_resource AS
  SELECT hash, resource_id, t, f FROM panako_fingerprint
  UNION
  SELECT p.hash, r.resource_id, p.t, p.f
  FROM panako_fingerprint_resource_packed r
  CROSS JOIN LATERAL unnest(r.hashes, r.times, r.freqs) p(hash,t,f);

-- Arrays are sorted in B-tree tuple order by every writer below. Stop after
-- enough eligible hits instead of expanding/sorting an entire common hash.
CREATE OR REPLACE FUNCTION panako_compact_packed_hits(
  low_hash bigint, high_hash bigint, avoid integer[], max_hits integer)
RETURNS TABLE(hash bigint,resource_id integer,t integer,f integer)
LANGUAGE plpgsql STABLE AS $fn$
DECLARE held record; i integer; emitted integer := 0; rs integer[]; ts integer[]; fs integer[];
BEGIN
  FOR held IN SELECT h.hash,h.resources,h.times,h.freqs
    FROM panako_fingerprint_hash_packed h WHERE h.hash BETWEEN low_hash AND high_hash ORDER BY h.hash
  LOOP
    rs := held.resources; ts := held.times; fs := held.freqs;
    FOR i IN 1..cardinality(rs) LOOP
      IF rs[i] <> ALL(avoid) THEN
        hash := held.hash; resource_id := rs[i]; t := ts[i]; f := fs[i];
        RETURN NEXT;
        emitted := emitted + 1;
        IF max_hits > 0 AND emitted >= max_hits THEN RETURN; END IF;
      END IF;
    END LOOP;
  END LOOP;
END $fn$;

-- Each branch is unique and contributes its first K eligible tuples. Their
-- deduplicated union therefore contains the first K of the complete union,
-- including reprints that occur in both branches. Apply exclusions before K.
CREATE OR REPLACE FUNCTION panako_compact_hits(
  low_hash bigint, high_hash bigint, avoid integer[], max_hits integer)
RETURNS TABLE(hash bigint,resource_id integer,t integer,f integer)
LANGUAGE sql STABLE AS $fn$
  SELECT * FROM (
    (SELECT h.hash,h.resource_id,h.t,h.f FROM panako_fingerprint h
     WHERE h.hash BETWEEN low_hash AND high_hash AND h.resource_id <> ALL(avoid)
     ORDER BY h.hash,h.resource_id,h.t,h.f LIMIT nullif(greatest(max_hits,0),0))
    UNION
    SELECT * FROM panako_compact_packed_hits(low_hash,high_hash,avoid,max_hits)
  ) hits ORDER BY hash,resource_id,t,f LIMIT nullif(greatest(max_hits,0),0)
$fn$;

-- Both orientations are written in the same transaction. Arrays are kept in
-- the original B-tree tuple order, with exact duplicate tuples removed.
CREATE OR REPLACE FUNCTION panako_compact_pack_incoming() RETURNS void
LANGUAGE plpgsql AS $fn$
BEGIN
  INSERT INTO panako_fingerprint_hash_packed AS held(hash,resources,times,freqs)
  SELECT hash,array_agg(resource_id ORDER BY resource_id,t,f),
         array_agg(t ORDER BY resource_id,t,f),array_agg(f ORDER BY resource_id,t,f)
    FROM (SELECT DISTINCT hash,resource_id,t,f FROM pg_temp.ear_compact_incoming) d
   GROUP BY hash ORDER BY hash
  ON CONFLICT (hash) DO UPDATE SET (resources,times,freqs) = (
    SELECT array_agg(resource_id ORDER BY resource_id,t,f),
           array_agg(t ORDER BY resource_id,t,f),array_agg(f ORDER BY resource_id,t,f)
      FROM (SELECT * FROM unnest(held.resources,held.times,held.freqs)
            UNION SELECT * FROM unnest(excluded.resources,excluded.times,excluded.freqs)) p(resource_id,t,f)
  );
  INSERT INTO panako_fingerprint_resource_packed AS held(resource_id,hashes,times,freqs)
  SELECT resource_id,array_agg(hash ORDER BY hash,t,f),
         array_agg(t ORDER BY hash,t,f),array_agg(f ORDER BY hash,t,f)
    FROM (SELECT DISTINCT hash,resource_id,t,f FROM pg_temp.ear_compact_incoming) d
   GROUP BY resource_id ORDER BY resource_id
  ON CONFLICT (resource_id) DO UPDATE SET (hashes,times,freqs) = (
    SELECT array_agg(hash ORDER BY hash,t,f),
           array_agg(t ORDER BY hash,t,f),array_agg(f ORDER BY hash,t,f)
      FROM (SELECT * FROM unnest(held.hashes,held.times,held.freqs)
            UNION SELECT * FROM unnest(excluded.hashes,excluded.times,excluded.freqs)) p(hash,t,f)
  );
END $fn$;

-- Only an actual direct partition of this engine's fingerprint table is
-- admitted. Table names are identifiers, never arbitrary SQL supplied by CLI.
CREATE OR REPLACE FUNCTION panako_compact_check_partition(partition_name text) RETURNS regclass
LANGUAGE plpgsql AS $fn$
DECLARE target regclass;
BEGIN
  IF partition_name !~ '^panako_fingerprint_p[0-9]+$' THEN
    RAISE EXCEPTION 'Unexpected fingerprint partition';
  END IF;
  target := to_regclass(partition_name);
  IF target IS NULL OR NOT EXISTS (
    SELECT 1 FROM pg_inherits WHERE inhrelid = target AND inhparent = 'panako_fingerprint'::regclass
  ) THEN RAISE EXCEPTION 'Not a fingerprint partition'; END IF;
  RETURN target;
END $fn$;

CREATE OR REPLACE FUNCTION panako_compact_batch(partition_name text, batch_size integer DEFAULT 100000)
RETURNS bigint LANGUAGE plpgsql AS $fn$
DECLARE target regclass; moved bigint;
BEGIN
  IF batch_size IS NULL OR batch_size < 1 OR batch_size > 1000000 THEN RAISE EXCEPTION 'Batch size must be 1..1000000'; END IF;
  target := panako_compact_check_partition(partition_name);
  PERFORM pg_advisory_xact_lock(179108082);
  EXECUTE format('CREATE TEMP TABLE ear_compact_incoming ON COMMIT DROP AS
    WITH moved AS (DELETE FROM %s WHERE ctid IN
      (SELECT ctid FROM %s LIMIT $1 FOR UPDATE) RETURNING hash,resource_id,t,f)
    SELECT * FROM moved',target,target) USING batch_size;
  GET DIAGNOSTICS moved = ROW_COUNT;
  IF moved > 0 THEN PERFORM panako_compact_pack_incoming(); END IF;
  DROP TABLE pg_temp.ear_compact_incoming;
  RETURN moved;
END $fn$;

-- Offline restoration mode: pack one entire partition and reclaim its files
-- atomically. The lock intentionally excludes all reads/writes of that
-- partition. Never schedule this function against a serving database.
CREATE OR REPLACE FUNCTION panako_compact_partition(partition_name text)
RETURNS bigint LANGUAGE plpgsql AS $fn$
DECLARE target regclass; moved bigint;
BEGIN
  target := panako_compact_check_partition(partition_name);
  PERFORM pg_advisory_xact_lock(179108082);
  EXECUTE format('LOCK TABLE %s IN ACCESS EXCLUSIVE MODE',target);
  EXECUTE format('CREATE TEMP TABLE ear_compact_incoming ON COMMIT DROP AS SELECT hash,resource_id,t,f FROM %s',target);
  GET DIAGNOSTICS moved = ROW_COUNT;
  IF moved > 0 THEN PERFORM panako_compact_pack_incoming(); END IF;
  EXECUTE format('TRUNCATE TABLE %s',target);
  DROP TABLE pg_temp.ear_compact_incoming;
  RETURN moved;
END $fn$;

CREATE OR REPLACE FUNCTION panako_compact_delete_resource(wanted integer)
RETURNS void LANGUAGE plpgsql AS $fn$
DECLARE keys bigint[];
BEGIN
  PERFORM pg_advisory_xact_lock(179108082);
  SELECT hashes INTO keys FROM panako_fingerprint_resource_packed WHERE resource_id = wanted;
  IF keys IS NOT NULL THEN
    UPDATE panako_fingerprint_hash_packed h SET (resources,times,freqs) = (
      SELECT coalesce(array_agg(resource_id ORDER BY resource_id,t,f),'{}'::integer[]),
             coalesce(array_agg(t ORDER BY resource_id,t,f),'{}'::integer[]),
             coalesce(array_agg(f ORDER BY resource_id,t,f),'{}'::integer[])
      FROM unnest(h.resources,h.times,h.freqs) p(resource_id,t,f) WHERE resource_id <> wanted
    ) WHERE h.hash = ANY(keys);
    DELETE FROM panako_fingerprint_hash_packed WHERE hash = ANY(keys) AND cardinality(resources) = 0;
  END IF;
  DELETE FROM panako_fingerprint_resource_packed WHERE resource_id = wanted;
  DELETE FROM panako_fingerprint WHERE resource_id = wanted;
  DELETE FROM panako_resource WHERE resource_id = wanted;
END $fn$;

CREATE OR REPLACE FUNCTION panako_compact_delete_fingerprints(
  input_hashes bigint[], input_resources integer[], input_times integer[], input_freqs integer[])
RETURNS void LANGUAGE plpgsql AS $fn$
BEGIN
  IF input_hashes IS NULL OR input_resources IS NULL OR input_times IS NULL OR input_freqs IS NULL OR
     cardinality(input_hashes) <> cardinality(input_resources) OR cardinality(input_resources) <> cardinality(input_times) OR
     cardinality(input_times) <> cardinality(input_freqs) OR array_position(input_hashes,NULL) IS NOT NULL OR
     array_position(input_resources,NULL) IS NOT NULL OR array_position(input_times,NULL) IS NOT NULL OR array_position(input_freqs,NULL) IS NOT NULL
  THEN RAISE EXCEPTION 'Mismatched or null fingerprint arrays'; END IF;
  PERFORM pg_advisory_xact_lock(179108082);
  CREATE TEMP TABLE ear_compact_deletes ON COMMIT DROP AS
    SELECT DISTINCT p.hash,p.resource_id,p.t,p.f FROM unnest(input_hashes,input_resources,input_times,input_freqs) p(hash,resource_id,t,f);
  CREATE UNIQUE INDEX ON ear_compact_deletes(hash,resource_id,t,f);
  CREATE INDEX ON ear_compact_deletes(resource_id,hash,t,f);
  ANALYZE ear_compact_deletes;
  DELETE FROM panako_fingerprint h USING ear_compact_deletes d
    WHERE (h.hash,h.resource_id,h.t,h.f) = (d.hash,d.resource_id,d.t,d.f);
  UPDATE panako_fingerprint_hash_packed h SET (resources,times,freqs) = (
    SELECT coalesce(array_agg(resource_id ORDER BY resource_id,t,f),'{}'::integer[]),
           coalesce(array_agg(t ORDER BY resource_id,t,f),'{}'::integer[]),
           coalesce(array_agg(f ORDER BY resource_id,t,f),'{}'::integer[])
    FROM unnest(h.resources,h.times,h.freqs) p(resource_id,t,f)
    WHERE NOT EXISTS (SELECT 1 FROM ear_compact_deletes d WHERE (d.hash,d.resource_id,d.t,d.f) = (h.hash,p.resource_id,p.t,p.f))
  ) WHERE h.hash IN (SELECT d.hash FROM ear_compact_deletes d);
  UPDATE panako_fingerprint_resource_packed r SET (hashes,times,freqs) = (
    SELECT coalesce(array_agg(hash ORDER BY hash,t,f),'{}'::bigint[]),
           coalesce(array_agg(t ORDER BY hash,t,f),'{}'::integer[]),
           coalesce(array_agg(f ORDER BY hash,t,f),'{}'::integer[])
    FROM unnest(r.hashes,r.times,r.freqs) p(hash,t,f)
    WHERE NOT EXISTS (SELECT 1 FROM ear_compact_deletes d WHERE (d.hash,d.resource_id,d.t,d.f) = (p.hash,r.resource_id,p.t,p.f))
  ) WHERE r.resource_id IN (SELECT d.resource_id FROM ear_compact_deletes d);
  DELETE FROM panako_fingerprint_hash_packed WHERE hash IN (SELECT d.hash FROM ear_compact_deletes d) AND cardinality(resources) = 0;
  DELETE FROM panako_fingerprint_resource_packed WHERE resource_id IN (SELECT d.resource_id FROM ear_compact_deletes d) AND cardinality(hashes) = 0;
  DROP TABLE pg_temp.ear_compact_deletes;
END $fn$;

COMMIT;
