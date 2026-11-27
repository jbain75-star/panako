# Ordered compact lookup merge

The old compact lookup combines the first K eligible packed tuples with the
first K eligible recent tuples using `UNION`, then orders and limits the union.
On a dense query, PostgreSQL builds a hash aggregate and sorts that whole union
for each queried fingerprint hash. Thousands of query hashes repeat this work.

Use `DISTINCT ON (hash, resource_id, t, f)` over `UNION ALL`, with the same final
ordering and limit. PostgreSQL can use Merge Append, Unique and Limit instead:
merge the ordered streams, skip adjacent duplicates, stop after K unique tuples.
The packed helper retains its existing early stop. The recent branch retains
its existing covering index and cap. Both branches are evaluated in the same
statement snapshot, including during concurrent compaction.

This changes neither which tuples are eligible nor their canonical order.
Exclusions still apply before each branch cap; deduplication still applies after
the branch caps and before the final cap. Nonpositive caps still mean unlimited.
Repeated query hashes retain their original hit multiplicity. The ordinary
non-compact lookup, chunk size, cursor fetch size, candidate verification,
alignment and acceptance rules remain unchanged. No database migration or stored
function replacement is required.

## Validation

`CompactMergeQueryCheck` compares the actual storage implementation with the old
`panako_compact_hits` function in an isolated local PostgreSQL database. Its 67
exact ordered comparisons cover dense neighboring hashes, ranges, caps of
-1/0/1/2/7/2000, exclusions, signed/extreme resource IDs, packed/buffer overlap,
packed duplicates, missing hashes, empty input, and repeated query hashes across
the 2,000-input chunk boundary. Startup also refuses a missing packed helper.
The test refuses a nonlocal or nondedicated DB.

The ordinary Gradle test suite and its 486 actual-query golden fixtures pass.
The external audio dataset tests remain skipped when their files are absent.

## Measured lookup performance

A read-only comparison reused 3,599 saved fingerprints from a dense noise query.
Both variants used the same 2,000-hit cap, hash ranges, 2,000-input chunks,
10,000-row JDBC cursor fetches, and one repeatable-read database snapshot.
Each returned the same 6,092,845 tuples, in the same order, with the same SHA-256
digest (`a+7ZzTS+MX3dTGlGJS+0/SbEb1OhHrQ+7aRN0I3qxtA=`).

| Execution order | Lookup and JDBC read time |
| --- | ---: |
| Previous lookup | 37.882 s |
| Ordered merge | 15.468 s |
| Ordered merge | 14.683 s |
| Previous lookup | 25.030 s |

The reversed-order warm comparison improved about 41%; the first comparison
improved about 59%. Earlier 32-hash comparisons showed the plan changing from
hash aggregate plus sort to Merge Append, Unique and Limit. Their 58,728 tuples
also matched exactly.

These are query-retrieval measurements, not end-to-end service latency or a
production percentile guarantee. The running collection workload was not paused;
Ear-container CPU readings before/after were 71%/46%, and memory 6.78/5.09 GiB.
Query plans depend on PostgreSQL statistics and workload. The standalone
`CompactLookupBenchmark` reproduces the old/new/new/old comparison using an
explicit retained fingerprint file and read-only transaction; it neither loads
source audio nor writes fingerprints.


## Full engine query checks

Installed and candidate engines then queried the same retained Jesse Perez,
Vidaloca and white-noise fingerprints against the live catalog. All returned
resource IDs, scores, time factors and frequency factors matched exactly,
including below-threshold candidates. Jesse retained scores 55/49, Vidaloca
retained 91/84, and white noise returned no candidate.

Whole CLI command times (including startup and alignment, with extraction
replaced by the same frozen fingerprints) were 13.562 -> 7.736 seconds for Jesse,
19.448 -> 8.425 for Vidaloca, and 8.071 -> 5.116 for noise. The candidate ran the
samples in reverse order. These six bounded checks are not a production latency
percentile or a full-catalog recognition estimate. Temporary query-cache copies,
placeholder files and diagnostic jars were removed; no source audio was decoded
or retained, and no production engine or database function was replaced.
