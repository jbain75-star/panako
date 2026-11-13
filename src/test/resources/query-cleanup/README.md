# Query cleanup checks

The frozen baseline contains the matching helpers/query from Panako
`54d35cf4c1a887cded153dce97eb65c2afabb2bf`, with only the private query made
public inside the replay and whitespace normalized. The optimized methods are
extracted from the working
`PanakoStrategy.java` at test time, not maintained as a second implementation.
The replay supplies synthetic storage, pre-extracted fingerprints, metadata,
and numeric configuration. It does not connect to PostgreSQL or run native DSP.

`Golden.java.template` checks all 486 fixtures from the offline investigation:
432 combinations of row counts, seeds, shapes and result limits, plus 54
accepted-frequency/affine-stretch cases. Results include ordered identifiers,
paths, scores, times and factors serialized with raw double bits. The original
baseline digest is frozen and must not be regenerated to accept a change.
Additional checks reuse a strategy instance across configuration changes,
verify lazy invalid-setting failures, preserve cursor-error propagation, and
explicitly guard the original HashMap delta-mode tie winner.

`QueryCleanupTest` runs these checks under the existing Gradle test task.
With only JDK 11 or later, from the repository root:

```
java src/test/java/be/panako/strategy/panako/QueryCleanupCheck.java
```

Generated sources/classes are isolated in `build/query-cleanup-check`. For a
fresh local benchmark of those generated methods, after running the check:

```
python3 src/test/resources/query-cleanup/benchmark.py
```

The driver uses randomized cold JVMs, a 2 GiB maximum heap, five forks per
candidate/workload, and grouped-by-query-hash synthetic cursor rows. It compares
1.67m-row mixed, no-match and dense workloads, plus 4m mixed rows. The estimate
comes from the earlier 3,335-row/four-hash probe extrapolated to a 2,000-hash
chunk; four million is the default per-chunk cap, not an observed query count.
Timing output is milliseconds; allocation is MiB (thread allocation, not RSS).
JVM process CPU includes compiler/GC activity during the matching interval.
Database waits, real JDBC decoding, native extraction and full application
startup are excluded. No full-query or production-throughput gain is implied.

`benchmark.json` records the working strategy source hash and exact result
digests. Results are local measurements and must be compared within one run.
Do not multiply cleanup gains by the earlier larger allocation prototype's
gains: this change does not include primitive hit blocks or thinner matches.

## Local measurement, November 13, 2026 (PT)

OpenJDK 17, two logical CPUs, `-Xmx2048m`, five randomized cold forks per
candidate/workload; measured from the actual working query methods:

| Workload | Baseline CPU ms | Cleanup CPU ms | Reduction | Allocation MiB, before → after |
|---|---:|---:|---:|---:|
| Mixed, 1.67m rows | 1,510 | 1,350 | 10.6% | 487.25 → 405.28 |
| No-match, 1.67m rows | 1,460 | 1,260 | 13.7% | 497.82 → 405.14 |
| Dense, 1.67m rows | 1,230 | 820 | 33.3% | 575.96 → 332.92 |
| Mixed, 4m rows | 3,050 | 2,770 | 9.2% | 1,130.80 → 935.00 |

Mixed 1.67m query wall time was 828 → 745 ms; no-match 790 → 729 ms.
These are exploratory phase-only medians, not full-query improvements against
the observed 13–18 Java CPU-seconds or a measured ingestion-rate change.
