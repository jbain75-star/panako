# Optional query alignment recovery

`PANAKO_ROBUST_ALIGNMENT_MIN_HITS=0` is the default and preserves the previous
query behavior. A value of at least 6 enables recovery for candidates whose
legacy timing fit has fewer aligned hits than that value. Consumers should set
the value to their acceptance floor (AudioScout uses 40), then rerun their
recognition and false-positive acceptance tests before enabling it.

Recovery proposes a bounded set of timing lines, fits their supporting hits,
and requires the configured minimum plus independent query times. It uses the
existing time tolerance and tempo/pitch bounds. Duration, seconds-with-matches,
filtered-hit gates, exclusions and result ordering still run afterward. A
candidate already supported by the configured number of legacy aligned hits
retains its original fit. Fingerprint extraction and stored formats do not change.

The setting does not retrieve missing fingerprints. If a storage result cap
removed the correct evidence, alignment alone cannot recover it. Raising a cap
alone can also expose conflicting modes in the old timing estimate. Validate
retrieval and alignment together on the actual index; a small isolated corpus
does not exercise dense hash buckets or prove whole-catalog precision.

## Offline regression checks (JDK 17, no downloaded test dependencies)

From the repository root:

```sh
java src/test/java/be/panako/strategy/panako/QueryCleanupCheck.java
mkdir -p build/robust-check
javac --release 11 -d build/robust-check \
  src/main/java/be/panako/strategy/panako/RobustAlignment.java \
  src/test/java/be/panako/strategy/panako/RobustAlignmentCheck.java
java -cp build/robust-check be.panako.strategy.panako.RobustAlignmentCheck
```

The checks cover 486 disabled-mode golden fixtures, enabled recovery in the
actual query method, preservation of established strong answers and acceptance
gates, and 285 synthetic timing/pitch/noise cases. Synthetic fixtures contain no
private recordings or production fingerprints. `QueryCleanupTest` also runs
these checks from the normal JUnit suite.
