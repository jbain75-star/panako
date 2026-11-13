"""Benchmark the generated working query and frozen baseline; standard library only."""
import hashlib
import json
import os
from pathlib import Path
import random
import statistics
import subprocess

root = Path(__file__).resolve().parents[4]
output = root / "build/query-cleanup-check"
java = str(Path(os.environ["JAVA_HOME"]) / "bin/java") if "JAVA_HOME" in os.environ else "java"
source = root / "src/main/java/be/panako/strategy/panako/PanakoStrategy.java"
source_hash = hashlib.sha256(source.read_bytes()).hexdigest()
jobs = [(v, s, n, i) for v in ["baseline", "optimized"]
        for s, n in [("mixed", 1670000), ("noise", 1670000), ("dense", 1670000), ("mixed", 4000000)]
        for i in range(5)]
random.Random(1978).shuffle(jobs)
rows = []
for variant, shape, count, fork in jobs:
    result = subprocess.run([java, "-Xmx2048m", "-cp", str(output), "QueryReplay",
                             variant, str(count), "0", "1", shape],
                            check=True, capture_output=True, text=True, timeout=120)
    record = json.loads(result.stdout.strip())
    record.update(shape=shape, fork=fork, source_sha256=source_hash)
    rows.append(record)
    print(variant, shape, count, record["process_cpu_ms"], flush=True)
assert source_hash == hashlib.sha256(source.read_bytes()).hexdigest(), "Source changed during benchmark"
(output / "benchmark.json").write_text(json.dumps(rows, indent=2))
for shape, count in [("mixed", 1670000), ("noise", 1670000), ("dense", 1670000), ("mixed", 4000000)]:
    group = [r for r in rows if r["shape"] == shape and r["rows"] == count]
    assert len({r["digest"] for r in group}) == 1, "Ordered results differ"
    for variant in ["baseline", "optimized"]:
        records = [r for r in group if r["variant"] == variant]
        medians = {k: round(statistics.median(r[k] for r in records), 3)
                   for k in ["process_cpu_ms", "wall_ms", "allocated_mb"]}
        print("MEDIAN", shape, count, variant, medians, flush=True)
print("BENCHMARK_EQUIVALENCE_CHECK: PASS (four workloads; identical ordered-result digests)")
