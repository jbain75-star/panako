# Optional complete-candidate verification

A capped catalog search can find the right recording but omit enough of its
fingerprint evidence to make the alignment fail. Increasing that cap also
adds unrelated evidence and does not reliably recover the match.

`PANAKO_PG_REFINE_CANDIDATES=8` enables a second, bounded read for the compact
PostgreSQL layout. The default is zero, preserving the previous behavior.
Candidates are ranked by the number of distinct queried hashes that support
them; repeated copies of one beat cannot dominate the shortlist. At most eight
recordings with at least six distinct supporting hashes are checked.

The additional read combines the resource-keyed packed index with recent rows
selected by their hash index. It retrieves only the requested neighboring
hashes for those recordings, deduplicates packed/buffer overlap, and honors
resource exclusions. No schema, extraction, stored fingerprint or acceptance
threshold changes are required.

Complete evidence is aligned separately from the broad-search evidence. The
better passing result for each recording is retained, so a failed verification
cannot discard an established answer. Existing tempo, pitch, duration and
seconds-with-matches gates apply to both. Final recording identifiers are
unique before the caller's result limit is applied.

## Bounds and fallback

Verification is skipped outside compact PostgreSQL, for more than 4,000 query
fingerprints or 2,000 distinct hashes, or for query ranges above four. The
additional SQL statement has a three-second timeout. Reads are capped at
100,000 distinct fingerprint rows and expansion at 500,000 hits; crossing a
budget discards the entire additional result. SQL failure or cancellation
retains the original search results and resets the connection for reuse.

This is candidate verification, not an exhaustive catalog search. It cannot
recover a recording that never enters the shortlist or has no usable reference.
The original broad-search cap remains in force. Extra work has a bounded cost;
this feature does not promise lower total latency under production load.

## Stable alignment of sparse candidates

With `PANAKO_ROBUST_ALIGNMENT_MIN_HITS` enabled (at least six; AudioScout uses
40), complete candidates retain their original-order fits and additionally
canonicalize their timing/pitch evidence. An extra tempo/offset/pitch vote
proposes up to 64 hypotheses for the
unchanged full-resolution refinement and result gates. This recovers sparse
valid evidence that the fixed random-pair budget could miss depending on SQL
row order. Voting centers are seeds only: pitch is re-estimated from the
inliers before applying its bounds. All fits face the caller's result gates;
a short burst cannot suppress the vote. Broad-search alignment and previously
established answers remain.

The extra search applies only to complete shortlisted candidates with at most
10,000 hit rows, positive timing tolerance and at most 1,024 tempo grid steps.
Inputs outside those bounds retain the existing matcher. This is a bounded
recall improvement, not exhaustive recognition or a guarantee for every mix.
No fingerprints, score floors, tempo/pitch gates or database layout change.

Build with JDK 11 or later: `./gradlew test shadowJar`. The Gradle suite includes
synthetic sparse-consensus, input-order, noise and out-of-range checks. Do not
add private recordings or production fingerprint dumps to the source tree.

## Validation

- `QueryCleanupCheck` executes the actual matching methods: 486 unchanged
  disabled-mode golden fixtures, plus alignment, configuration lifecycle and
  candidate verification cases. The new cases cover recovery, deduplication,
  result limits, existing-answer preservation and acceptance gates.
- `CandidateRefinementCheck` uses a dedicated local PostgreSQL database. It
  proves recovery after cap truncation, packed/buffer deduplication, exclusion
  handling, duplicate-query multiplicity, shortlist and input bounds, and
  connection reuse after a verification timeout. Its database-name guard
  refuses a production connection; it clears only the isolated test database.
- Frozen-query comparisons against a live compact index recovered two known
  misses without changing their audio, extraction, broad cap or acceptance
  floor. Private evidence and operational results are recorded by the consumer.
