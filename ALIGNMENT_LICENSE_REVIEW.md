# Alignment change: source and licensing review

Reviewed 25 November 2026 before editing the engine.

The engine is the existing public AudioScout fork of Panako. Its `license.txt`
and source notices license modifications under AGPL-3.0; `MODIFICATIONS.md`
already documents the PostgreSQL store and query changes. This change will
remain in that public fork under the same license, with notices retained and
the modification recorded. No TarsosDSP or JGaborator source is changed.

The corresponding source must stay available at the exact revision used to
build the binary, including the build scripts and dependency pins. Do not ship
a private-only patch or a binary built from an unpublished working tree. The
consumer should link the public fork and pin the tested commit. Preserve the
source offer when distributing the engine; if exposing a modified version
directly to remote users, retain the source opportunity required by section 13.

The review is limited to this engine change and the existing subprocess
boundary. It does not determine the licensing of unrelated application code.
Private diagnostic recordings, credentials, host details and production raw
fingerprints are not part of this public source change; regression fixtures
must be synthetic.

References: [AGPL-3.0](https://www.gnu.org/licenses/agpl-3.0.html), sections
4–6 and 13, and the [GNU FAQ on remote interaction](https://www.gnu.org/licenses/gpl-faq.html#AGPLv3InteractingRemotely).
