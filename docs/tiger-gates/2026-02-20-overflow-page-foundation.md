# Tiger Gate Artifact: 2026-02-20-overflow-page-foundation

- Commit: `<pending>`
- Title: `Introduce versioned overflow page type and chunk primitives`
- Scope: `Adds a dedicated overflow page type and storage module for single-chunk payload + next-page chaining, with deterministic format/validation tests.`

## PR Checklist

- What invariant was added or changed?
  - `Pages can now be typed as overflow pages, and overflow page content must satisfy explicit magic/version/header bounds invariants before payload access.`

- What is the crash-consistency contract for the modified path?
  - `No mutation/WAL contract changes in this increment; this is a storage primitive foundation only. Overflow chain durability ordering is intentionally deferred to the next integration increment.`

- Which error classes can now be returned?
  - `New overflow-local structural errors are introduced in storage module (`InvalidPageFormat`, `UnsupportedPageVersion`) and capacity exhaustion (`PageFull`) for chunk writes beyond page payload capacity.`

- Does this change modify any persistent format or protocol?
  - Persistent format: `yes (new page type id .overflow and overflow page content header format with magic/version/payload_len/next_page_id)`
  - Protocol: `none`

- Which deterministic crash/fault tests were added?
  - `Added deterministic storage tests in src/storage/overflow.zig for init/read/write roundtrip, payload capacity bounds, and corrupted format rejection.`

- Which performance baseline or threshold was updated (if any)?
  - `none`

