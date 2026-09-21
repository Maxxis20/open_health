# Reviewed BLE transport dependency

This is the `oura-link` crate from `Th0rgal/open_oura` revision
`bd2f1508fbd571b7742b56c0bfd32d8f16e29585`, vendored for the same reason as the
sibling `oura-store`: the root Cargo patch keeps local and CI builds
reproducible without publishing an intermediate upstream revision.

Changes: a larger extended-API drain batch (see `EXT_BATCH_MAX_EVENTS`) so a
full-history sync costs round trips in the tens rather than the hundreds, and a
`path` field on `SyncOutcome` recording whether the drain used the extended or
the legacy event API.

After these changes are published upstream, replace the patch with the
published revision in the existing dependency pins and remove this directory.
