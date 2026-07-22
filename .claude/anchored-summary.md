## Objective
Implement epoch-based generation tracking in bosk-mongo, stored in the manifest document instead of state documents, preventing flushes from completing after external delete+recreate cycles

## Important Details
- **Epoch lives in the `!Manifest` document**, not in individual state documents — shared cleanly across Sequoia and Pando
- **Epoch is a `final @Nullable String` field in `Manifest` record**, known before driver construction (read from manifest during `detectFormat()`, or generated as a UUID during first-time init)
- **A new epoch begins iff the manifest is re-created**; replacing the manifest (refurbish) carries forward the same epoch
- **Full-manifest equality** is the invariant: `validateManifestEvent()` compares the DB manifest against `expectedManifest` (which includes epoch), so any deviation triggers disconnect
- `flush()` epoch check reads the manifest document's `epoch` field via `readEpoch()`
- `Manifest.forSequoia(@Nullable String epoch)`, `Manifest.forPando(PandoFormat, @Nullable String epoch)`, `Manifest.forFormat(DatabaseFormat, @Nullable String epoch)` all require an epoch parameter
- **Refurbish** now constructs a new manifest with the preferred format while preserving the old epoch, preventing format metadata mismatch

## Work State
### Completed
- **Manifest.java**: added `@Nullable String epoch` field; updated factory methods to take epoch
- **FormatDriver.java**: added `default @Nullable Manifest manifest()`; `epoch()` now delegates to `manifest().epoch()`
- **AbstractFormatDriver.java**: removed `AtomicReference` epoch tracking; added `expectedManifest` final field; constructor takes manifest instead of epoch string; `initialDocument()` no longer writes `_epoch` to state docs; `loadAllState()` no longer reads epoch; `validateManifestEvent()` uses `this.expectedManifest` (no parameter); `readEpoch()` reads `"epoch"` field from the manifest document
- **SequoiaFormatDriver.java**: constructor takes `@Nullable Manifest`; removed `readEpoch()` override; `initializeCollection()` writes `expectedManifest` directly; removed per-document epoch check from `onEvent()`
- **PandoFormatDriver.java**: constructor takes `@Nullable Manifest`; `initializeCollection()` writes `expectedManifest`; `validateManifestEvent()` call now parameterless
- **MainDriver.java**: `newFormatDriver(DatabaseFormat, @Nullable Manifest)` and `newPreferredFormatDriver(@Nullable Manifest)` accept manifest; `detectFormat()` passes manifest from DB directly; first-time init generates UUID and builds `Manifest.forFormat(preferredFormat, uuid)`; `refurbishTransaction()` builds new manifest with preferred format and old epoch
- **Formatter.java**: removed `getEpochFromFullDocument()`; `validateManifest()` now allows `"epoch"` key in manifest
- **BsonFormatter.java**: removed `DocumentFields.epoch` enum constant
- **Cleanup**: removed unused imports (`AtomicReference`, `UUID`, `NoSuchElementException`, `getEpochFromFullDocument` references); updated `BsonStateAndMetadata` record (removed `@Nullable BsonString epoch`)
- **Tests updated**: `SchemaEvolutionTest` uses epoch from actual manifest for comparison; `MongoStatus.with()` preserves epoch from actual manifest
- **Full test suite passes (410 tests, 0 failures, 11 skipped)**

### Active
- (none)

### Blocked
- (none)

## Next Move
1. Consider adding an explicit epoch-mismatch integration test (e.g., external delete+recreate → disconnect → reconnect with new manifest epoch)
2. Update TLA+ model to reflect manifest-based epoch (the model currently uses per-document epoch)

## Relevant Files
- `bosk-mongo/src/main/java/works/bosk/drivers/mongo/internal/Manifest.java`: added `@Nullable String epoch` field; factory methods updated
- `bosk-mongo/src/main/java/works/bosk/drivers/mongo/internal/AbstractFormatDriver.java`: removed `AtomicReference` epoch tracking; added `expectedManifest` final field; `readEpoch()` reads from manifest; `validateManifestEvent()` uses `expectedManifest` directly
- `bosk-mongo/src/main/java/works/bosk/drivers/mongo/internal/SequoiaFormatDriver.java`: manifest-based constructor; removed per-doc epoch check
- `bosk-mongo/src/main/java/works/bosk/drivers/mongo/internal/PandoFormatDriver.java`: manifest-based constructor
- `bosk-mongo/src/main/java/works/bosk/drivers/mongo/internal/FormatDriver.java`: added `manifest()` default; `epoch()` delegates
- `bosk-mongo/src/main/java/works/bosk/drivers/mongo/internal/MainDriver.java`: manifest threading through `newFormatDriver`, `newPreferredFormatDriver`, `detectFormat`, first-time init, `refurbishTransaction`
- `bosk-mongo/src/main/java/works/bosk/drivers/mongo/internal/Formatter.java`: removed `getEpochFromFullDocument()`; `validateManifest()` allows `"epoch"` key
- `bosk-mongo/src/main/java/works/bosk/drivers/mongo/internal/BsonFormatter.java`: removed `DocumentFields.epoch`
- `bosk-mongo/src/main/java/works/bosk/drivers/mongo/status/MongoStatus.java`: `with()` preserves epoch from actual manifest
