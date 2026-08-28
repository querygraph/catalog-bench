# Changelog

- C3-01 deterministic fault substrate: add a benchmark-owned HTTP reverse
  proxy with typed one-shot before-upstream and after-upstream disconnect rules,
  occurrence matching, sanitized hash-only event evidence, strict control input,
  and real-socket tests that distinguish definite non-persistence from
  accepted-state ambiguity.
- C3-01 source binding: compile the fault proxy into the exact source-built
  infrastructure image from its immutable implementation revision.
- C3-01 isolated fault topology: add a Compose overlay with one object-store
  proxy and per-catalog REST proxies, private benchmark-network upstreams,
  loopback-only control ports, and fault-specific Lakekeeper/Polaris/Gravitino/
  Nessie/LakeCat object-store endpoints.
- C3-01 retry-resistant faults: make each rule declare a bounded injection
  count, with overflow/cap validation and real-socket proof that a configured
  range disconnects retries before allowing later traffic through.
- C3-01 metadata persistence probe: add a source-built, signed-S3 probe that
  arms the proxy, performs one metadata PUT, observes the object directly, and
  emits bounded JSON proving absence before upstream admission and presence
  after an upstream success whose response was disconnected.
- C3-01 metadata-probe source binding: compile the retry-resistant proxy and
  object persistence observer together from their immutable public revision.
- C3-01 reproducible object-fault workflow: define the neutral scenario and a
  fresh-state one-command runner that verifies both persistence sides, hashes
  each artifact, rejects existing output, and removes its fixture state and
  Docker volumes.
- Fresh-run teardown now activates every declared Compose profile while
  detaching recognized prior harness projects, preventing stopped engine/catalog
  services from surviving on the fixed benchmark network; prior volumes remain
  preserved unless the owning run explicitly removes them.
- C3-01 runnable profile: pin the exact source, local image, executable, binary,
  and scenario hashes for the verified Linux ARM64 metadata fault topology.
- Permit non-catalog fault-injection profiles to omit the Iceberg REST adapter
  vocabulary when they contain no catalog component or adapter declarations;
  catalog-bearing profiles retain exhaustive capability coverage.
- Publish the reviewed `objfault_0828a` source evidence and reproduction guide,
  explicitly limiting it to the C3-01 injection substrate rather than claiming
  catalog recovery or performance.
- C3-02 recovery probe foundation: add a dependency-free protocol-native
  Iceberg REST client that negotiates routing/OAuth, injects lost commit
  responses, reconciles through a direct endpoint, performs exact retry and
  advertised idempotency-drift checks, cleans fixtures, and emits sanitized
  evidence. Catalog proxy data listeners remain loopback-only.
- Pin the fault overlay's LakeCat service to the accepted `b8be6bc9` staged-
  create revision and explicit shared-MinIO warehouse root without changing the
  already-published Phase 1/2 base topology.
- C3-02 common recovery workflow: define the engine-neutral response-loss
  scenario and a fresh-state four-catalog runner that verifies direct-state
  reconciliation, exact retry, optional idempotency drift behavior, sanitized
  fault events, fixture cleanup, and project-volume cleanup.
- C3-02 deterministic in-flight gate: add a typed `during-upstream` rule that
  transmits the first request-body byte, records a sanitized pause event, and
  blocks the remainder until an explicit control-plane release. Real-socket
  tests prove the upstream cannot complete before release.
- C3-02 immutable gate delivery: advance the shared infrastructure image and
  deployment audit to the reviewed in-flight-gate source revision.
- C3-02 restart recovery workflow: extend the protocol-native four-catalog
  scenario with a deterministic mid-body pause, target-service restart,
  direct durable-state reconciliation, exact commit retry, and cleanup proof.
- Refresh an OAuth bearer after catalog restart so the Polaris recovery case
  measures durable commit behavior instead of the process-local token epoch.
- Wait for the restarted OAuth endpoint before refreshing the bearer; Compose
  restart completion precedes application-level Polaris readiness.
- Preserve comparative restart failures as evidence instead of aborting the
  matrix: a catalog may lose the run-owned fixture, receive a 404 exact retry,
  and still permit the remaining catalogs and fresh-state cleanup to run.
- Publish the reviewed `restart_0828d` four-catalog recovery matrix, raw
  sanitized artifacts, exact hashes, cleanup proof, and configuration-scoped
  Polaris and Lakekeeper findings.

- Keep the contract's OAuth scope in evidence while omitting DuckDB's
  unsupported `SCOPE` attach option; Polaris applies the principal-role scope
  for the benchmark credential.

- Decode DuckDB CLI setup and query result arrays as a bounded JSON stream and
  retain only the final query result.

- Use DuckDB's accepted in-memory `CREATE SECRET` grammar for each isolated CLI
  invocation.

- Materialize the source-complete DuckDB composite image, runner, connector
  stack, and exact observed artifact digests into a runnable four-catalog
  profile.

- Admit DuckDB's release-pinned Iceberg, HTTPFS, and Avro extension stack as
  the engine-owned connector identity without weakening Java-engine checks.

- Bind the verified DuckDB runtime to the exact catalog-bench engine runner
  revision in a minimal composite benchmark image.

- Execute DuckDB's full stock Iceberg REST workflow through its CLI, inject
  credentials only over child stdin, and cross-check table metadata through
  independent REST and object-store observations.

- Add a closed, catalog-neutral DuckDB execution plan and renderer for the full
  namespace, table, append, read, schema-evolution, and snapshot workflow.

- Add a Linux ARM64 DuckDB 1.5.3 source build with the release-pinned Iceberg,
  HTTPFS, and Avro revisions packaged as checksum-locked signed offline
  extensions.

- Admit the canonical stock-Trino launcher in reviewed engine evidence, enabling
  validated Trino bundle publication through the existing engine pipeline.

- Advance the Trino runner to the committed relative-snapshot and compressed
  metadata observer before collecting replacement evidence.

- Decode standard gzip-compressed Iceberg metadata under the same bounded,
  strict projection used for JSON metadata, enabling Lakekeeper observation.

- Reconcile stock-engine snapshot counts relative to the table-creation
  baseline, covering engines such as Trino that create an initial snapshot.

- Advance the Trino runner to the committed warehouse-root topology and corrected
  schema-evolution implementation before collecting fresh four-catalog evidence.

- Add a Trino-only LakeCat `b424f778` service with a configured S3 warehouse
  root, preserving the already-published Spark/Flink LakeCat image while Trino
  exercises standard REST warehouse selection without explicit table location.

- Render Trino schema evolution with the required `ALTER TABLE ... ADD COLUMN`
  grammar, covered by the catalog-neutral program test.

- Advance the runnable Trino profile to `catalog-bench@6ea0f803` and bind the
  closed scenario-property allowlist to the rebuilt runner and composite stock
  Trino image identities.

- Derive Trino's closed `iceberg.allowed-extra-properties` connector allowlist
  from the scenario property oracle so the common table contract is admitted
  identically by every REST catalog.

- Advance the runnable Trino profile to `catalog-bench@eeac1003` and bind the
  corrected readiness timeout to the rebuilt runner and composite stock Trino
  image identities.

- Give the stock Trino CLI readiness query an independent bounded execution
  timeout instead of accidentally limiting each JVM invocation to the 250 ms
  polling interval; cover the boundary with a delayed-success regression.

- Advance the runnable Trino profile to `catalog-bench@f9097f77` and bind the
  successful-query readiness supervisor to its rebuilt runner and composite
  stock Trino image identities.

- Make Trino readiness depend on successful execution of the fixed stock-CLI
  `SELECT 1` probe instead of its presentation format; benchmark reads and
  scalar observations retain their strict bounded JSON decoders.

- Advance the runnable Trino profile to `catalog-bench@33b3d656`, including
  the corrected stock-server node environment, and bind it to the rebuilt
  optimized runner and composite Trino image identities.

- Correct the generated Trino node environment from `catalog-bench` to
  `catalog_bench`; Trino 483 requires `[a-z0-9][_a-z0-9]*` and rejected the
  hyphenated value during configuration admission.

- Advance the Trino materialization to `catalog-bench@836a3cd0`, which binds
  Airlift's private data directory before server startup. Both earlier Trino
  diagnostic runs remain excluded from publication.

- Pass Trino's private staged data directory through Airlift's `--data-dir`
  launcher option. Airlift resolves its PID/log paths before Trino expands
  `${ENV:...}` in `node.properties`; without the explicit option it attempted
  to create a literal root-level environment-reference directory and exited.

- Advance the materialized Trino runner pin to `catalog-bench@0bbf0c40`, the
  verified launcher-grammar correction, before producing any replacement live
  evidence. The earlier `6131423f` image remains diagnostic-only.

- Correct the stock Airlift launcher grammar from live Trino 483 help output:
  global `--etc-dir` must precede the `run` command. The previous order caused
  all four diagnostic executions in `trino_0828070341` to fail before startup;
  that run is not publication evidence.

- C2-06 fresh Trino launcher: build and independently admit the pinned profile,
  create one run-owned Docker topology, execute the same stock Trino workflow
  against LakeCat, Polaris, Gravitino, and Lakekeeper, and require a complete,
  exit-consistent transcript set before reporting success.

- C2-06 live Trino profile materialization: bind the exact built runner donor,
  independently labeled stock Iceberg plugin donor, and final Trino image to
  their live ARM64 image IDs, labels, artifact digests, and byte counts. Add
  byte-for-byte rerender/check tests and prove CLI dispatch from the runnable
  profile selects Trino.

- C2-06 deterministic Trino profile policy: require the exact source-bound
  runner, Trino server shell launcher, native ARM64 launcher, stock CLI, and
  Iceberg 1.11.0 core/AWS plugin bytes from the one executed image, with exact
  base-image and runner-source labels and no artifact-copy assumptions.

- C2-06 stock Trino image topology: compose the exact pinned Linux ARM64 Trino
  483 child with only the source-bound optimized Rust runner, verify the base
  descriptor and platform before construction, and expose a hardened one-shot
  engine service on the shared catalog and MinIO network.

- C2-06 Trino transcript dispatch: select the production stock Trino runner
  whenever the runnable profile resolves a Trino execution plan, while retaining
  the same negotiation, three-authority reconciliation, cleanup, sanitization,
  and immutable transcript pipeline used by Spark and Flink.

- C2-06 production Trino effects: wire the verified stock Trino server and CLI
  into the closed state machine, admit credentials without serializing them,
  probe the live engine version, execute catalog-neutral SQL, decode bounded
  reads, and independently observe each catalog-returned metadata pointer via a
  confined object-store read and strict Iceberg v2 decoder.

- C2-06 confined metadata retrieval: extend the shared object-store auditor
  with a size-checked read that accepts only an Iceberg metadata pointer inside
  its validated table root, rejects bucket/path escape, and rechecks the final
  payload bound before returning bytes to an engine observer.

- C2-06 Trino launcher secret boundary: introduce a validated, redacted,
  zeroizing server environment and inject only the generated node/data values,
  S3 credentials, and optional REST OAuth credential after process environment
  sanitization. Tests prove the exact values reach the stock launcher without
  appearing in debug output and reject empty or unsafe inputs.

## Unreleased

- C3-04 cold-state helper: add a dependency-free, source-built volume archive
  command with exclusive backup creation, empty-target restore, path traversal
  rejection, symlink rejection, and round-trip tests for run-scoped catalog
  state volumes.

- Add the exact-source DuckDB 1.5.3 four-catalog launcher and runnable profile,
  including official signed offline extensions, bounded result decoding, OAuth
  negotiation, and a passing fresh LakeCat/Polaris/Gravitino/Lakekeeper run.
- Admit DuckDB's canonical launcher to reviewed engine evidence and publish the
  independently validated four-pass run as an immutable correctness bundle.

- Correct Trino 483 launcher provenance from the live pinned ARM64 image:
  `/usr/lib/trino/bin/launcher` is an engine-owned Bash architecture selector,
  and the executed `/usr/lib/trino/bin/linux-arm64/launcher` is a separate
  native ELF. Require both exact nonempty artifacts instead of misclassifying
  the wrapper as Python or omitting the program it executes.

- Publish the reviewed fresh stock-Flink v2 correctness bundle from run
  `flinkv2_08280635`: LakeCat, Polaris, Gravitino, and Lakekeeper each pass all
  required assertions. Generalize review admission to require the canonical
  launcher selected by the evidenced engine rather than hard-coding Spark.

- Rematerialize the local-target Flink runtime at `catalog-bench@ce0c11f` and
  verify every source, image, and embedded-artifact identity. A direct complete
  LakeCat v2 workflow now passes writes, reads, schema evolution, three-way
  state reconciliation, shared-object checks, and cleanup.

- Execute the one-shot stock Flink child with its supported `local` deployment
  target. Retained diagnostic logs proved the default target submitted INSERT
  jobs to an absent remote JobManager at `0.0.0.0:8081`; the exact invocation
  contract now requires the catalog-neutral local target.

- Rematerialize the Flink candidate at `catalog-bench@df38c81` with the admitted
  Hadoop client pair. The source-derived profile, image identities, eight
  runtime artifacts, and live byte checks now agree; a stock SQL-client probe
  successfully creates and selects the LakeCat REST catalog.

- Add checksum-locked Hadoop 3.4.3 client API and runtime JARs to the Flink
  connector/runtime boundary. Stock Flink initialization proved Iceberg's REST
  catalog factory requires Hadoop configuration classes; materialization now
  requires both source artifacts and byte-identical runtime copies.

- Advance and rematerialize the unpublished Flink profile at
  `catalog-bench@701f2b9`, binding the profile-aware CLI dispatcher into the
  optimized runner and final stock runtime with newly observed image and ELF
  digests. Live artifact verification passes for every copied byte.

- Dispatch the production engine CLI from the profile-selected engine instead
  of unconditionally invoking the Spark executor. Flink profiles now use the
  verified Flink process path, with a CLI regression test that distinguishes
  correct runtime admission from the former execution-plan mismatch.

- Add the fresh four-catalog Flink production launcher. It rebuilds and verifies
  the exact materialized images, creates isolated run-owned state, executes the
  same v2 workflow sequentially through LakeCat, Polaris, Gravitino, and
  Lakekeeper, and requires complete exit/transcript classification agreement.

- Materialize the buildable Flink 2.1.3 runtime: bind the exact stock ARM64
  image, Iceberg 1.11.0 JARs, optimized Rust harness, source-bound Java effects
  JAR, current LakeCat image, and every copied artifact byte into a verified
  deterministic runnable profile. No behavioral result is claimed yet.

- Advance the unpublished Flink candidate and Compose source pin to
  `catalog-bench@df3a68da`, the first revision whose pinned-JDK build topology
  can actually compile the Java effects runner. No prior Flink materialization
  or result exists, so the corrected draft supersedes the unbuildable pin.

- Build the source-bound Flink Java effects runner in an immutable Linux ARM64
  Eclipse Temurin 17 JDK stage instead of the stock Flink JRE image. The first
  production build proved Maven had no `javac`; the executed runtime remains the
  exact admitted stock Flink child and receives only the compiled runner JAR.

- Add an immutable Flink 2.1.3 source candidate that preserves the admitted
  `catalog-bench@36906515` runner while advancing only LakeCat to `65f0a4c3`,
  the revision proven by Spark to support catalog-owned field IDs and multipart
  REST namespace probes. The preserved 2026-08-27 candidate remains unchanged.

- Publish the reviewed fresh stock-Spark v2 correctness bundle from run
  `sparkv2_08280548`: LakeCat, Polaris, Gravitino, and Lakekeeper each pass all
  14 required assertions. Archive the four sanitized transcripts and review,
  bind exact profile/scenario/runtime identities, and deterministically emit
  four records, an unranked matrix, and a complete immutable manifest.

- Advance the source-built LakeCat image to `lakecat@65f0a4c3`, whose REST
  table routes decode multipart namespaces for Spark's standard metadata-table
  probe. Preserve both earlier v2 generations while admitting this exact
  second repair as a new source profile.
  Materialize the launcher's exact ARM64 OCI identity and service ELF, derive
  and verify the runnable profile, and advance both host and in-container
  launcher selection to this generation.

- Pass the newly materialized LakeCat repair profile to the in-container Spark
  runner as well as the host-side artifact verifier. Deployment coverage now
  rejects the superseded runner profile path so a launch cannot produce
  split-brain evidence that claims the old LakeCat revision.

- Advance the source-built LakeCat benchmark image to `lakecat@5d62f1c4`,
  which assigns catalog-owned positive Iceberg field IDs for stock Spark
  `createTable` requests. Preserve the first v2 runtime generation while
  admitting a new immutable source profile for the corrected four-catalog run.
  Materialize its exact ARM64 OCI identity and 19,691,168-byte service ELF,
  deterministically derive the runnable profile, verify every selected live
  image/artifact, and advance the Spark launcher to this additive generation.
  Record the launcher's attestation-free image identity, while separately
  confirming its embedded service bytes match the provenance-bearing build.

- C2-02 JVM native-library runtime boundary: preserve read-only Spark/Flink
  containers while marking only their bounded 512-MiB ephemeral `/tmp` tmpfs as
  executable. A fresh stock-Spark reproduction proved the no-exec mount caused
  `zstd-jni` to fail mapping its Linux ARM64 library at the first Parquet write;
  deployment coverage now freezes the narrow executable tmpfs requirement.

- C2-02 Spark v2 runtime materialization: build the optimized Linux ARM64 runner
  from public `catalog-bench@59840b95c33e`, prove its 4,986,064-byte ELF is
  byte-identical in the donor and Spark 4.1.3 images, record exact OCI identities
  and labels for every selected image, and deterministically generate the new
  runnable `spark-v2-2026-08-28` profile. The artifact verifier and separate v1
  and v2 policy tests preserve both generations; no behavioral result is yet
  claimed.

- C2-02 immutable Spark v2 source selection: preserve the prior v1 profile and
  materialization, add a distinct draft v2 source profile, and pin the optimized
  runner donor plus Spark copy to public `catalog-bench@59840b95c33e`. Advance
  the materialization policy scope and production launcher paths to new v2
  artifacts, which intentionally do not exist until independently observed.

- C2-02 Spark image build topology: make the standalone builder default to the
  complete catalog profile set required by Compose dependency validation while
  preserving an explicitly supplied profile set from the fresh-run launcher.
  Deployment tests freeze the dependency-complete default and shell syntax.

- C2-02 Spark v2 launch contract: advance the four-catalog production launcher
  to the current engine interoperability v2 scenario and freeze that exact path
  in deployment tests, while explicitly rejecting the superseded v1 selection.
  The existing runnable profile still binds prior optimized runner bytes and
  must be freshly materialized before execution; no result claim is created by
  this launcher-only unit.

- C2-04 supervised stock Trino server lifecycle: start only the closed verified
  launcher against the private staged configuration, discard server output,
  retain the isolated process-group boundary, and wait within positive bounds
  for a typed stock-CLI `SELECT 1` readiness result. Classify invalid limits,
  spawn failure, early server exit, probe-construction failure, and readiness
  timeout without retaining child output; terminate and reap the complete
  process group on explicit shutdown, timeout, or drop. Fake launcher/CLI tests
  prove delayed readiness, exact probe retries, early exit, bounded timeout,
  invalid configuration, and post-shutdown process removal. Concrete Trino
  effects and live interoperability evidence remain separate.

- C1-09 Phase 1 behavioral publication: archive a fresh optimized 5-scenario by
  5-catalog matrix covering config, namespace, table, deterministic commit, and
  stock PyIceberg workflows; bind all 25 value-safe transcripts to a runnable
  artifact-resolved profile and human-reviewed runtime/redaction sidecar; and
  deterministically emit 25 correctness results plus one immutable manifest.
  The admitted matrix has 20 pass and five fail outcomes, preserves every
  optional not-evaluated assertion, and makes no timing or resource claim.

- C1-09 cross-scenario publication gate: add one-command `smoke` and `full`
  result checks, discover and validate every checked-in immutable bundle,
  recursively secret-scan manifests plus all referenced profiles, scenarios,
  results, raw source evidence, and result evidence, and generate the bundle
  index and known-gaps report strictly from admitted records. The full profile
  first recomputes the historical, production-contention, and Phase 1
  behavioral bundles. Mutable operation-level transcripts remain outside
  publication unless admitted through a reviewed immutable bundle.

- C2-06 private Trino server staging: materialize the closed source-derived
  Trino configuration into a create-new temporary tree with `0700` directories
  and `0600` files, fsync each file before launch, reject non-normal relative
  paths, expose only the configuration and data roots needed by the pinned
  launcher, and remove the complete private tree on drop. Regression coverage
  binds the staged bytes to the typed LakeCat plan and proves credential values
  remain environment references rather than persisted configuration.

- C2-06 bounded stock Trino CLI execution: execute the closed CLI invocation in
  the shared cleared-environment, private-home, null-input/error, isolated
  process-group boundary with one positive timeout and caller-selected output
  limit up to 16 MiB. Drain stdout concurrently, terminate the whole group on
  timeout or excess output, require a successful exit, and retain no stderr.
  Separate fake-executable tests cover exact capture, private environment,
  excess output, nonzero exit, timeout, and invalid limits. Server lifecycle
  and concrete Trino effects remain separate.

- C2-06 closed stock Trino invocation grammar: represent server startup only as
  the verified launcher `run --etc-dir <private absolute path>` and every query
  only as a bounded single stock-CLI batch against the fixed loopback server,
  benchmark user/catalog/source, disabled progress, and either JSON or discard
  output. Reject relative or control-bearing paths and empty, control-bearing,
  or over-1-MiB SQL before process creation. Separate tests freeze exact
  arguments and malformed inputs. This unit starts no process and retains no
  output.

- C2-06 strict Trino CLI scalar decoder: admit preflight counts and immutable
  metadata locations only as one bounded JSON object, one closed identifier
  column, one exact unsigned integer or nonempty control-free text value, and
  one final LF. Reject output over 64 KiB, text over 4 KiB, duplicate or extra
  columns, multiple rows, malformed shapes, and type drift. Separate tests
  cover exact values and every boundary; no raw CLI output enters evidence.

- C2-06 bounded Iceberg metadata projection for Trino: decode at most 4 MiB of
  the immutable metadata object located through Trino's stock
  `$metadata_log_entries` table, require canonical UUID, format v2, same-table
  S3 metadata ancestry, current struct schema, scenario-exact initial or evolved
  fields/IDs, valid snapshots, and bounded string properties. Emit only the
  closed table observation and match/mismatch for scenario-owned properties;
  unknown properties are discarded. Extract a recursive duplicate-key-rejecting
  JSON decoder shared with stock CLI rows. Separate tests cover initial/evolved
  projection, property redaction, identity/location/schema/snapshot drift,
  duplicate keys, malformed JSON, and the byte limit. Object retrieval and live
  Trino effects remain separate.

- C2-06 strict Trino CLI read decoder: consume Trino 483's stock JSON output as
  one bounded object per newline, reject missing final LF, malformed or blank
  rows, duplicate/missing/extra columns, nested values, excess rows, and input
  over 16 MiB. Reconstruct the shared compact JSON-array-per-row canonical bytes
  in oracle column order and expose only rows, bytes, and SHA-256 for the child
  state machine's exact oracle comparison. Separate tests cover key-order
  independence, duplicate and shape rejection, row/byte bounds, trailing LF,
  and the zero-row identity. No raw CLI output enters evidence.

- C2-06 Trino child state machine: add a transport-free `TrinoEffects` algebra
  and deterministic runner over the closed eight-operation program. Runtime,
  catalog initialization, fixture preflight, namespace/table creation,
  appends, canonical reads, schema evolution, snapshot counts, final
  observation, and terminal classification now emit the shared engine event
  vocabulary in decoder-valid order. Collision stops before mutation; read
  evidence requires exact oracle equality; every effect failure maps to one
  fixed stage/category without retaining details. Separate tests cover complete
  order, collision non-mutation, read mismatch, malformed operation order, and
  effect failure. This pure state machine has no launcher/CLI effects and makes
  no runtime claim.

- C2-06 stock Trino launcher provenance: require the engine image to contain
  Trino 483's nonempty engine-owned `/usr/lib/trino/bin/launcher` Python program
  in addition to `run-trino` and the CLI JAR. The process adapter needs the
  underlying stock launcher because `run-trino` hardcodes `/etc/trino`, while
  benchmark configuration is staged into a private run-owned `--etc-dir`.
  Synthetic materialized-profile tests reject an absent or wrongly attributed
  launcher before credentials, staging, or process creation. This provenance
  unit starts no process and makes no runtime claim.

- C2-06 closed Trino server configuration: deterministically project the typed
  rendered program into a complete private `--etc-dir` tree using the exact
  Trino 483 single-node and JVM defaults, fixed task concurrency, static Iceberg
  catalog, native S3 settings, and environment-bound node/data identities.
  Catalog OAuth and S3 credentials appear only as Trino's documented
  `${ENV:…}` secret references; no value is written to configuration. Property
  names and values are bounded and injection-safe, files have a closed sorted
  path set, and separate tests cover anonymous/OAuth configurations, the pinned
  JVM boundary, required references, and malformed-property rejection. This
  pure unit neither writes files nor starts a process.

- C2-06 catalog-neutral Trino renderer: translate the closed Trino plan into
  exact Trino 483 REST-catalog and `fs.s3.enabled` properties plus an ordered
  eight-operation SQL program for schema/table creation, deterministic appends
  and canonical reads, additive evolution, and `$snapshots` inspection. Preserve
  scenario-owned Iceberg properties through Trino's `extra_properties` map,
  carry only typed secret-free authentication setup, and reject execution,
  routing, policy, identifier, generator, or file-I/O drift before mutation.
  Extract overflow-checked row and insert generation into one shared Rust module
  used by Flink and Trino. Separate tests cover all four catalogs, exact SQL and
  oracles, closed serialization, authentication, drift, and absence of catalog
  branches or direct transports. This unit does not start Trino or claim a
  runtime result.

- C2-06 closed Trino execution policy: add a typed Trino 483 plan variant over
  the shared catalog, fixture, scenario, authentication, and object-store
  representations. Preserve Trino's actual `fs.s3.enabled` configuration as its own
  file-I/O ADT instead of mislabeling it as Iceberg `S3FileIO`; require the
  stock server launcher, engine-owned CLI JAR, source-correlated optimized Rust
  runner, and byte-correlated Iceberg 1.11.0 connector artifacts before plan
  construction. Runtime admission requires exactly Trino 483 and Java 25.0.3.
  Separate synthetic-profile tests prove the closed plan and reject Java or CLI
  drift. This policy-only unit has no Trino renderer, process adapter, image,
  runtime result, or ranking claim.

- C2-06 immutable Flink candidate: preserve the broad stock-engine and
  already-materialized Spark inputs byte-for-byte while deriving a dedicated
  Flink 2.1.3 candidate whose only semantic changes are its document identity
  and the `catalog-bench-engine` version/source revision advanced to
  `36906515b69a61ac26d44327b2a9ff94c2b84551`. A separate projection test
  reconstructs the candidate from the broad profile and rejects any unrelated
  catalog, engine, connector, topology, build, or policy drift. Flink policy
  tests now consume the checked-in candidate directly. The profile remains a
  draft source contract and makes no image-build or runtime claim.

- C2-06 typed Flink profile materialization: add a deterministic projection
  from a dedicated source-bound Flink candidate and strict image-observation
  sidecar into the runnable v2 interoperability profile. The policy requires
  the audited stock Flink 2.1.3 Linux ARM64 child, Iceberg 1.11.0 connector
  coordinates, the optimized Rust runner, and the Java child JAR; all four
  donor-to-runtime artifact copies must retain exact digest, byte count, and
  media type. New `materialize-flink` and `check-flink` commands expose the
  shared pure materializer, while separate tests prove the selected topology
  and reject base-image, runner-copy, or connector drift. This unit defines and
  verifies materialization policy only: it neither rewrites the immutable Spark
  candidate nor claims that the Flink images have been built or executed.

- C2-06 Flink image topology: add a non-destructive launcher that resolves the
  selected Linux ARM64 child from the immutable Flink index, pulls that child
  by digest, verifies its descriptor and platform, and creates a local build
  indirection only after both identities agree. Wire source revision
  `36906515b69a61ac26d44327b2a9ff94c2b84551` into separate optimized-Rust,
  connector, composite-runner, and executed-Flink Compose services. The final
  harness and child share one hardened, read-only runtime with the same catalog
  topology and MinIO; the already materialized Spark donor remains unchanged.
  Static Rust tests and daemon-free Compose validation cover the exact sources,
  services, profiles, entry points, and absence of destructive Docker actions.
  No image was built while the local Docker filesystem remains unsafe.

- C2-06 checksum-locked Flink image definition: add a four-stage BuildKit
  boundary that compiles and tests the Java child inside the exact Flink Java
  17 image using SHA-512-locked Maven 3.9.16 with strict repository checksums;
  downloads the Iceberg Flink 2.1 runtime and AWS bundle under exact SHA-256;
  combines the JAR with the separately optimized, source-pinned Rust donor; and
  copies those byte-identical artifacts into the executed stock Flink image.
  Both donor and runtime compare their independent source-revision files, while
  the runtime labels the audited Linux ARM64 Flink child digest and upstream
  revisions. A separate Rust audit freezes checksums, paths, non-root runtime,
  and absence of unverified package/download commands. Docker execution remains
  intentionally pending while the local Docker filesystem is unsafe, so this
  definition creates no materialization or runtime result.

- C2-06 stock Flink child effects: add the Java 17 entry point and a catalog-
  neutral `EngineEffects` implementation over Flink 2.1.3's batch Table API and
  Iceberg 1.11.0's Flink 2.1 catalog. The child creates the catalog through a
  `CatalogDescriptor`, injects OAuth only into its in-memory factory options,
  executes every mutation and read through stock Flink SQL, observes the table
  through that same `FlinkCatalog`, and bounds canonical compact-JSON-lines
  reads by their row and byte oracles. It emits only sanitized S3 routes,
  expected-property agreement, typed schemas, snapshot counts, runtime
  identity, and closed failures. Flink and Iceberg are compile-only/provided
  dependencies so the source-bound JAR cannot shadow the selected engine
  runtime; `-Xlint:all -Werror` and seventeen separate Java tests cover decoding,
  orchestration, credentials, canonical identity, bounds, and arguments. This
  unit has not run against the production Docker topology and is not runtime or
  ranking evidence.

- C2-06 Flink child protocol state machine: add a Java-side `EngineEffects`
  algebra, typed `ChildEvent` vocabulary, bounded JSON-lines `EventSink`, and
  deterministic `ProgramRunner` that executes the closed operation ADT in the
  shared runtime/catalog/preflight/namespace/table/append/read/evolve/final
  order. Fixture collision exits before mutation; read evidence is emitted only
  after exact oracle agreement; every effect failure maps to the existing
  closed stage/category pair without serializing exceptions; and event encoding
  is capped at 16 KiB. Nine Java tests now cover decoding plus complete event
  order, collision non-mutation, read mismatch, namespace observation failure,
  oversized events, and absence of raw/private diagnostics. This pure state
  machine still has no Flink implementation and makes no runtime claim.

- C2-06 strict Flink child decoder: add a Java 17 Maven module for the
  source-bound child artifact with sealed authentication and operation models,
  a bounded regular-file decoder, duplicate-key/unknown-field/trailing-token
  rejection, constrained JSON depth/text/numbers, exact eight-operation order,
  additive-only SQL effect shapes, closed catalog properties, credential-free
  HTTP/S3 routes, schema and read-oracle agreement, and credential-shaped key
  rejection. Pin Jackson 2.18.2—the version selected by Flink 2.1.3—plus every
  build/test plugin, relocate the private Jackson copy, and fix archive output
  time. Separate JUnit tests cover the valid Rust wire shape and malformed,
  duplicate, reordered, secret-bearing, unsafe-route, invalid-oracle,
  oversized, empty, and symlinked inputs. Two clean Java 17 builds produced an
  identical shaded JAR. This decoder-only unit contains no Flink dependency,
  engine effect, event emitter, or runtime result.

- C2-06 Flink process adapter: add `FlinkProcessExecutor` and
  `StockFlinkRunner` over the existing engine-neutral process evidence and
  workflow boundary. The adapter verifies all profile artifacts before staging
  or secret reads, renders one closed program JSON file, invokes the fixed
  source-bound child JAR through the stock `/opt/flink/bin/flink run` CLI, maps
  only allowlisted public environment plus zeroized child-only credentials, and
  reuses Spark's process-group timeout, bounded stdout drain, event decoder, and
  terminal classifier. Add a closed `render-plan` preparation failure rather
  than collapsing renderer rejection into encoding. Separate fake-CLI tests
  prove exact arguments, staged oracle presence, secret absence, OAuth/S3 child
  environment, collision classification, pre-effect runtime rejection, and
  timeout validation. This unit does not yet provide the Java child JAR or
  execute Flink, so it creates no runtime result.

- C2-06 closed Flink child envelope: replace freely paired statement-purpose
  records with a tagged `FlinkOperation` ADT whose read variants carry their
  exact row/byte/SHA-256 oracles. Extend the rendered program with a bounded
  fixture target and observation policy containing only the expected format,
  initial fields, evolved field, and scenario-owned properties. The future
  child therefore receives one self-contained, secret-free effect program and
  cannot reach back into the broader scenario, infer expected reads, or pair a
  read oracle with a mutating operation. Round-trip and unknown-field tests
  freeze the closed wire shape. This structural unit launches no process and
  changes no result.

- C2-06 source-bound Flink runner policy: require every runnable Flink profile
  to contain both the stock engine-owned `/opt/flink/bin/flink` CLI and one
  byte-identical copy of the catalog-bench Flink runner JAR in the selected
  engine and source-bound runner images. Generalize runner artifact correlation
  from one ELF to a nonempty set while preserving the exact optimized Rust ELF
  requirement. The JAR must be a nonempty Java archive at its fixed image path
  and attributed to exactly the runner and selected engine; missing runner
  provenance, missing copies, media-type drift, and byte drift fail during plan
  construction, before credentials or a child process. Synthetic materialized
  profile tests cover these gates. No JAR or runnable Flink image is
  materialized by this policy-only unit.

- C2-06 catalog-neutral Flink renderer: translate the closed Flink plan into a
  typed, secret-free catalog setup and an ordered stock Flink SQL program for
  namespace/table creation, both deterministic appends and reads, additive
  schema evolution, and snapshot metadata inspection. Shared scenario data
  drives types, requiredness, properties, locations, generators, columns, and
  ordering; SQL identifiers use a closed vocabulary and literals are escaped.
  Rendering rejects plan-format, execution, policy, endpoint, file-IO, fixture,
  and generator drift before mutation. OAuth mode remains a typed setup value,
  while credentials and object-store keys are deliberately absent for the
  future process adapter to inject without serialization. Separate tests cover
  all four profile catalogs, exact operation order and generated boundary rows,
  authentication, escaping, drift, and absence of catalog branches or direct
  transports. This validation-only unit does not execute Flink or claim a
  result.

- C2-06 closed Flink execution policy: extend the engine execution-plan ADT
  with an Apache Flink 2.1.3 variant while sharing the catalog-neutral REST,
  authentication, S3FileIO, fixture, and scenario representations already used
  by Spark. Profile dispatch now selects only the exact supported engine and
  Iceberg 1.11.0 connector, requires a nonempty engine-owned stock Flink CLI,
  and matches the closed Flink/Java/Scala runtime identity. Focused tests derive
  a secret-free Flink plan from a synthetic materialized profile and reject
  dependency and artifact drift. Pinned Iceberg source confirms that its Flink
  2.1 catalog converts `TableChange.AddColumn` into an Iceberg `UpdateSchema`,
  despite the pinned DDL prose still describing property-only alteration. This
  unit establishes policy and provenance only; it does not add a renderer,
  process adapter, materialized profile, or interoperability result.

- C2-06 explicit engine selection: add a role-validated
  `InteroperabilityPlan::from_contracts_for_engine` constructor for candidate
  profiles that intentionally contain several `stock-engine` services. The
  existing constructor remains the unambiguous convenience path for runnable
  single-engine profiles. Explicit selection requires exactly one service that
  binds the requested engine ID and still passes renderer-specific version,
  connector, artifact, and runner-copy policy; unrelated components cannot be
  selected by ID alone. Focused tests cover singular-profile rejection,
  successful Spark selection from a multi-engine profile, unsupported Flink
  dispatch, and non-engine-role rejection. This is selection infrastructure,
  not a Flink or Trino runtime claim.

- C2-06 engine-neutral runtime identity: replace Spark-, Scala-, and Java-named
  event fields with an engine version plus an exact dependency map, and move
  expected runtime matching behind the selected `EngineExecutionPlan` variant.
  Shared reconciliation now knows only the selected plan and normalized
  platform; the Spark renderer alone requires and emits the closed `java` and
  `scala` dependency set. Missing, extra, legacy-shaped, or version-drifted
  identities fail closed. Because this intentionally changes persisted event
  JSON, advance the common scenario and transcript format to v2 instead of
  accepting an ambiguous untagged compatibility shape. The immutable runnable
  profile remains honestly scoped to v1 until a fresh optimized v2 runner image
  is materialized and verified. No v1 production Spark result had been
  published, and this unit does not claim a new runtime result.

- C2-06 engine-neutral execution policy: store renderer-specific policy in the
  `EngineExecutionPlan` algebraic data type and expose common fixture and
  scenario views from `InteroperabilityPlan`. Catalog projection, independent
  reconciliation, transcript construction and validation, sanitization, and
  test support no longer reach through a Spark plan for shared semantics. The
  Spark adapter explicitly selects and serializes only the Spark variant, with
  a closed preparation-failure category for an adapter/plan mismatch. Existing
  Spark plan and transcript JSON remain unchanged. This creates the typed
  extension seam for later Flink and Trino variants; it does not claim either
  runtime or result yet.

- C2-06 engine-neutral process evidence: extract credential and preparation
  failures, terminal outcomes, runtime verification, event capture, exit code,
  and elapsed-time evidence from the Spark process adapter into a reusable
  `EngineProcessExecution` algebraic data type. The common `EngineRunner`,
  reconciliation workflow, transcript, and result materializer now depend only
  on that neutral boundary; `SparkProcessExecutor` is one producer rather than
  the owner of the evidence vocabulary. The shared event protocol centrally
  owns its `0`/`2`/`3` terminal mapping, existing JSON labels remain unchanged,
  and formerly unit-shaped terminal variants now reject stray fields. A focused
  wire-format test freezes every no-detail status plus bounded credential,
  preparation, and engine-failure shapes. This is a structural prerequisite for
  Flink and Trino, not an interoperability result for either engine.

- C2-05 deterministic Spark result materialization: add create-new
  `engine-import write` and exact `engine-import check` commands that accept
  only a reviewed profile, scenario, sidecar, and complete transcript set
  archived in the repository's public evidence boundaries. The importer copies
  those exact bytes into a self-contained bundle, independently projects all
  fourteen scenario assertions into one result per profile catalog, and emits a
  validated manifest plus generated correctness matrix. A result passes only
  when every required assertion and the process terminal are trusted; observed
  assertion failures remain failures, an untrusted terminal after otherwise
  successful checks is a harness failure, and a pre-existing fixture collision
  remains `not-tested`. Results contain no measurements and the matrix contains
  no rank. The checker recomputes every byte and rejects missing, extra,
  modified, symlinked, or nonregular output entries.

- C2-05 reviewed Spark live-run envelope: add a bounded, closed review sidecar
  that drives evidence admission from its own source locations and binds exact
  profile, scenario, and transcript bytes to one fixture, canonical launcher
  invocation, calendar-validated UTC interval, profile-matching execution
  environment, and completed redaction review. Portable normalized paths,
  deterministic catalog ordering, exact container-runtime capture, and an
  output destination below `results/v1` fail closed; only the resulting typed
  review can enter the forthcoming deterministic result materializer. The
  contention importer now shares the same strict UTC timestamp parser, and
  environment manifests expose their existing semantic validation directly.

- C2-05 independent Spark evidence admission: add a contract-tool command that
  derives the complete catalog file set from the runnable profile, accepts only
  bounded regular files with canonical newline-terminated encoding, and
  revalidates every transcript against the exact profile/scenario bytes and
  shared fixture. Missing, extra, swapped, oversized, contract-drifted, or
  sanitization-invalid evidence fails closed before result materialization; the
  validated set exposes only deterministic pass, fail, and fixture-collision
  counts to the forthcoming bundle importer.

- C2-04 fresh four-catalog Spark launcher: add one command that rejects reused
  evidence, projects, and state volumes; builds the source-pinned production
  images under a stable Compose identity; verifies every materialized image and
  embedded artifact; and then attempts the common stock-Spark workflow against
  LakeCat, Polaris, Gravitino, and Lakekeeper in one run-owned Docker topology
  with shared MinIO. Exit status is checked against each immutable transcript's
  classification, all four catalogs are attempted after behavioral failures,
  and the prior contention launcher now shares the same fail-closed network and
  fresh-state boundary without deleting historical volumes.

- C2-04 immutable source-bound Spark runtime: materialize the exact optimized
  `catalog-bench-engine` donor and its byte-identical copy inside the combined
  Spark image, bind both to public revision `5e10f36e…`, and advance the runnable
  profile to the dedicated 2026-08-27 source pinset. A reusable artifact-copy
  policy now rejects digest, byte-count, or media-type drift for the runner and
  both Iceberg JARs during generation; the independent Docker verifier confirms
  all five actual images and every embedded artifact before execution.

- C2-04 source-bound engine profile: preserve the published contention source
  profile and add a separate stock-engine candidate that pins
  `catalog-bench-engine` to public revision
  `5e10f36e7e99815df273c7b567e466749f04d4be` with the full optimized Rust
  production recipe. The donor and combined Spark builds now consume that same
  revision, with deployment tests deriving the pin from the profile.

- C2-04 runner artifact correlation: recognize the singular `engine-runner`
  service only when it selects `catalog-bench-engine`, bind its source identity
  into the execution plan and transcript, and require exactly one runner ELF
  whose bytes and in-image location match the copy embedded in Spark. Runtime
  verification now hashes that executable together with the engine and connector
  artifacts whenever the new profile role is present.

- C2-04 single-container Spark harness topology: build the optimized engine
  runner from exact public revision `45e0f82d7bfb17b2d6da9918e89bcc146938addd`,
  copy its ELF and source marker into the pinned Spark/Iceberg image, and add a
  hardened `spark-engine` service. The Rust workflow and its stock
  `spark-submit` child now execute in one container on the existing catalog and
  shared-MinIO Docker network.

- C2-04 optimized engine executable recipe: build and install
  `catalog-bench-engine` beside the existing contention and conformance runners
  in the source-pinned production image, retain the common Rust 1.97.1
  opt-level-3/fat-LTO/single-codegen-unit/stripped/panic-abort recipe, and embed
  the exact catalog-bench source revision as an independently copyable marker.

- C2-04 contract-only engine CLI: add the `catalog-bench-engine` executable with
  only profile, scenario, catalog, fixture, and output inputs. It emits
  newline-terminated sanitized evidence through the shared no-clobber publisher,
  writes valid fail and collision transcripts before returning a nonzero status,
  and never creates evidence for invalid contracts or execution policy.

- C2-04 durable evidence publication seam: replace the duplicated contention
  and conformance output writers with one shared same-directory, synchronized,
  hard-link publication primitive. Concurrent writers can publish only one
  complete file, existing evidence is never overwritten, and fixed failure
  stages distinguish preparation, writing, synchronization, and publication.

- C2-04 sanitized engine transcript: bind each stock Spark execution to the
  exact profile and scenario bytes, runner/catalog/engine/connector/MinIO
  identities, run-owned fixture, and reconciled execution evidence. The
  production path audits only credential values actually observed by its shared
  secret source, rejects raw bearer forms and complete canonical rows, and
  makes transcript sanitization the fourteenth fail-closed assertion with
  offline contract and invariant validation.

- C2-04 bounded catalog negotiation evidence: project the reusable conformance
  session into a closed engine-specific record containing only adapter identity,
  authentication outcome, response status and byte count, routing modes,
  failure stage, and redaction count. Dynamic config JSON, routing values,
  request data, and backend explanations cannot cross the engine evidence
  boundary, even when catalog-controlled object keys contain private values.

- C2-04 production engine adapters: compose the verified stock Spark executor,
  bounded profile-driven Iceberg REST negotiation, independent REST projection,
  and shared MinIO auditor behind the generic engine workflow. All three
  adapters share one opaque credential source, discard backend failure detail
  at the evidence boundary, and preserve the runtime-rejection guarantee that
  no secret, catalog, or object-store access occurs before artifact admission.

- C2-04 engine evidence workflow core: run the stock engine before every
  harness REST or object-store effect, preserve collision as a no-cleanup
  terminal state, and authorize reconciliation only after the engine's trusted
  absence event. The generic orchestration core retains bounded catalog and
  MinIO outcomes, attempts every non-purging cleanup and absence check after
  ownership, and derives all thirteen behavioral assertions from exact runtime,
  schema, snapshot, canonical-row, REST-correlation, object, and cleanup state.

- C2-04 verified engine cleanup: extend the engine-bound REST port with bounded
  table and namespace presence observations so orchestration can prove both
  run-owned resources absent after non-purging cleanup instead of inferring
  cleanliness from DELETE status alone. Presence responses discard bodies and
  retain only standard HTTP 200/404 classifications and byte counts.

- C2-04 bounded engine observations: give the stock Spark renderer the exact
  profile bucket, validate every emitted table UUID, S3 table root, metadata
  pointer, field, count, digest, and runtime label before stdout, and represent
  scenario-property agreement with a closed `match`/`mismatch` ADT instead of
  copying catalog-controlled property values. Unknown properties remain absent
  from both engine and independent REST evidence.

- C2-04 shared transcript value audit: extract the contention evidence's
  recursive serialized-value scanner into the common crate so engine and
  contention transcripts share one rule for detecting sensitive runtime values
  and forbidden identifiers without treating fixed JSON schema keys as data.
  The contention API retains its existing domain-specific failure categories.

- C2-04 concurrent process-test stability: replace the remaining malformed and
  descendant timeout fixtures' CPU-saturating shell loops with sleeping waits.
  The descendant test still proves isolated process-group termination, while a
  three-second timeout gives the separate ownership fixture an explicit process
  scheduling margin when Rust runs tests in parallel.

- C2-04 independent engine REST evidence: bind the negotiated catalog session
  to the profile-derived fixture, load the Spark-created table through the
  standard Iceberg REST route, and project only bounded structural state plus
  scenario-owned properties. The parser validates format, UUID, table root,
  metadata pointer, schema IDs and fields, snapshot uniqueness, and property
  mismatches without retaining arbitrary response values; cleanup uses only
  standard table and namespace deletes with `purgeRequested=false` and fixed,
  secret-free failure categories.

- C2-04 process timeout test stability: exercise descendant termination with a
  sleeping child instead of an unbounded busy loop, preserving the production
  timeout and process-group behavior while avoiding scheduler starvation in
  concurrent debug-profile test runs.

- C2-04 reusable table-object audit: generalize the contention benchmark's
  returned-table-root validator and MinIO auditor without changing its existing
  metadata-only transcript shape. A separate engine-facing audit now counts
  metadata JSON and Parquet objects and bytes recursively, proves the exact
  catalog-referenced metadata pointer exists under the returned root, and
  excludes sibling tables plus unrelated Iceberg objects.

- C2-04 Spark process and secret boundary: launch only the exact profile-pinned
  `spark-submit` file after its platform, byte count, and SHA-256 verification;
  embed the catalog-neutral renderer in the Rust executable; and stage its
  secret-free plan in a private temporary directory. The executor clears the
  inherited environment, restores only a runtime allowlist, maps object-store
  and optional OAuth credentials into child-only standard variables after
  verification, zeroizes its secret buffers, discards stderr, bounds and
  decodes stdout, and immediately terminates the isolated process group on a
  hard timeout or protocol violation. It reports only closed process categories
  while retaining trusted cleanup ownership after timeout or protocol failure.

- C2-04 stock Spark renderer: add one catalog-neutral PySpark implementation of
  the common workflow using the pinned Iceberg `SparkCatalog`, REST properties,
  `S3FileIO`, DataFrameWriterV2 appends, SQL DDL, metadata tables, and public
  Iceberg table utilities. It renders all identifiers and literals safely,
  regenerates rows solely from scenario parameters, proves canonical hashes in
  a no-Spark validation mode, reports only the closed event schema, suppresses
  tracebacks and raw rows, and contains no catalog-name branch or harness HTTP
  substitute.

- C2-04 runtime and engine-event safety boundary: verify Linux/ARM64 plus every
  profile-pinned engine and connector file by streamed byte count and SHA-256
  before credentials or network access, and decode Spark output through a
  bounded, closed event protocol. The protocol trusts cleanup ownership only
  after an ordered, flushed stock-engine absence observation; collision,
  malformed, oversized, duplicated, out-of-order, post-terminal, and incomplete
  streams remain explicit without persisting arbitrary engine logs or exception
  text.

- C2-04 typed engine execution policy: decode the canonical common workflow into
  closed algebraic data types, derive the selected Spark/connector/catalog and
  shared-MinIO bindings from the runnable profile, generate collision-safe
  fixtures and optional standard table locations, and project a catalog-neutral
  secret-free Spark plan. The policy rejects scenario drift, mutable or
  non-Docker profiles, behavior-changing shims, ambiguous engine roles,
  unsupported runtime lines, unsafe fixture identifiers, malformed object-store
  settings, and connector artifacts that are not byte-identical to the copies
  in the executed engine image.

- C2-04 shared adapter runtime seam: separate profile adapter resolution from
  scenario capability classification so stock-engine orchestration can reuse
  the credential-safe Iceberg REST authentication, config negotiation, route
  construction, response bounds, and redaction path without pretending that
  engine capabilities belong to the catalog vocabulary. Existing conformance
  probes retain their strict capability and predeclared-limitation gates.

- C2-03 Spark runtime materialization: build Apache Spark 4.1.3 and Apache
  Iceberg 1.11.0 as separate, inspectable Linux ARM64 Docker artifacts, then
  copy the checksum-locked Spark 4.1/Scala 2.13 runtime and AWS/S3FileIO bundle
  into the executed Spark image. The build preflights the profile-pinned Spark
  index and audited ARM64 child, rejects Maven byte drift, records source and
  Compose labels, runs unprivileged with a read-only root, and stays on the
  common Docker/MinIO network. Added a deterministic runnable profile for
  LakeCat, Polaris, Gravitino, and Lakekeeper; exact image/JAR/entry-point
  evidence; a shared runtime artifact verifier; fail-closed profile tests; and a
  stock `spark-submit --version` smoke proving Spark 4.1.3, Scala 2.13.17,
  OpenJDK 21.0.11, and source revision `77bbf77e...`. This unit makes no
  interoperability-result claim; the profile/scenario-driven workflow runner is
  the next unit.

- C2-02 reusable profile materialization core: extract source-digest checks,
  component/service/catalog-adapter narrowing, local-image projection, platform
  and Compose-label validation, embedded-artifact verification, runnable-state
  derivation, and deterministic serialization behind a scenario policy. The
  C110 contention module is now a thin policy wrapper and still regenerates its
  accepted profile byte-for-byte. Added a second synthetic projection proving
  that catalog adapters narrow with components, standard host and Docker
  architecture names reconcile, and duplicate, unselected, empty, or ambiguous
  policy entries fail closed; embedded-artifact media types are explicit policy
  data. This core is the DRY boundary for Spark, Flink, Trino, and later scenario
  profiles.

- C2-01 common stock-engine contract: define one catalog-neutral, no-shim
  Iceberg REST write/read/additive-evolution scenario for Spark, Flink, Trino,
  and later engines. The workflow pins exact format-v2 Parquet semantics,
  generates one shared 16-row initial and four-row evolved fixture, verifies
  deterministic canonical row hashes, correlates stock-engine observations
  with independent REST and shared-MinIO evidence, preserves objects through
  non-purging cleanup, and requires sanitized transcripts. Added focused tests
  for implementation neutrality, strict assertion coverage, fixture hashes,
  cleanup ordering, and comprehensive methodology with exact upstream source
  references. Conflict synchronization and OpenLineage correlation remain
  separate Phase 2 contracts so unsupported behavior cannot be hidden inside a
  weaker common workflow.

- C1-09 production contention publication: preserve the complete sanitized C110
  transcript as immutable source evidence and pair it with a minimal reviewed
  environment and server-failure sidecar. Added a deterministic importer that
  hash-pins both inputs, reuses the runner's closed transcript ADTs and
  scenario-derived aggregation policy, requires exact aggregate/ranking
  agreement, evaluates all 14 assertions per catalog, and emits five validated
  result records, one manifest, and the generated pass-only matrix. LakeCat ranks
  first among passing catalogs at 147.536 accepted commits/s, followed by Apache
  Polaris at 58.110/s and Apache Gravitino at 56.823/s; Lakekeeper and Apache
  Nessie retain complete diagnostics but remain unranked after PostgreSQL
  deadlock-backed HTTP 503 and Quarkus request-context HTTP 500 errors. Added
  tamper/drift tests, full reproduction commands, current result reporting, and
  dedicated forensic documentation for both non-pass outcomes.

- C1-09 runnable contention profile: deterministically narrow the broad current
  candidate to the ten components used by same-table contention, retain all five
  neutral catalog adapters, and replace the runner, LakeCat, and MinIO
  source-build placeholders with audited Linux ARM64 local-image and embedded
  executable identities. Added a strict materialization sidecar, source and
  observation digest binding, external drift/attribution tests, CLI regeneration
  and staleness checks, and a pre-run Docker verifier that compares actual image
  IDs, platforms, labels, executable hashes, and byte sizes before any measured
  service starts. The production launcher now passes only this runnable profile.

- C1-09 MinIO helper provenance: copy bucket, health, setup, and readiness
  helper sources from the immutable public catalog-bench revision
  `f2f66ee45574a64d1e76330e95e7aa551c3a148b` instead of the mutable local
  context. The image now records this helper revision independently from its
  exact upstream MinIO revision, and deployment tests reject local helper COPYs.

- C1-09 stable production image identity: build the source-built MinIO,
  LakeCat, and benchmark images under the ordinary `catalog-bench` Compose
  project before launching a run-scoped evidence project. Compose project labels
  are part of the exported image config, and BuildKit's default provenance
  wrapper changes identity on every invocation; stable project labels plus
  `--provenance=false` keep those two wrappers from changing an otherwise
  identical local-image digest. Exact source, build recipe, OCI revision, image,
  and embedded executable identities remain recorded independently.

- C1-08 LakeCat contention recovery candidate: advance the exact public
  source-built LakeCat image to
  `962f43cb2d2f345addf188e63be0cf6059bc26b0`. This revision classifies Turso
  busy outcomes without flattening them into internal failures, retries bounded
  transaction boundaries, drops connections whose rollback cannot be
  confirmed, configures the driver busy timeout, and pools bounded read
  connections for commit-adjacent policy, table, storage-profile, and
  idempotency reads.

- C1-08 rerun isolation hardening: after stopping every recognized benchmark
  project without deleting its volumes, the fail-closed launcher now verifies
  that `catalog-bench-net` has zero remaining container attachments before
  starting either production build. This also catches an orphan carrying a
  plausible Compose project label but absent from that project's current model.

- C1-08 fresh production rerun contract: advance the optimized contention
  runner to source `e5345a260a42148aa5cd1044fb3f43acfc2232d2` and LakeCat to
  `bccb5075047f20686519dcb4192359bfe4d39d87`. LakeCat now builds from that
  exact public Git commit and records it as the OCI revision instead of trusting
  a mutable sibling checkout. The runner image likewise consumes its exact
  public source commit instead of labeling a mutable local context. Added a
  run-ID-scoped Compose override and one fail-closed launcher that rejects
  existing evidence, containers, or any of the four persistent state volumes;
  preserves all prior volumes; builds both Rust executables with the production
  recipe; and executes all five catalogs, the runner, and MinIO on the same
  Docker network.

- C1-08 metadata-retention invariant: set Iceberg's standard
  `write.metadata.delete-after-commit.enabled=false` and
  `write.metadata.previous-versions-max=100000` properties identically at table
  creation, codify them in the v2 scenario, and verify the exact wire request.
  The final MinIO growth check can now distinguish missing persistence from a
  catalog's otherwise-valid old-metadata cleanup policy.

- C1-08 optimized same-Docker deployment: pin the contention runner to source
  `efcd6f2123cf9c9107d0e06de64ab97cad67f1e4`, inject that identity only at
  production compile time, and tag the shared Rust image by revision. The
  read-only Linux ARM64 runner now drops all capabilities, mounts contracts
  read-only, writes only create-new evidence, receives the fixed MinIO/Polaris
  fixture credentials, and waits for protocol-level readiness from MinIO and
  all five catalogs on `catalog-bench-net`. Added a LakeCat readiness gate,
  deployment regressions, full operator/methodology documentation, and removed
  commit contention from the host-spawned legacy `BenchReport` driver so it
  cannot discard the strict sweep transcript.

- C1-08 profile-driven sweep and ranking: replace the legacy ad hoc commit
  binary with a closed four-input CLI that accepts only the checked profile,
  canonical scenario, run-owned fixture ID, and create-new transcript path.
  The runner verifies its compile-time source revision and Linux/ARM64 runtime
  before credentials or network access, executes the balanced 30-round schedule
  through the shared catalog and MinIO ports, retains negotiation failures and
  every round outcome, and audits serialized values for credentials and raw
  request identities. Strict aggregation requires one passing conditioning and
  five passing measured rounds per catalog; the full ranking uses median
  concurrent accepted throughput, then sequential p50 latency and catalog ID,
  while failed catalogs remain visible but unranked. Added separate schedule,
  aggregation, tie-break, sanitization, runtime-gating, and CLI-surface tests.

- C1-08 contention round executor: run collision-safe setup, baseline MinIO
  audit, exact warmup and sequential phases, and barrier-synchronized timed
  writers through injected catalog and object-store ports. Every request that
  starts before the deadline now completes and is classified; measured phases
  reuse the setup UUID without hidden table loads; accepted identities cross
  the evidence boundary only as SHA-256 values; final table state and metadata
  growth fail closed; and every post-mutation exit performs verified non-purging
  cleanup. Separate workflow tests cover a passing contended round, no-mutation
  fixture collisions, ambiguous setup cleanup, concurrent request errors,
  failed object audits, metadata undercount, identity redaction, and the absence
  of setup I/O from measured latency.

- C1-08 REST and object-store ports: bind each run-owned fixture to precomputed
  standard Iceberg REST routes outside measured commit latency; require a
  committed format-v2 table snapshot; send only `assert-table-uuid` and one
  unique set-properties update; classify HTTP 200, HTTP 409, and bounded
  explicit failures separately; and hard-code non-purging cleanup without an
  arbitrary-header escape hatch. Added a credential-redacting MinIO auditor
  that recursively consumes every paginated object-list result under the exact
  returned table root, counts only `.metadata.json` objects, totals bytes, and
  verifies the transcript-referenced pointer. Separate integration tests cover
  request shape, route shape, location drift, format drift, oversized bodies,
  nested metadata, sibling exclusion, missing objects, bucket drift, path
  escape, and non-metadata pointers.

- Shared profile-driven catalog runtime: let performance runners reuse the
  conformance suite's tested OAuth2, config negotiation, static/negotiated/
  unprefixed routing, and namespace encoding through a clone-cheap session.
  Standard JSON requests deliberately expose no arbitrary-header hook, retain
  bearer tokens and response bodies only in non-serializable state, redact and
  bound failure details, and either privately collect or allocation-efficiently
  drain every response under the common one-MiB limit. Added anonymous, OAuth,
  routing, credential-secrecy, no-idempotency-header, private-body, bad-config,
  and oversized-response integration coverage without changing existing probe
  behavior.

- C1-08 contention benchmark core: add a typed, canonical scenario/profile
  boundary; balanced rotate-left conditioning and measured-round planning;
  collision-safe per-catalog/per-round fixtures; deterministic finite latency,
  throughput, quantile, median, and range statistics; and complete
  accepted/conflict/error accounting. Raw request identities now live only in
  redacted, non-serializable in-memory types, while final-state evidence retains
  validated SHA-256 values. Duplicate identities, malformed hashes, unaccounted
  requests, regressed metadata counts, zero elapsed time, non-finite samples,
  behavior-changing shims, and shared-object-store drift all fail closed in
  focused integration tests kept outside the implementation modules.

- Catalog community C1-08 contention contract: preserve the historical v1
  scenario bytes while adding a strict v2 authority for profile-driven routing,
  collision-safe fixtures, synchronized writers, complete request and latency
  accounting, final-state attribution, table-root-scoped MinIO growth,
  non-purging cleanup, sanitized no-overwrite evidence, rotated conditioning
  and measured rounds, and median-with-range aggregation. The common workload
  explicitly omits asymmetric idempotency headers. Added focused contract tests
  and corrected the documented current capability count.

- Catalog community C1-07 acceptance: document the exact commit-built stock
  PyIceberg runtime and production LakeCat artifact, five-catalog required and
  optional matrix, four exact row-state digests, all 135 retained metadata,
  manifest, and Parquet objects in shared MinIO, delegated-credential category
  boundaries, complete cleanup and sanitization audit, catalog deployment
  corrections, rejected diagnostics, reproduction workflow, and the C1-09
  publication boundary.

- C1-07 catalog data-plane reconciliation: make Nessie's client-visible S3
  endpoint resolve to shared MinIO inside the benchmark network, enable
  Gravitino's documented `s3-secret-key` credential provider, and extend the
  typed Polaris bootstrap to idempotently grant and verify
  `CATALOG_MANAGE_CONTENT` on `catalog_admin` while enabling MinIO-backed STS
  credential vending with an explicit fixture role. Omit PyIceberg 0.11.1's
  optional `s3.force-virtual-addressing` flag because its stock S3FS adapter
  treats the non-empty string `false` as enabled. Added deterministic grant
  creation/no-op/failure tests and static regressions for the same-Docker
  Nessie and effective Gravitino configuration boundaries.

- C1-07 stock-runtime completeness: select PyIceberg's public no-op auth manager
  for anonymous adapters instead of its legacy `Bearer None` fallback, and add
  profile-pinned S3FS 2026.7.0 plus all exact transitive wheels so
  catalog-selected `FsspecFileIO` remains a stock supported path. Runtime
  identity, transcript provenance, contracts, tests, and profile documentation
  now cover both object-store data planes.

- C1-07 live-smoke corrections: construct Arrow batches with the scenario's
  required `id` nullability instead of relying on nullable inference, and make
  embedded-secret rejection inspect evidence values while comparing map keys
  exactly so short fixture credentials cannot collide with safe schema field
  names. Added regressions for both representation boundaries.

- Catalog community C1-07 reproducible client image: build CPython 3.13.15 from
  the profile's Linux ARM64 child manifest, install all 41
  PyIceberg/PyArrow/S3FS
  distributions from exact wheel hashes, and run the stock-client oracle as an
  unprivileged read-only Compose service on the catalogs' shared Docker network
  and MinIO. Added exact five-catalog startup, readiness, smoke-matrix,
  classification, cleanup, security, and lock-maintenance documentation, plus a
  deployment regression test that binds image, lock, profile, and Compose
  invariants together.

- Catalog community C1-07 stock-client runner: execute the pinned PyIceberg
  namespace/table round trip, real Arrow append and exact scan, independent
  property/schema/delete/conflict/delegation/rename/register classifications,
  explicit client-level view and pagination limitations, conservative fixture
  reconciliation, and immutable value-sanitized transcripts across all five
  protocol-native adapters. Strict contract loading rejects workload drift and
  behavior-changing shims; deterministic fakes cover successful and refusing
  paths without replacing the production stock client.

- Catalog community C1-07 contract: pin the stock PyIceberg runtime and Arrow
  data plane, split optional client operations into explicit capabilities, and
  define a no-shim five-catalog workflow whose evidence distinguishes pass,
  fail, client/catalog unsupported, and dependency-not-evaluated outcomes.

- LakeCat canonical provenance repair: repin every current-profile and
  conformance milestone to its reachable commit after a privacy-only history
  rewrite. Verified `Cargo.toml`, `Cargo.lock`, and the complete `crates/` tree
  are source-identical across each rewritten milestone, rebuilt the exact
  C1-06 LakeCat pin with the production recipe, and reran the five-catalog
  commit matrix plus all 16 direct MinIO object checks.

- Catalog community C1-06 acceptance: document the exact stable-Rust,
  production-optimized commit-correctness runner, five-catalog required and
  config-gated optional matrix, direct audit of all 16 transcript-referenced
  metadata objects in shared MinIO, complete cleanup and sanitization evidence,
  Lakekeeper's and Nessie's error-envelope mismatches, Lakekeeper's exact-replay
  success and content-binding defect, rejected runner diagnostics, reproduction
  workflow, and the C1-09 publication boundary.

- C1-06 optional-branch independence: permit advertised idempotency checks after
  the required final-state reload proves the complete baseline unchanged, even
  when the stale response's status/type envelope fails its separate required
  assertion. Unsafe or mutated final state still suppresses every optional
  request.

- C1-06 successful-transition projection: compare the scenario-owned
  `catalog-bench.*` and `c1-06.*` properties exactly while treating unrelated
  catalog-managed metadata properties as opaque across admitted commits. Exact
  replay and rejected stale/content-drift checks still compare the complete
  property map, so this permits legitimate values such as Nessie's changing
  commit ID without weakening atomicity.

- C1-06 operator guidance: document the optimized Docker invocation for commit
  correctness, its deterministic required branch, config-gated optional UUIDv7
  replay checks, collision and cleanup guarantees, and the distinction between
  mutable smoke transcripts and publishable result bundles.

- Catalog community C1-06 runner: implement a strict Iceberg REST commit
  correctness probe with matching requirement admission, a deterministic stale
  schema conflict and atomicity proof, config-gated UUIDv7 exact replay and
  content-binding checks, full fixture reconciliation, and typed idempotency
  handling that can send raw keys without serializing them into evidence.

- C1-06 protocol preparation: extract committed-table request construction,
  profile-root location derivation, namespace response validation, and generic
  Iceberg metadata/schema snapshots into one reusable conformance module. The
  C1-05 runner retains its exact scenario and evidence shape while commit
  correctness can reuse the same protocol parser instead of cloning it.

- Catalog community C1-06 contract: define a neutral Iceberg REST commit
  correctness scenario that proves valid requirement admission, a deterministic
  stale-schema 409 with no mutation, UUIDv7 exact-retry behavior when advertised,
  same-key content-drift rejection, complete fixture reconciliation, and
  sanitized evidence without turning optional idempotency support into a hidden
  required capability.

- Catalog community C1-05 acceptance: documented the exact stable-Rust,
  production-optimized table-conformance runner and LakeCat artifact,
  five-catalog required/optional matrix, direct audit of all 15 referenced
  metadata objects in shared MinIO, complete cleanup and sanitization evidence,
  LakeCat's repaired no-snapshot rename defect, Gravitino's repaired deployment
  defaults, Nessie's narrow missing-namespace mismatch, rejected exploratory
  evidence, reproduction workflow, and the C1-09 publication boundary.

- C1-05 Gravitino state initialization: added a least-privilege one-shot that
  prepares only Gravitino's named state volume for the image's UID 1000 before
  the catalog starts. Fresh SQLite-backed deployments no longer fail with a
  root-owned volume, and the catalog process itself remains unprivileged.

- C1-05 Gravitino storage correction: aligned the pinned 1.3.0 container's
  Compose environment with its `GRAVITINO_ICEBERG_REST_*` rewrite contract, so
  the declared SQLite JDBC backend, S3 warehouse, S3FileIO, MinIO endpoint, and
  path-style credentials replace the image's `/tmp`/memory defaults. Added a
  deployment regression test and operator diagnostics for proving the effective
  rewritten configuration before accepting shared-storage evidence.

- C1-05 final runner provenance: advanced the draft conformance-runner component
  to `catalog-bench@621cc4b`, whose table probe sends and verifies the profile's
  explicit shared-storage root. The production executable remains unresolved in
  the draft profile until C1-09 materializes immutable artifacts.

- C1-05 shared-storage correction: the table runner now consumes an adapter's
  validated `create_table_location` as a fixture root, derives unique
  namespace/table child locations, sends them on every create attempt, and
  verifies the catalog preserves each requested table location. Adapters without
  an explicit root continue to exercise their configured catalog default.

- C1-05 LakeCat provenance pin: advanced the draft profile and current-profile
  report to the exact pushed table-lifecycle implementation
  `lakecat@ef94b550` (`v0.3.0-32-gef94b550`). The pin includes register and
  rename support plus compatible no-current-snapshot commit evidence; C1-09
  still owns immutable artifact resolution and public result publication.

- C1-05 provenance pin: advanced only the draft conformance-runner component to
  the independently reviewed table-runner revision `catalog-bench@efbce26`.
  Its stable Rust 1.97.1, fat-LTO, single-codegen-unit production recipe remains
  unresolved until the optimized Docker artifact is built and hashed; C1-09
  still owns conversion of reviewed smoke evidence into immutable results.

- Catalog community C1-05 runner: implemented a strict Iceberg REST table
  lifecycle probe with run-owned namespace preflight, committed two-table
  create/list/load, exact isolated pagination, immutable property update, three
  spec-shaped errors, same-namespace rename, non-purging drop, metadata
  registration, complete candidate reconciliation, and sanitized no-overwrite
  evidence. Shared routing negotiation keeps config/auth/prefix/separator policy
  identical across probes; 15 adversarial table tests cover optional limitations
  and failures, collisions, metadata drift, pagination defects, response bounds,
  OAuth secrecy, and cleanup after failed assertions.

- Catalog community C1-05 contract: added a neutral, versioned Iceberg REST
  table-behavior scenario with isolated namespace ownership, two-table
  create/list/load/update/drop coverage, bounded pagination, optional standard
  rename and register operations, exact duplicate/missing-resource error shapes,
  full candidate reconciliation, and sanitized no-shim evidence policy.

- Catalog community C1-05 preparation: generalized the namespace probe's HTTP
  operation recorder, typed observation facts, response-shape checks, and
  Iceberg error validation into one reusable evidence engine. Existing public
  namespace type names remain aliases with byte-identical serialization, while
  subsequent probes inherit the same bounds, sanitization, and failure model.

- Catalog community C1-05 preparation: extracted the Iceberg REST namespace
  identifier, separator negotiation, fixture validation, and prefix-aware route
  construction into shared conformance primitives. The namespace probe retains
  its exact scenario and transcript contract while the table lifecycle probe can
  reuse one routing implementation instead of cloning protocol-sensitive code.

- Catalog community C1-04 acceptance: documented the exact optimized
  same-Docker runner and LakeCat artifacts, profile/scenario/transcript hashes,
  five-catalog required/optional matrix, repaired LakeCat defects, Nessie's
  missing-parent HTTP 200, Polaris's optional property-update HTTP 409,
  cleanup/sanitization guarantees, reproduction workflow, and the explicit
  C1-09 publication boundary.

- C1-04 provenance pin: advanced the draft current profile to the independently
  verified namespace-runner revision `catalog-bench@1f4e640` and corrected
  LakeCat namespace implementation `lakecat@c821a0dc`. Both source builds retain
  the exact stable Rust 1.97.1 production recipe; publication was intentionally
  deferred so C1-09 could own resolved executable/image artifacts and conversion
  of smoke transcripts into a publishable immutable bundle.

- Catalog community C1-04 runner: added a strict Iceberg REST namespace
  lifecycle probe covering isolated create/list/load, multipart hierarchy,
  property update, duplicate and missing-parent errors, bounded pagination, and
  child-first cleanup. Refactored shared target, authentication, transport,
  evidence, and specification primitives out of the config runner; added
  recursively sanitized no-overwrite transcripts, explicit optional-operation
  classification, adversarial mock-server coverage, and a production CLI that
  keeps protocol failures as evidence instead of losing them as process errors.

- C1-03 provenance pin: bound the production commit driver and conformance
  runner to `catalog-bench@feb803f8`, LakeCat to its independently verified
  endpoint-correction revision `10d98cbe`, and modeled the conformance runner as
  its own unresolved source-build component and service. Corrected the candidate
  profile's previously future-dated resolution timestamp; it remains `draft`
  until C1-09 materializes and hashes every listed artifact.

- Catalog community C1-03: added a strict, catalog-neutral Iceberg REST config
  negotiation runner with anonymous/OAuth2 authentication, exact profile and
  scenario projection, bounded and recursively sanitized HTTP evidence, config
  map/media/prefix/endpoint assertions, predeclared unsupported classification,
  and overwrite-safe production CLI output. Added exact Apache Iceberg 1.11.0
  OpenAPI provenance, portable OAuth environment bindings, production-optimized
  same-Docker Rust builds, typed Polaris reconciliation, generic catalog
  readiness gates, comprehensive Rust/Go tests, and operator documentation.
  Live smoke transcripts remain non-publishable until C1-09 wraps reviewed
  evidence in immutable result bundles.

- Catalog community C1-02: added a typed, schema-backed catalog adapter contract
  with exact Iceberg REST config/prefix/auth routing, exhaustive 27-capability
  coverage, protocol-native versus behavior-changing-shim disclosure, secret and
  endpoint drift rejection, and complete current-profile bindings for LakeCat,
  Polaris, Gravitino, Lakekeeper, and Nessie; preserved historical profile bytes,
  regenerated affected schemas, and documented the no-shim semantics and gates.

- Phase 1 infrastructure: made the benchmark Compose project own its Docker
  network, exact source-built MinIO release, idempotently initialized warehouse
  bucket, and state volumes. Added digest-pinned Lakekeeper 0.13.3 and PostgreSQL
  17.11 services with migration, process-health, management-bootstrap, warehouse,
  and isolated-state readiness gates; typed/tested MinIO and Lakekeeper setup
  helpers that reconcile current state and fail on configuration drift; and
  current operations documentation. The final public benchmark artifact
  pipeline was explicitly deferred to C1-09.
- Contract test portability: embedded checked-in profiles, scenarios, and schemas
  in the integration-test binary so a shared Cargo target directory cannot reuse
  stale absolute paths from a removed clean worktree.
- Documentation quality: escaped the write-data example URI so workspace
  Rustdoc builds are warning-free under `-D warnings`.
- Historical evidence: added a deterministic importer that hash-checks and
  recomputes the 2026-08-08 raw TSV evidence into four typed aggregate result
  records and an immutable bundle manifest. Added bundle-wide digest, identity,
  scenario, assertion, and evidence validation plus a generated concurrent
  matrix that ranks only `pass` outcomes and preserves Nessie's diagnostic
  measurements as an unranked `fail`.
- Result provenance: modeled single executions and multi-round aggregates as
  distinct run variants with explicit included/excluded repetitions and rules.
- Phase 0 pinsets: added a runnable reconstruction of the 2026-08-08 Linux ARM64
  commit environment, an explicitly draft 2026-08-26 catalog/client/engine
  profile, and a neutral versioned same-table contention scenario.
- Component taxonomy: added an explicit connector kind for engine/catalog runtime
  artifacts such as Apache Iceberg Java bundles.
- Evidence fidelity: environment values now encode exact, approximate, or unknown
  precision, allowing historical imports to retain incomplete hardware/runtime
  capture without fabricated values.
- Build provenance: generalized component build options and compiler flags so
  Rust, Go, C++, Java, and other toolchains share one neutral recipe shape.
- Profiles: distinguished runnable profiles from draft pinsets. Runnable profiles
  now reject unresolved source-build or package artifacts; drafts must enumerate
  every unresolved component and cannot silently look executable.
- Provenance: normalized source and build identity at the component boundary so
  source-built container images retain both their revision/build recipe and their
  scoped image plus embedded-executable digests.
- Contracts: added the catalog-neutral `catalog-bench/v1` scenario, profile,
  result, and bundle-manifest ADTs; checked-in Draft 2020-12 JSON Schemas; strict
  semantic validation and evidence-sanitization gates; and a stable-Rust CLI
  that regenerates, drift-checks, and validates contract documents. Tests now
  live outside production modules.
- Reproducibility: replaced three ambient `../../../sail` path dependencies with
  one immutable `querygraph/sail@bddb1706` workspace dependency. A standalone
  checkout now resolves the same Foyer object-store implementation regardless of
  neighboring directories, and `--locked` checks no longer depend on local Sail
  state.
