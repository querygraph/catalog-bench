use std::collections::BTreeMap;
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use catalog_bench_commit::store::{ObjectStoreAuditor, ObjectStoreFailure, TableRoot};
use catalog_bench_common::contract::Profile;
use catalog_bench_conformance::{
    connect_catalog_adapter, CatalogConnectionOutcome, CatalogNegotiationFailureStage,
    CATALOG_RESPONSE_LIMIT_BYTES,
};
use tokio::io::AsyncWriteExt as _;
use tokio::process::Command;
use zeroize::Zeroize as _;

use crate::{
    decode_iceberg_table_metadata, decode_trino_canonical_read, decode_trino_single_text,
    decode_trino_single_u64, run_trino_child, EngineCatalog, EngineCatalogConnection,
    EngineCatalogConnectionFailure, EngineCatalogConnectionFailureKind, EngineCatalogConnector,
    EngineCatalogNegotiationEvidence, EngineCredentialFailure, EngineCredentialFailureKind,
    EngineCredentialKind, EngineObjectStoreConnector, EngineProcessExecution, EngineProcessOutcome,
    EngineRunner, EngineRuntimeObservation, EngineTableLoad, FlinkProcessExecutor,
    InteroperabilityPlan, RestEngineCatalog, RuntimeVerifier, SecretRead, SecretSource,
    SparkProcessExecutor, StagedTrinoServer, TrinoCatalogSetup, TrinoCliInvocation, TrinoCliOutput,
    TrinoCommandExecutor, TrinoEffectFailure, TrinoEffects, TrinoFixtureTarget,
    TrinoLauncherInvocation, TrinoObservationPolicy, TrinoOperation, TrinoRenderedProgram,
    TrinoServerConfiguration, TrinoServerEnvironment, TRINO_CLI_LOCATION, TRINO_JAVA_VERSION,
    TRINO_LAUNCHER_LOCATION,
};

const CATALOG_REQUEST_TIMEOUT_MS: u64 = 30_000;
const TRINO_STARTUP_TIMEOUT: Duration = Duration::from_secs(120);
const TRINO_PROBE_INTERVAL: Duration = Duration::from_millis(250);
const TRINO_QUERY_TIMEOUT: Duration = Duration::from_secs(60);
const TRINO_METADATA_LIMIT: usize = 4 * 1024 * 1024;

pub struct StockSparkRunner<S> {
    executor: SparkProcessExecutor,
    verifier: RuntimeVerifier,
    secrets: Arc<S>,
}

impl<S> StockSparkRunner<S> {
    #[must_use]
    pub fn production(secrets: Arc<S>) -> Self {
        Self {
            executor: SparkProcessExecutor::default(),
            verifier: RuntimeVerifier::host(),
            secrets,
        }
    }

    #[must_use]
    pub fn from_parts(
        executor: SparkProcessExecutor,
        verifier: RuntimeVerifier,
        secrets: Arc<S>,
    ) -> Self {
        Self {
            executor,
            verifier,
            secrets,
        }
    }
}

impl<S> Clone for StockSparkRunner<S> {
    fn clone(&self) -> Self {
        Self {
            executor: self.executor.clone(),
            verifier: self.verifier.clone(),
            secrets: Arc::clone(&self.secrets),
        }
    }
}

impl<S> EngineRunner for StockSparkRunner<S>
where
    S: SecretSource + Send + Sync + 'static,
{
    async fn execute(&self, plan: &InteroperabilityPlan) -> EngineProcessExecution {
        self.executor
            .execute_with_source(plan, &self.verifier, self.secrets.as_ref())
            .await
    }
}

pub struct StockFlinkRunner<S> {
    executor: FlinkProcessExecutor,
    verifier: RuntimeVerifier,
    secrets: Arc<S>,
}

impl<S> StockFlinkRunner<S> {
    #[must_use]
    pub fn production(secrets: Arc<S>) -> Self {
        Self {
            executor: FlinkProcessExecutor::default(),
            verifier: RuntimeVerifier::host(),
            secrets,
        }
    }

    #[must_use]
    pub fn from_parts(
        executor: FlinkProcessExecutor,
        verifier: RuntimeVerifier,
        secrets: Arc<S>,
    ) -> Self {
        Self {
            executor,
            verifier,
            secrets,
        }
    }
}

impl<S> Clone for StockFlinkRunner<S> {
    fn clone(&self) -> Self {
        Self {
            executor: self.executor.clone(),
            verifier: self.verifier.clone(),
            secrets: Arc::clone(&self.secrets),
        }
    }
}

impl<S> EngineRunner for StockFlinkRunner<S>
where
    S: SecretSource + Send + Sync + 'static,
{
    async fn execute(&self, plan: &InteroperabilityPlan) -> EngineProcessExecution {
        self.executor
            .execute_with_source(plan, &self.verifier, self.secrets.as_ref())
            .await
    }
}

pub struct StockTrinoRunner<S> {
    profile: Arc<Profile>,
    verifier: RuntimeVerifier,
    secrets: Arc<S>,
}

impl<S> StockTrinoRunner<S> {
    #[must_use]
    pub fn production(profile: Arc<Profile>, secrets: Arc<S>) -> Self {
        Self {
            profile,
            verifier: RuntimeVerifier::host(),
            secrets,
        }
    }
}

impl<S> Clone for StockTrinoRunner<S> {
    fn clone(&self) -> Self {
        Self {
            profile: Arc::clone(&self.profile),
            verifier: self.verifier.clone(),
            secrets: Arc::clone(&self.secrets),
        }
    }
}

impl<S> EngineRunner for StockTrinoRunner<S>
where
    S: SecretSource + Send + Sync + 'static,
{
    async fn execute(&self, plan: &InteroperabilityPlan) -> EngineProcessExecution {
        let runtime = self.verifier.verify(plan);
        if !runtime.passed() {
            return EngineProcessExecution::before_process(
                runtime,
                EngineProcessOutcome::RuntimeRejected {},
            );
        }
        let Some(trino) = plan.trino() else {
            return EngineProcessExecution::before_process(
                runtime,
                EngineProcessOutcome::PreparationFailed {
                    kind: crate::EnginePreparationFailureKind::ExecutionPlanMismatch,
                },
            );
        };
        let program = match TrinoRenderedProgram::render(trino) {
            Ok(program) => program,
            Err(_) => {
                return EngineProcessExecution::before_process(
                    runtime,
                    EngineProcessOutcome::PreparationFailed {
                        kind: crate::EnginePreparationFailureKind::RenderPlan,
                    },
                )
            }
        };
        let access_key = match required_runner_secret(
            self.secrets.as_ref(),
            &plan.object_store().access_key_env,
            EngineCredentialKind::ObjectStoreAccessKey,
        ) {
            Ok(value) => value,
            Err(failure) => return credential_rejected(runtime, failure),
        };
        let secret_key = match required_runner_secret(
            self.secrets.as_ref(),
            &plan.object_store().secret_key_env,
            EngineCredentialKind::ObjectStoreSecretKey,
        ) {
            Ok(value) => value,
            Err(failure) => return credential_rejected(runtime, failure),
        };
        let oauth = match plan.credential_source() {
            crate::CatalogCredentialSource::Anonymous => None,
            crate::CatalogCredentialSource::OAuth2ClientCredentials {
                client_id_env,
                client_secret_env,
            } => {
                let mut id = match required_runner_secret(
                    self.secrets.as_ref(),
                    client_id_env,
                    EngineCredentialKind::CatalogClientId,
                ) {
                    Ok(value) => value,
                    Err(failure) => return credential_rejected(runtime, failure),
                };
                let mut secret = match required_runner_secret(
                    self.secrets.as_ref(),
                    client_secret_env,
                    EngineCredentialKind::CatalogClientSecret,
                ) {
                    Ok(value) => value,
                    Err(failure) => return credential_rejected(runtime, failure),
                };
                let credential = format!("{id}:{secret}");
                id.zeroize();
                secret.zeroize();
                Some(credential)
            }
        };
        let configuration = match TrinoServerConfiguration::render(&program) {
            Ok(configuration) => configuration,
            Err(_) => return preparation_failed(runtime),
        };
        let staged = match StagedTrinoServer::create(&configuration) {
            Ok(staged) => staged,
            Err(_) => return preparation_failed(runtime),
        };
        let environment = match TrinoServerEnvironment::new(
            format!("catalog-bench-{}", plan.fixture().namespace),
            staged.data().to_owned(),
            access_key,
            secret_key,
            oauth,
        ) {
            Ok(environment) => environment,
            Err(_) => return preparation_failed(runtime),
        };
        let launcher = match TrinoLauncherInvocation::new(
            Path::new(TRINO_LAUNCHER_LOCATION),
            staged.configuration(),
        ) {
            Ok(launcher) => launcher,
            Err(_) => return preparation_failed(runtime),
        };
        let server = match crate::RunningTrinoServer::start(
            &launcher,
            Path::new(TRINO_CLI_LOCATION),
            staged.root(),
            &environment,
            TRINO_STARTUP_TIMEOUT,
            TRINO_PROBE_INTERVAL,
        )
        .await
        {
            Ok(server) => server,
            Err(_) => {
                return EngineProcessExecution::before_process(
                    runtime,
                    EngineProcessOutcome::SpawnFailed {},
                )
            }
        };
        let catalog =
            match connect_observation_catalog(&self.profile, plan, self.secrets.as_ref()).await {
                Some(catalog) => catalog,
                None => {
                    server.shutdown().await;
                    return preparation_failed(runtime);
                }
            };
        let store = match ObjectStoreAuditor::from_connection(plan.object_store(), |name| {
            optional_secret(self.secrets.as_ref(), name)
        }) {
            Ok(store) => store,
            Err(_) => {
                server.shutdown().await;
                return preparation_failed(runtime);
            }
        };
        let started = Instant::now();
        let handle = tokio::runtime::Handle::current();
        let root = staged.root().to_owned();
        let task = tokio::task::spawn_blocking(move || {
            let mut effects = ProductionTrinoEffects {
                handle,
                executor: TrinoCommandExecutor::new(TRINO_QUERY_TIMEOUT)
                    .expect("nonzero fixed timeout"),
                root,
                server: Some(server),
                program: program.clone(),
                catalog,
                store,
            };
            let run = run_trino_child(&program, &mut effects);
            (run, effects.server.take())
        })
        .await;
        let Ok((run, server)) = task else {
            return EngineProcessExecution::before_process(
                runtime,
                EngineProcessOutcome::WaitFailed {},
            );
        };
        if let Some(server) = server {
            server.shutdown().await;
        }
        EngineProcessExecution::from_in_process_events(
            runtime,
            run.exit_code,
            run.events,
            u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
        )
    }
}

struct ProductionTrinoEffects {
    handle: tokio::runtime::Handle,
    executor: TrinoCommandExecutor,
    root: std::path::PathBuf,
    server: Option<crate::RunningTrinoServer>,
    program: TrinoRenderedProgram,
    catalog: RestEngineCatalog,
    store: ObjectStoreAuditor,
}

impl ProductionTrinoEffects {
    fn query(
        &self,
        sql: &str,
        output: TrinoCliOutput,
        limit: usize,
    ) -> Result<Vec<u8>, TrinoEffectFailure> {
        let invocation = TrinoCliInvocation::new(Path::new(TRINO_CLI_LOCATION), sql, output)
            .map_err(|_| TrinoEffectFailure)?;
        self.handle
            .block_on(self.executor.execute_cli(&invocation, &self.root, limit))
            .map_err(|_| TrinoEffectFailure)
    }
}

impl TrinoEffects for ProductionTrinoEffects {
    fn runtime_observation(&mut self) -> Result<EngineRuntimeObservation, TrinoEffectFailure> {
        let output = self.query(
            "SELECT version() AS version",
            TrinoCliOutput::Json,
            64 * 1024,
        )?;
        let version =
            decode_trino_single_text(&output, "version").map_err(|_| TrinoEffectFailure)?;
        Ok(EngineRuntimeObservation {
            engine_version: version,
            dependencies: BTreeMap::from([("java".to_owned(), TRINO_JAVA_VERSION.to_owned())]),
            operating_system: std::env::consts::OS.to_owned(),
            architecture: std::env::consts::ARCH.to_owned(),
        })
    }

    fn initialize_catalog(
        &mut self,
        catalog: &TrinoCatalogSetup,
        configuration: &TrinoServerConfiguration,
    ) -> Result<(), TrinoEffectFailure> {
        if self.server.is_some()
            && catalog == &self.program.catalog
            && configuration
                == &TrinoServerConfiguration::render(&self.program)
                    .map_err(|_| TrinoEffectFailure)?
        {
            Ok(())
        } else {
            Err(TrinoEffectFailure)
        }
    }

    fn fixture_absent(&mut self, fixture: &TrinoFixtureTarget) -> Result<bool, TrinoEffectFailure> {
        let sql = format!(
            "SELECT count(*) AS matches FROM information_schema.tables WHERE table_schema = {} AND table_name = {}",
            crate::sql::literal(&fixture.namespace),
            crate::sql::literal(&fixture.table)
        );
        let output = self.query(&sql, TrinoCliOutput::Json, 64 * 1024)?;
        Ok(decode_trino_single_u64(&output, "matches").map_err(|_| TrinoEffectFailure)? == 0)
    }

    fn execute(&mut self, operation: &TrinoOperation) -> Result<(), TrinoEffectFailure> {
        self.query(operation.sql(), TrinoCliOutput::Discard, 1024)
            .map(|_| ())
    }

    fn namespace_listed_exactly(
        &mut self,
        fixture: &TrinoFixtureTarget,
    ) -> Result<bool, TrinoEffectFailure> {
        let sql = format!(
            "SELECT count(*) AS matches FROM information_schema.schemata WHERE schema_name = {}",
            crate::sql::literal(&fixture.namespace)
        );
        let output = self.query(&sql, TrinoCliOutput::Json, 64 * 1024)?;
        Ok(decode_trino_single_u64(&output, "matches").map_err(|_| TrinoEffectFailure)? == 1)
    }

    fn observe_table(
        &mut self,
        fixture: &TrinoFixtureTarget,
        policy: &TrinoObservationPolicy,
    ) -> Result<crate::EngineTableObservation, TrinoEffectFailure> {
        let load = self
            .handle
            .block_on(self.catalog.load_table())
            .map_err(|_| TrinoEffectFailure)?;
        let EngineTableLoad::Present { state, .. } = load else {
            return Err(TrinoEffectFailure);
        };
        let root = TableRoot::new(
            &state.table.location,
            &state.table.metadata_location,
            &fixture.bucket,
        )
        .map_err(|_| TrinoEffectFailure)?;
        let bytes = self
            .handle
            .block_on(self.store.read_metadata(
                &root,
                &state.table.metadata_location,
                TRINO_METADATA_LIMIT,
            ))
            .map_err(|_| TrinoEffectFailure)?;
        decode_iceberg_table_metadata(&bytes, &state.table.metadata_location, fixture, policy)
            .map_err(|_| TrinoEffectFailure)
    }

    fn read(
        &mut self,
        operation: &TrinoOperation,
    ) -> Result<crate::RowReadObservation, TrinoEffectFailure> {
        let expected = match operation {
            TrinoOperation::InitialRead { expected, .. }
            | TrinoOperation::EvolvedRead { expected, .. } => expected,
            _ => return Err(TrinoEffectFailure),
        };
        let output = self.query(operation.sql(), TrinoCliOutput::Json, 16 * 1024 * 1024)?;
        decode_trino_canonical_read(&output, expected).map_err(|_| TrinoEffectFailure)
    }

    fn snapshot_count(&mut self, operation: &TrinoOperation) -> Result<u64, TrinoEffectFailure> {
        let sql = format!("SELECT count(*) AS snapshots FROM ({})", operation.sql());
        let output = self.query(&sql, TrinoCliOutput::Json, 64 * 1024)?;
        decode_trino_single_u64(&output, "snapshots").map_err(|_| TrinoEffectFailure)
    }
}

pub struct StockDuckDbRunner<S> {
    profile: Arc<Profile>,
    verifier: RuntimeVerifier,
    secrets: Arc<S>,
}

impl<S> StockDuckDbRunner<S> {
    #[must_use]
    pub fn production(profile: Arc<Profile>, secrets: Arc<S>) -> Self {
        Self {
            profile,
            verifier: RuntimeVerifier::host(),
            secrets,
        }
    }
}

impl<S> Clone for StockDuckDbRunner<S> {
    fn clone(&self) -> Self {
        Self {
            profile: Arc::clone(&self.profile),
            verifier: self.verifier.clone(),
            secrets: Arc::clone(&self.secrets),
        }
    }
}

impl<S> EngineRunner for StockDuckDbRunner<S>
where
    S: SecretSource + Send + Sync + 'static,
{
    async fn execute(&self, plan: &InteroperabilityPlan) -> EngineProcessExecution {
        let runtime = self.verifier.verify(plan);
        if !runtime.passed() {
            return EngineProcessExecution::before_process(
                runtime,
                EngineProcessOutcome::RuntimeRejected {},
            );
        }
        let Some(duckdb) = plan.duckdb() else {
            return EngineProcessExecution::before_process(
                runtime,
                EngineProcessOutcome::PreparationFailed {
                    kind: crate::EnginePreparationFailureKind::ExecutionPlanMismatch,
                },
            );
        };
        let program = match crate::DuckDbRenderedProgram::render(duckdb) {
            Ok(program) => program,
            Err(_) => return preparation_failed(runtime),
        };
        let access_key = match required_runner_secret(
            self.secrets.as_ref(),
            &plan.object_store().access_key_env,
            EngineCredentialKind::ObjectStoreAccessKey,
        ) {
            Ok(value) => value,
            Err(failure) => return credential_rejected(runtime, failure),
        };
        let secret_key = match required_runner_secret(
            self.secrets.as_ref(),
            &plan.object_store().secret_key_env,
            EngineCredentialKind::ObjectStoreSecretKey,
        ) {
            Ok(value) => value,
            Err(failure) => return credential_rejected(runtime, failure),
        };
        let oauth = match plan.credential_source() {
            crate::CatalogCredentialSource::Anonymous => None,
            crate::CatalogCredentialSource::OAuth2ClientCredentials {
                client_id_env,
                client_secret_env,
            } => {
                let id = match required_runner_secret(
                    self.secrets.as_ref(),
                    client_id_env,
                    EngineCredentialKind::CatalogClientId,
                ) {
                    Ok(value) => value,
                    Err(failure) => return credential_rejected(runtime, failure),
                };
                let secret = match required_runner_secret(
                    self.secrets.as_ref(),
                    client_secret_env,
                    EngineCredentialKind::CatalogClientSecret,
                ) {
                    Ok(value) => value,
                    Err(failure) => return credential_rejected(runtime, failure),
                };
                Some((id, secret))
            }
        };
        let catalog =
            match connect_observation_catalog(&self.profile, plan, self.secrets.as_ref()).await {
                Some(value) => value,
                None => return preparation_failed(runtime),
            };
        let store = match ObjectStoreAuditor::from_connection(plan.object_store(), |name| {
            optional_secret(self.secrets.as_ref(), name)
        }) {
            Ok(value) => value,
            Err(_) => return preparation_failed(runtime),
        };
        let started = Instant::now();
        let mut effects = DuckDbProductionEffects {
            program,
            access_key,
            secret_key,
            oauth,
            catalog,
            store,
        };
        let (exit_code, events) = run_duckdb_program(&mut effects).await;
        EngineProcessExecution::from_in_process_events(
            runtime,
            exit_code,
            events,
            u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
        )
    }
}

struct DuckDbProductionEffects {
    program: crate::DuckDbRenderedProgram,
    access_key: String,
    secret_key: String,
    oauth: Option<(String, String)>,
    catalog: RestEngineCatalog,
    store: ObjectStoreAuditor,
}

impl Drop for DuckDbProductionEffects {
    fn drop(&mut self) {
        self.access_key.zeroize();
        self.secret_key.zeroize();
        if let Some((id, secret)) = &mut self.oauth {
            id.zeroize();
            secret.zeroize();
        }
    }
}

impl DuckDbProductionEffects {
    fn setup_sql(&self) -> Option<String> {
        let endpoint = url::Url::parse(&self.program.file_io.endpoint).ok()?;
        let host = endpoint.host_str()?;
        let endpoint = match endpoint.port() {
            Some(port) => format!("{host}:{port}"),
            None => host.to_owned(),
        };
        let mut attach = format!(
            "ATTACH {} AS \"{}\" (TYPE iceberg, ENDPOINT {}, ACCESS_DELEGATION_MODE 'none'",
            crate::sql::literal(self.program.warehouse.as_deref().unwrap_or("")),
            self.program.catalog_name,
            crate::sql::literal(&self.program.catalog_uri)
        );
        match (&self.program.authentication, &self.oauth) {
            (crate::EngineCatalogAuthentication::Anonymous, None) => attach.push_str(", AUTHORIZATION_TYPE 'none'"),
            (crate::EngineCatalogAuthentication::OAuth2ClientCredentials { oauth2_server_uri, scope }, Some((id, secret))) => attach.push_str(&format!(", AUTHORIZATION_TYPE 'oauth2', CLIENT_ID {}, CLIENT_SECRET {}, OAUTH2_SERVER_URI {}, SCOPE {}", crate::sql::literal(id), crate::sql::literal(secret), crate::sql::literal(oauth2_server_uri), crate::sql::literal(scope))),
            _ => return None,
        }
        attach.push_str(");");
        Some(format!("LOAD httpfs; LOAD iceberg; CREATE SECRET catalog_bench_s3 (TYPE s3, PROVIDER config, KEY_ID {}, SECRET {}, REGION {}, ENDPOINT {}, URL_STYLE 'path', USE_SSL {}); {attach}", crate::sql::literal(&self.access_key), crate::sql::literal(&self.secret_key), crate::sql::literal(&self.program.file_io.region), crate::sql::literal(&endpoint), endpoint_is_https(&self.program.file_io.endpoint)))
    }

    async fn query(&self, sql: &str) -> Result<Vec<serde_json::Value>, ()> {
        let mut input = self.setup_sql().ok_or(())?;
        input.push(' ');
        input.push_str(sql);
        input.push(';');
        let mut child = Command::new(crate::DUCKDB_CLI_LOCATION)
            .args(["-json"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|_| ())?;
        child
            .stdin
            .take()
            .ok_or(())?
            .write_all(input.as_bytes())
            .await
            .map_err(|_| ())?;
        input.zeroize();
        let output = tokio::time::timeout(Duration::from_secs(60), child.wait_with_output())
            .await
            .map_err(|_| ())?
            .map_err(|_| ())?;
        if !output.status.success() {
            return Err(());
        }
        if output.stdout.len() > 16 * 1024 * 1024 {
            return Err(());
        }
        if output.stdout.is_empty() {
            return Ok(Vec::new());
        }
        serde_json::Deserializer::from_slice(&output.stdout)
            .into_iter::<Vec<serde_json::Value>>()
            .last()
            .ok_or(())?
            .map_err(|_| ())
    }

    async fn table_observation(&self) -> Result<crate::EngineTableObservation, ()> {
        let load = self.catalog.load_table().await.map_err(|_| ())?;
        let EngineTableLoad::Present { state, .. } = load else {
            return Err(());
        };
        let root = TableRoot::new(
            &state.table.location,
            &state.table.metadata_location,
            &self.program.fixture.bucket,
        )
        .map_err(|_| ())?;
        let bytes = self
            .store
            .read_metadata(&root, &state.table.metadata_location, TRINO_METADATA_LIMIT)
            .await
            .map_err(|_| ())?;
        decode_iceberg_table_metadata(
            &bytes,
            &state.table.metadata_location,
            &self.program.fixture,
            &self.program.observation,
        )
        .map_err(|_| ())
    }
}

fn endpoint_is_https(value: &str) -> bool {
    url::Url::parse(value).is_ok_and(|url| url.scheme() == "https")
}

async fn run_duckdb_program(
    effects: &mut DuckDbProductionEffects,
) -> (i32, Vec<crate::EngineEvent>) {
    use crate::{EngineEvent, EngineFailureCategory as Category, EngineStage as Stage};
    let mut events = Vec::with_capacity(13);
    let runtime_rows = match effects.query("SELECT version() AS version").await {
        Ok(rows) => rows,
        Err(_) => return duckdb_failed(events, Stage::VerifyRuntime, Category::Runtime),
    };
    if runtime_rows
        .first()
        .and_then(|row| row.get("version"))
        .and_then(serde_json::Value::as_str)
        != Some("v1.5.3")
    {
        return duckdb_failed(events, Stage::VerifyRuntime, Category::Runtime);
    }
    events.push(EngineEvent::RuntimeReady {
        runtime: EngineRuntimeObservation {
            engine_version: "1.5.3".to_owned(),
            dependencies: BTreeMap::from([
                ("avro".to_owned(), "1.5.3".to_owned()),
                ("httpfs".to_owned(), "1.5.3".to_owned()),
                ("iceberg".to_owned(), "1.5.3".to_owned()),
            ]),
            operating_system: std::env::consts::OS.to_owned(),
            architecture: std::env::consts::ARCH.to_owned(),
        },
    });
    events.push(EngineEvent::CatalogReady);
    let fixture = &effects.program.fixture;
    let absent_sql = format!("SELECT count(*) AS matches FROM information_schema.tables WHERE table_catalog = {} AND table_schema = {} AND table_name = {}", crate::sql::literal(&effects.program.catalog_name), crate::sql::literal(&fixture.namespace), crate::sql::literal(&fixture.table));
    let absent = effects
        .query(&absent_sql)
        .await
        .ok()
        .and_then(|rows| duckdb_u64(&rows, "matches"))
        .is_some_and(|count| count == 0);
    events.push(EngineEvent::FixturePreflight { absent });
    if !absent {
        return (3, events);
    }
    let operations = effects.program.operations.clone();
    if duckdb_execute(effects, &operations[0]).await.is_err() {
        return duckdb_failed(events, Stage::CreateNamespace, Category::Catalog);
    }
    let namespace_sql = format!("SELECT count(*) AS matches FROM information_schema.schemata WHERE catalog_name = {} AND schema_name = {}", crate::sql::literal(&effects.program.catalog_name), crate::sql::literal(&fixture.namespace));
    if effects
        .query(&namespace_sql)
        .await
        .ok()
        .and_then(|rows| duckdb_u64(&rows, "matches"))
        != Some(1)
    {
        return duckdb_failed(events, Stage::CreateNamespace, Category::Catalog);
    }
    events.push(EngineEvent::NamespaceReady {
        listed_exactly: true,
    });
    if duckdb_execute(effects, &operations[1]).await.is_err() {
        return duckdb_failed(events, Stage::CreateTable, Category::Connector);
    }
    let table = match effects.table_observation().await {
        Ok(value) => value,
        Err(_) => return duckdb_failed(events, Stage::CreateTable, Category::Connector),
    };
    events.push(EngineEvent::TableReady { table });
    if duckdb_execute(effects, &operations[2]).await.is_err() {
        return duckdb_failed(events, Stage::AppendInitial, Category::Data);
    }
    let snapshots = match duckdb_snapshot_count(effects).await {
        Ok(value) => value,
        Err(_) => return duckdb_failed(events, Stage::AppendInitial, Category::Data),
    };
    events.push(EngineEvent::InitialAppended { snapshots });
    let read = match duckdb_read(effects, &operations[3]).await {
        Ok(value) => value,
        Err(_) => return duckdb_failed(events, Stage::ReadInitial, Category::Data),
    };
    events.push(EngineEvent::InitialRead { read });
    if duckdb_execute(effects, &operations[4]).await.is_err() {
        return duckdb_failed(events, Stage::EvolveSchema, Category::Connector);
    }
    let table = match effects.table_observation().await {
        Ok(value) => value,
        Err(_) => return duckdb_failed(events, Stage::EvolveSchema, Category::Connector),
    };
    events.push(EngineEvent::SchemaEvolved { table });
    if duckdb_execute(effects, &operations[5]).await.is_err() {
        return duckdb_failed(events, Stage::AppendEvolved, Category::Data);
    }
    let snapshots = match duckdb_snapshot_count(effects).await {
        Ok(value) => value,
        Err(_) => return duckdb_failed(events, Stage::AppendEvolved, Category::Data),
    };
    events.push(EngineEvent::EvolvedAppended { snapshots });
    let read = match duckdb_read(effects, &operations[6]).await {
        Ok(value) => value,
        Err(_) => return duckdb_failed(events, Stage::ReadEvolved, Category::Data),
    };
    events.push(EngineEvent::EvolvedRead { read });
    let table = match effects.table_observation().await {
        Ok(value) => value,
        Err(_) => return duckdb_failed(events, Stage::ObserveFinalTable, Category::Connector),
    };
    events.push(EngineEvent::FinalTable { table });
    events.push(EngineEvent::Completed);
    (0, events)
}

async fn duckdb_execute(
    effects: &DuckDbProductionEffects,
    operation: &crate::DuckDbOperation,
) -> Result<(), ()> {
    effects.query(operation.sql().ok_or(())?).await.map(|_| ())
}
async fn duckdb_read(
    effects: &DuckDbProductionEffects,
    operation: &crate::DuckDbOperation,
) -> Result<crate::RowReadObservation, ()> {
    let expected = match operation {
        crate::DuckDbOperation::InitialRead { expected, .. }
        | crate::DuckDbOperation::EvolvedRead { expected, .. } => expected,
        _ => return Err(()),
    };
    let rows = effects.query(operation.sql().ok_or(())?).await?;
    if u64::try_from(rows.len()).ok() != Some(expected.rows) {
        return Err(());
    }
    let mut canonical = Vec::new();
    for row in &rows {
        let object = row.as_object().ok_or(())?;
        let values = expected
            .columns
            .iter()
            .map(|column| object.get(column).ok_or(()))
            .collect::<Result<Vec<_>, _>>()?;
        serde_json::to_writer(&mut canonical, &values).map_err(|_| ())?;
        canonical.push(b'\n');
    }
    let read = crate::RowReadObservation {
        rows: expected.rows,
        bytes: u64::try_from(canonical.len()).unwrap_or(u64::MAX),
        sha256: catalog_bench_conformance::sha256_hex(&canonical),
    };
    let expected_read = crate::RowReadObservation {
        rows: expected.rows,
        bytes: expected.bytes,
        sha256: expected.sha256.clone(),
    };
    if read == expected_read {
        Ok(read)
    } else {
        Err(())
    }
}
async fn duckdb_snapshot_count(effects: &DuckDbProductionEffects) -> Result<u64, ()> {
    let load = effects.catalog.load_table().await.map_err(|_| ())?;
    let EngineTableLoad::Present { state, .. } = load else {
        return Err(());
    };
    let sql = format!(
        "SELECT count(*) AS snapshots FROM iceberg_snapshots({})",
        crate::sql::literal(&state.table.metadata_location)
    );
    let rows = effects.query(&sql).await?;
    duckdb_u64(&rows, "snapshots").ok_or(())
}
fn duckdb_u64(rows: &[serde_json::Value], field: &str) -> Option<u64> {
    rows.first()?.get(field)?.as_u64()
}
fn duckdb_failed(
    mut events: Vec<crate::EngineEvent>,
    stage: crate::EngineStage,
    category: crate::EngineFailureCategory,
) -> (i32, Vec<crate::EngineEvent>) {
    events.push(crate::EngineEvent::Failed { stage, category });
    (2, events)
}

async fn connect_observation_catalog(
    profile: &Profile,
    plan: &InteroperabilityPlan,
    secrets: &(impl SecretSource + ?Sized),
) -> Option<RestEngineCatalog> {
    let attempt = connect_catalog_adapter(
        profile,
        &plan.catalog().id,
        CATALOG_REQUEST_TIMEOUT_MS,
        CATALOG_RESPONSE_LIMIT_BYTES,
        |name| optional_secret(secrets, name),
    )
    .await
    .ok()?;
    let CatalogConnectionOutcome::Ready(session) = attempt.outcome else {
        return None;
    };
    RestEngineCatalog::from_plan(session, plan).ok()
}

fn required_runner_secret(
    source: &(impl SecretSource + ?Sized),
    name: &str,
    credential: EngineCredentialKind,
) -> Result<String, EngineCredentialFailure> {
    match source.read_secret(name) {
        SecretRead::Missing => Err(EngineCredentialFailure {
            credential,
            kind: EngineCredentialFailureKind::Missing,
        }),
        SecretRead::Unreadable => Err(EngineCredentialFailure {
            credential,
            kind: EngineCredentialFailureKind::Unreadable,
        }),
        SecretRead::Value(value) if value.is_empty() => Err(EngineCredentialFailure {
            credential,
            kind: EngineCredentialFailureKind::Empty,
        }),
        SecretRead::Value(value) => Ok(value),
    }
}

fn credential_rejected(
    runtime: crate::RuntimeVerification,
    failure: EngineCredentialFailure,
) -> EngineProcessExecution {
    EngineProcessExecution::before_process(
        runtime,
        EngineProcessOutcome::CredentialRejected { failure },
    )
}

fn preparation_failed(runtime: crate::RuntimeVerification) -> EngineProcessExecution {
    EngineProcessExecution::before_process(
        runtime,
        EngineProcessOutcome::PreparationFailed {
            kind: crate::EnginePreparationFailureKind::RenderPlan,
        },
    )
}

pub struct RestEngineCatalogConnector<S> {
    profile: Arc<Profile>,
    secrets: Arc<S>,
}

impl<S> RestEngineCatalogConnector<S> {
    #[must_use]
    pub fn new(profile: Arc<Profile>, secrets: Arc<S>) -> Self {
        Self { profile, secrets }
    }
}

impl<S> Clone for RestEngineCatalogConnector<S> {
    fn clone(&self) -> Self {
        Self {
            profile: Arc::clone(&self.profile),
            secrets: Arc::clone(&self.secrets),
        }
    }
}

impl<S> EngineCatalogConnector for RestEngineCatalogConnector<S>
where
    S: SecretSource + Send + Sync + 'static,
{
    type Catalog = RestEngineCatalog;

    async fn connect(&self, plan: &InteroperabilityPlan) -> EngineCatalogConnection<Self::Catalog> {
        let attempt = connect_catalog_adapter(
            &self.profile,
            &plan.catalog().id,
            CATALOG_REQUEST_TIMEOUT_MS,
            CATALOG_RESPONSE_LIMIT_BYTES,
            |name| optional_secret(self.secrets.as_ref(), name),
        )
        .await;
        let attempt = match attempt {
            Ok(attempt) => attempt,
            Err(_) => return failed_connection(None, EngineCatalogConnectionFailureKind::Setup),
        };
        let negotiation = match EngineCatalogNegotiationEvidence::try_from(attempt.evidence) {
            Ok(negotiation) => negotiation,
            Err(_) => return failed_connection(None, EngineCatalogConnectionFailureKind::Setup),
        };
        match attempt.outcome {
            CatalogConnectionOutcome::Ready(session) => {
                match RestEngineCatalog::from_plan(session, plan) {
                    Ok(catalog) => EngineCatalogConnection::Ready {
                        negotiation,
                        catalog,
                    },
                    Err(_) => failed_connection(
                        Some(negotiation),
                        EngineCatalogConnectionFailureKind::FixtureRoute,
                    ),
                }
            }
            CatalogConnectionOutcome::Failed(failure) => failed_connection(
                Some(negotiation),
                match failure.stage {
                    CatalogNegotiationFailureStage::Authentication => {
                        EngineCatalogConnectionFailureKind::Authentication
                    }
                    CatalogNegotiationFailureStage::Config => {
                        EngineCatalogConnectionFailureKind::Config
                    }
                    CatalogNegotiationFailureStage::Routing => {
                        EngineCatalogConnectionFailureKind::Routing
                    }
                },
            ),
        }
    }
}

fn failed_connection<C>(
    negotiation: Option<EngineCatalogNegotiationEvidence>,
    kind: EngineCatalogConnectionFailureKind,
) -> EngineCatalogConnection<C> {
    EngineCatalogConnection::Failed {
        negotiation,
        failure: EngineCatalogConnectionFailure { kind },
    }
}

pub struct SharedObjectStoreConnector<S> {
    secrets: Arc<S>,
}

impl<S> SharedObjectStoreConnector<S> {
    #[must_use]
    pub fn new(secrets: Arc<S>) -> Self {
        Self { secrets }
    }
}

impl<S> Clone for SharedObjectStoreConnector<S> {
    fn clone(&self) -> Self {
        Self {
            secrets: Arc::clone(&self.secrets),
        }
    }
}

impl<S> EngineObjectStoreConnector for SharedObjectStoreConnector<S>
where
    S: SecretSource + Send + Sync + 'static,
{
    type Store = ObjectStoreAuditor;

    fn connect(&self, plan: &InteroperabilityPlan) -> Result<Self::Store, ObjectStoreFailure> {
        ObjectStoreAuditor::from_connection(plan.object_store(), |name| {
            optional_secret(self.secrets.as_ref(), name)
        })
    }
}

pub(crate) fn optional_secret(source: &(impl SecretSource + ?Sized), name: &str) -> Option<String> {
    match source.read_secret(name) {
        SecretRead::Value(value) => Some(value),
        SecretRead::Missing | SecretRead::Unreadable => None,
    }
}
