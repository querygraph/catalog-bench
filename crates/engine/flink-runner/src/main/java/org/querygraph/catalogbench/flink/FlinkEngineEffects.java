package org.querygraph.catalogbench.flink;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.CatalogDescriptor;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkCatalog;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.NestedField;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

final class FlinkEngineEffects implements EngineEffects {
    static final String OAUTH_CLIENT_ID = "CATALOG_BENCH_ENGINE_CLIENT_ID";
    static final String OAUTH_CLIENT_SECRET = "CATALOG_BENCH_ENGINE_CLIENT_SECRET";
    private static final int MAX_SECRET_CHARS = 4096;
    private static final Pattern SAFE_IDENTIFIER = Pattern.compile("[a-z0-9_]+");

    private final Function<String, String> environment;
    private final ObjectMapper mapper;
    private TableEnvironment tables;
    private FlinkCatalog flinkCatalog;

    FlinkEngineEffects() {
        this(System::getenv, null);
    }

    FlinkEngineEffects(
            Function<String, String> environment,
            TableEnvironment tables) {
        this.environment = environment;
        this.tables = tables;
        this.mapper = new ObjectMapper();
    }

    @Override
    public ChildEvent.RuntimeObservation runtimeObservation() throws EffectFailure {
        try {
            tableEnvironment();
            return new ChildEvent.RuntimeObservation(
                    EnvironmentInformation.getVersion(),
                    Map.of(
                            "java", System.getProperty("java.version"),
                            "scala", scalaVersion()),
                    System.getProperty("os.name"),
                    System.getProperty("os.arch"));
        } catch (ReflectiveOperationException | RuntimeException failure) {
            throw new EffectFailure(failure);
        }
    }

    @Override
    public void initializeCatalog(Program.CatalogSetup setup) throws EffectFailure {
        try {
            Map<String, String> properties = catalogProperties(setup, environment);
            TableEnvironment environment = tableEnvironment();
            environment.createCatalog(
                    setup.name(),
                    CatalogDescriptor.of(setup.name(), Configuration.fromMap(properties)));
            environment.useCatalog(setup.name());
            flinkCatalog = environment.getCatalog(setup.name())
                    .filter(FlinkCatalog.class::isInstance)
                    .map(FlinkCatalog.class::cast)
                    .orElseThrow();
        } catch (EffectFailure failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw new EffectFailure(failure);
        }
    }

    @Override
    public boolean fixtureAbsent(Program.FixtureTarget fixture) throws EffectFailure {
        try {
            return Arrays.stream(tableEnvironment().listDatabases())
                    .noneMatch(fixture.namespace()::equals);
        } catch (EffectFailure failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw new EffectFailure(failure);
        }
    }

    @Override
    public void execute(Program.Operation operation) throws EffectFailure {
        try {
            tableEnvironment().executeSql(operation.sql()).await();
        } catch (EffectFailure failure) {
            throw failure;
        } catch (Exception failure) {
            throw effectFailure(failure);
        }
    }

    @Override
    public boolean namespaceListedExactly(Program.FixtureTarget fixture) throws EffectFailure {
        try {
            return Arrays.stream(tableEnvironment().listDatabases())
                    .filter(fixture.namespace()::equals)
                    .count() == 1;
        } catch (EffectFailure failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw new EffectFailure(failure);
        }
    }

    @Override
    public ChildEvent.TableObservation observeTable(
            Program.FixtureTarget fixture,
            Program.ObservationPolicy policy) throws EffectFailure {
        try {
            Table table = icebergCatalog().loadTable(identifier(fixture));
            table.refresh();
            List<ChildEvent.FieldObservation> schema = new ArrayList<>();
            int maximumFields = Math.addExact(policy.initialSchema().size(), 1);
            for (NestedField field : table.schema().columns()) {
                if (schema.size() >= maximumFields
                        || field.name().length() > 128
                        || !SAFE_IDENTIFIER.matcher(field.name()).matches()) {
                    throw new EffectFailure();
                }
                schema.add(new ChildEvent.FieldObservation(
                        field.fieldId(),
                        field.name(),
                        field.isRequired(),
                        primitiveType(field.type().typeId())));
            }
            Map<String, ChildEvent.PropertyObservation> properties = new TreeMap<>();
            policy.properties().forEach((key, expected) -> properties.put(
                    key,
                    expected.equals(table.properties().get(key))
                            ? ChildEvent.PropertyObservation.MATCH
                            : ChildEvent.PropertyObservation.MISMATCH));
            String location = safeTableRoute(table.location(), fixture.bucket());
            String metadataLocation = safeTableRoute(
                    ((SerializableTable) SerializableTable.copyOf(table)).metadataFileLocation(),
                    fixture.bucket());
            UUID uuid = table.uuid();
            if (uuid.version() == 0 || uuid.equals(new UUID(0, 0))) {
                throw new EffectFailure();
            }
            return new ChildEvent.TableObservation(
                    uuid.toString(),
                    metadataLocation,
                    location,
                    TableUtil.formatVersion(table),
                    table.schema().highestFieldId(),
                    List.copyOf(schema),
                    StreamSupport.stream(table.snapshots().spliterator(), false).count(),
                    Map.copyOf(properties));
        } catch (EffectFailure failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw new EffectFailure(failure);
        }
    }

    @Override
    public ChildEvent.ReadObservation read(
            Program.Operation operation,
            Program.ReadOracle oracle) throws EffectFailure {
        try {
            CanonicalRead identity = new CanonicalRead(mapper, oracle);
            TableResult result = tableEnvironment().executeSql(operation.sql());
            CloseableIterator<Row> rows = result.collect();
            try {
                while (rows.hasNext()) {
                    identity.add(rows.next());
                }
            } finally {
                close(rows);
            }
            return identity.finish();
        } catch (EffectFailure failure) {
            throw failure;
        } catch (Exception failure) {
            throw new EffectFailure(failure);
        }
    }

    @Override
    public long snapshotCount(Program.SnapshotRead operation) throws EffectFailure {
        try {
            long count = 0;
            TableResult result = tableEnvironment().executeSql(operation.sql());
            CloseableIterator<Row> rows = result.collect();
            try {
                while (rows.hasNext()) {
                    rows.next();
                    if (count >= ProgramCodec.MAX_READ_ROWS) {
                        throw new EffectFailure();
                    }
                    count = Math.addExact(count, 1);
                }
            } finally {
                close(rows);
            }
            return count;
        } catch (Exception failure) {
            throw effectFailure(failure);
        }
    }

    static Map<String, String> catalogProperties(
            Program.CatalogSetup setup,
            Function<String, String> environment) throws EffectFailure {
        Map<String, String> properties = new TreeMap<>(setup.properties());
        if (setup.authentication() instanceof Program.OAuthClientCredentials oauth) {
            String clientId = requiredSecret(environment, OAUTH_CLIENT_ID);
            String clientSecret = requiredSecret(environment, OAUTH_CLIENT_SECRET);
            if (clientId.indexOf(':') >= 0) {
                throw new EffectFailure();
            }
            properties.put("credential", clientId + ":" + clientSecret);
            properties.put("oauth2-server-uri", oauth.oauth2ServerUri());
            properties.put("scope", oauth.scope());
        }
        return Map.copyOf(properties);
    }

    private static TableEnvironment createTableEnvironment() {
        TableEnvironment tables = TableEnvironment.create(
                EnvironmentSettings.newInstance().inBatchMode().build());
        tables.getConfig().getConfiguration().set(CoreOptions.DEFAULT_PARALLELISM, 1);
        return tables;
    }

    private TableEnvironment tableEnvironment() throws EffectFailure {
        if (tables == null) {
            try {
                tables = createTableEnvironment();
            } catch (RuntimeException failure) {
                throw new EffectFailure(failure);
            }
        }
        return tables;
    }

    private static String requiredSecret(
            Function<String, String> environment,
            String name) throws EffectFailure {
        String secret = environment.apply(name);
        if (secret == null || secret.isEmpty() || secret.length() > MAX_SECRET_CHARS) {
            throw new EffectFailure();
        }
        return secret;
    }

    private static String scalaVersion()
            throws ClassNotFoundException, NoSuchFieldException, IllegalAccessException,
                    NoSuchMethodException, InvocationTargetException {
        Class<?> properties = Class.forName("scala.util.Properties$");
        Object module = properties.getField("MODULE$").get(null);
        return (String) properties.getMethod("versionNumberString").invoke(module);
    }

    static Program.PrimitiveType primitiveType(Type.TypeID type) throws EffectFailure {
        return switch (type) {
            case LONG -> Program.PrimitiveType.LONG;
            case STRING -> Program.PrimitiveType.STRING;
            default -> throw new EffectFailure();
        };
    }

    private static TableIdentifier identifier(Program.FixtureTarget fixture) {
        return TableIdentifier.of(Namespace.of(fixture.namespace()), fixture.table());
    }

    private org.apache.iceberg.catalog.Catalog icebergCatalog() throws EffectFailure {
        if (flinkCatalog == null) {
            throw new EffectFailure();
        }
        return flinkCatalog.catalog();
    }

    static String safeTableRoute(String value, String bucket) throws EffectFailure {
        try {
            URI route = new URI(value);
            if (!"s3".equals(route.getScheme())
                    || !bucket.equals(route.getHost())
                    || route.getUserInfo() != null
                    || route.getQuery() != null
                    || route.getFragment() != null
                    || route.getPath() == null
                    || route.getPath().isEmpty()
                    || Arrays.stream(route.getPath().split("/"))
                            .anyMatch(segment -> segment.equals(".") || segment.equals(".."))
                    || value.length() > 4096) {
                throw new EffectFailure();
            }
            return value;
        } catch (URISyntaxException failure) {
            throw new EffectFailure(failure);
        }
    }

    private static void close(CloseableIterator<Row> rows) throws EffectFailure {
        try {
            rows.close();
        } catch (Exception failure) {
            throw effectFailure(failure);
        }
    }

    private static EffectFailure effectFailure(Exception failure) {
        if (failure instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        return new EffectFailure(failure);
    }
}
