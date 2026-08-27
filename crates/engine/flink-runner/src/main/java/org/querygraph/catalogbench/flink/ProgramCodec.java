package org.querygraph.catalogbench.flink;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public final class ProgramCodec {
    static final int MAX_PROGRAM_BYTES = 256 * 1024;
    private static final int MAX_TEXT_CHARS = 4096;
    private static final Pattern IDENTIFIER = Pattern.compile("[a-z0-9_]+");
    private static final Pattern SHA256 = Pattern.compile("[0-9a-f]{64}");
    private static final Set<String> REQUIRED_CATALOG_PROPERTIES = Set.of(
            "type", "catalog-type", "uri", "io-impl", "s3.endpoint", "s3.region",
            "s3.path-style-access");
    private static final Set<String> OPTIONAL_CATALOG_PROPERTIES = Set.of("warehouse", "prefix");
    private static final List<Class<? extends Program.Operation>> OPERATION_ORDER = List.of(
            Program.CreateNamespace.class,
            Program.CreateTable.class,
            Program.InitialAppend.class,
            Program.InitialRead.class,
            Program.AddColumn.class,
            Program.EvolvedAppend.class,
            Program.EvolvedRead.class,
            Program.SnapshotRead.class);

    private static final ObjectMapper MAPPER = new ObjectMapper(
            JsonFactory.builder()
                    .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                    .streamReadConstraints(StreamReadConstraints.builder()
                            .maxNestingDepth(32)
                            .maxStringLength(MAX_TEXT_CHARS)
                            .maxNameLength(128)
                            .maxNumberLength(32)
                            .build())
                    .build())
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    private ProgramCodec() {}

    public static Program read(Path path) throws ProgramViolation {
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new ProgramViolation("program must be one regular non-symlink file");
        }
        final byte[] bytes;
        try {
            long size = Files.size(path);
            if (size <= 0 || size > MAX_PROGRAM_BYTES) {
                throw new ProgramViolation("program byte length is outside the closed bound");
            }
            bytes = Files.readAllBytes(path);
        } catch (IOException error) {
            throw new ProgramViolation("program cannot be read", error);
        }
        if (bytes.length > MAX_PROGRAM_BYTES) {
            throw new ProgramViolation("program byte length is outside the closed bound");
        }
        return decode(bytes);
    }

    static Program decode(byte[] bytes) throws ProgramViolation {
        try {
            Program program = MAPPER.readValue(bytes, Program.class);
            validate(program);
            return program;
        } catch (IOException | IllegalArgumentException | NullPointerException error) {
            throw new ProgramViolation("program does not match the closed envelope", error);
        }
    }

    static void validate(Program program) throws ProgramViolation {
        require(program.parallelism() == 1, "parallelism must be exactly one");
        validateCatalog(program.catalog());
        validateFixture(program.fixture());
        validateObservation(program.observation());
        List<Program.Operation> operations = List.copyOf(program.operations());
        require(operations.size() == OPERATION_ORDER.size(), "operation count differs");
        for (int index = 0; index < operations.size(); index++) {
            Program.Operation operation = operations.get(index);
            require(OPERATION_ORDER.get(index).isInstance(operation), "operation order differs");
            boundedText(operation.sql(), "operation SQL");
            require(operation.sql().chars().noneMatch(Character::isISOControl),
                    "operation SQL contains a control character");
        }
        validateSqlShapes(operations);
        List<String> initialColumns = program.observation().initialSchema().stream()
                .map(Program.Field::name)
                .toList();
        List<String> evolvedColumns = new java.util.ArrayList<>(initialColumns);
        evolvedColumns.add(program.observation().evolvedField().name());
        validateOracle(((Program.InitialRead) operations.get(3)).expected(), initialColumns);
        validateOracle(((Program.EvolvedRead) operations.get(6)).expected(), evolvedColumns);
    }

    private static void validateCatalog(Program.CatalogSetup catalog) throws ProgramViolation {
        identifier(catalog.name(), "catalog name");
        Map<String, String> properties = Map.copyOf(catalog.properties());
        require(properties.keySet().containsAll(REQUIRED_CATALOG_PROPERTIES),
                "catalog properties are incomplete");
        Set<String> allowed = new HashSet<>(REQUIRED_CATALOG_PROPERTIES);
        allowed.addAll(OPTIONAL_CATALOG_PROPERTIES);
        require(allowed.containsAll(properties.keySet()), "catalog properties are not closed");
        require("iceberg".equals(properties.get("type")), "catalog type differs");
        require("rest".equals(properties.get("catalog-type")), "Iceberg catalog type differs");
        require("org.apache.iceberg.aws.s3.S3FileIO".equals(properties.get("io-impl")),
                "file IO differs");
        require("true".equals(properties.get("s3.path-style-access")),
                "path style differs");
        httpUri(properties.get("uri"), "catalog URI");
        httpUri(properties.get("s3.endpoint"), "S3 endpoint");
        for (Map.Entry<String, String> property : properties.entrySet()) {
            boundedText(property.getKey(), "catalog property name");
            boundedText(property.getValue(), "catalog property value");
            String key = property.getKey().toLowerCase();
            require(!key.contains("credential") && !key.contains("secret")
                            && !key.contains("access-key") && !key.contains("token"),
                    "catalog properties contain credential material");
        }
        require(catalog.authentication() != null, "authentication is absent");
        if (catalog.authentication() instanceof Program.OAuthClientCredentials oauth) {
            httpUri(oauth.oauth2ServerUri(), "OAuth server URI");
            boundedText(oauth.scope(), "OAuth scope");
        }
    }

    private static void validateFixture(Program.FixtureTarget fixture) throws ProgramViolation {
        identifier(fixture.namespace(), "fixture namespace");
        identifier(fixture.table(), "fixture table");
        identifier(fixture.bucket(), "fixture bucket");
        if (fixture.requestedLocation() != null) {
            boundedText(fixture.requestedLocation(), "requested location");
            try {
                URI location = new URI(fixture.requestedLocation());
                require("s3".equals(location.getScheme())
                                && fixture.bucket().equals(location.getHost())
                                && location.getRawUserInfo() == null
                                && location.getRawQuery() == null
                                && location.getRawFragment() == null
                                && location.getRawPath() != null
                                && !location.getRawPath().equals("/"),
                        "requested location leaves the fixture bucket");
            } catch (URISyntaxException error) {
                throw new ProgramViolation("requested location is invalid", error);
            }
        }
    }

    private static void validateObservation(Program.ObservationPolicy observation)
            throws ProgramViolation {
        require(observation.formatVersion() == 2, "format version differs");
        List<Program.Field> fields = List.copyOf(observation.initialSchema());
        require(!fields.isEmpty(), "initial schema is empty");
        Set<Integer> ids = new HashSet<>();
        Set<String> names = new HashSet<>();
        for (Program.Field field : fields) {
            require(field.id() > 0 && ids.add(field.id()), "field ID is invalid or duplicated");
            identifier(field.name(), "field name");
            require(names.add(field.name()), "field name is duplicated");
            require(field.type() != null, "field type is absent");
        }
        Program.EvolutionField evolved = observation.evolvedField();
        identifier(evolved.name(), "evolved field name");
        require(names.add(evolved.name()), "evolved field already exists");
        require(evolved.type() != null, "evolved field type is absent");
        for (Map.Entry<String, String> property : Map.copyOf(observation.properties()).entrySet()) {
            boundedText(property.getKey(), "observation property name");
            boundedText(property.getValue(), "observation property value");
            String key = property.getKey().toLowerCase();
            require(!key.contains("credential") && !key.contains("secret")
                            && !key.contains("token"),
                    "observation policy contains credential material");
        }
    }

    private static void validateOracle(Program.ReadOracle oracle, List<String> expectedColumns)
            throws ProgramViolation {
        require(oracle.rows() > 0 && oracle.bytes() > 0, "read oracle counts must be positive");
        require(SHA256.matcher(oracle.sha256()).matches(), "read oracle SHA-256 is invalid");
        List<String> columns = List.copyOf(oracle.columns());
        require(!columns.isEmpty(), "read oracle columns are empty");
        Set<String> unique = new HashSet<>();
        for (String column : columns) {
            identifier(column, "read column");
            require(unique.add(column), "read column is duplicated");
        }
        require(columns.equals(expectedColumns), "read oracle columns differ from observation schema");
    }

    private static void validateSqlShapes(List<Program.Operation> operations)
            throws ProgramViolation {
        String[] prefixes = {
            "CREATE DATABASE IF NOT EXISTS ",
            "CREATE TABLE ",
            "INSERT INTO ",
            "SELECT ",
            "ALTER TABLE ",
            "INSERT INTO ",
            "SELECT ",
            "SELECT * FROM "
        };
        for (int index = 0; index < operations.size(); index++) {
            require(operations.get(index).sql().startsWith(prefixes[index]),
                    "operation SQL does not match its tagged effect");
        }
        require(operations.get(4).sql().contains(" ADD "),
                "schema operation is not additive");
    }

    private static void httpUri(String value, String label) throws ProgramViolation {
        boundedText(value, label);
        try {
            URI uri = new URI(value);
            require(("http".equals(uri.getScheme()) || "https".equals(uri.getScheme()))
                            && uri.getHost() != null
                            && uri.getRawUserInfo() == null
                            && uri.getRawQuery() == null
                            && uri.getRawFragment() == null,
                    label + " is not a credential-free HTTP(S) URI");
        } catch (URISyntaxException error) {
            throw new ProgramViolation(label + " is invalid", error);
        }
    }

    private static void identifier(String value, String label) throws ProgramViolation {
        boundedText(value, label);
        require(IDENTIFIER.matcher(value).matches(), label + " is outside the closed vocabulary");
    }

    private static void boundedText(String value, String label) throws ProgramViolation {
        require(value != null && !value.isEmpty() && value.length() <= MAX_TEXT_CHARS,
                label + " is empty or too long");
    }

    private static void require(boolean condition, String message) throws ProgramViolation {
        if (!condition) {
            throw new ProgramViolation(message);
        }
    }

    public static final class ProgramViolation extends Exception {
        ProgramViolation(String message) {
            super(message);
        }

        ProgramViolation(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
