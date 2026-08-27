package org.querygraph.catalogbench.flink;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.List;
import java.util.Map;

public record Program(
        int parallelism,
        CatalogSetup catalog,
        FixtureTarget fixture,
        ObservationPolicy observation,
        List<Operation> operations) {

    public record CatalogSetup(
            String name,
            Map<String, String> properties,
            Authentication authentication) {}

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
    @JsonSubTypes({
        @JsonSubTypes.Type(value = Anonymous.class, name = "anonymous"),
        @JsonSubTypes.Type(
                value = OAuthClientCredentials.class,
                name = "oauth2-client-credentials")
    })
    public sealed interface Authentication permits Anonymous, OAuthClientCredentials {}

    public record Anonymous() implements Authentication {}

    public record OAuthClientCredentials(
            @JsonProperty("oauth2_server_uri") String oauth2ServerUri,
            String scope) implements Authentication {}

    public record FixtureTarget(
            String namespace,
            String table,
            @JsonProperty("requested_location") String requestedLocation,
            String bucket) {}

    public record ObservationPolicy(
            @JsonProperty("format_version") int formatVersion,
            @JsonProperty("initial_schema") List<Field> initialSchema,
            @JsonProperty("evolved_field") EvolutionField evolvedField,
            Map<String, String> properties) {}

    public record Field(int id, String name, boolean required, PrimitiveType type) {}

    public record EvolutionField(String name, boolean required, PrimitiveType type) {}

    public enum PrimitiveType {
        LONG("long"),
        STRING("string");

        private final String wireName;

        PrimitiveType(String wireName) {
            this.wireName = wireName;
        }

        @JsonCreator
        public static PrimitiveType fromWireName(String value) {
            for (PrimitiveType type : values()) {
                if (type.wireName.equals(value)) {
                    return type;
                }
            }
            throw new IllegalArgumentException("unsupported primitive type");
        }

        @JsonValue
        public String wireName() {
            return wireName;
        }
    }

    public record ReadOracle(long bytes, List<String> columns, long rows, String sha256) {}

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "operation")
    @JsonSubTypes({
        @JsonSubTypes.Type(value = CreateNamespace.class, name = "create-namespace"),
        @JsonSubTypes.Type(value = CreateTable.class, name = "create-table"),
        @JsonSubTypes.Type(value = InitialAppend.class, name = "initial-append"),
        @JsonSubTypes.Type(value = InitialRead.class, name = "initial-read"),
        @JsonSubTypes.Type(value = AddColumn.class, name = "add-column"),
        @JsonSubTypes.Type(value = EvolvedAppend.class, name = "evolved-append"),
        @JsonSubTypes.Type(value = EvolvedRead.class, name = "evolved-read"),
        @JsonSubTypes.Type(value = SnapshotRead.class, name = "snapshot-read")
    })
    public sealed interface Operation permits CreateNamespace, CreateTable, InitialAppend,
            InitialRead, AddColumn, EvolvedAppend, EvolvedRead, SnapshotRead {
        String sql();
    }

    public record CreateNamespace(String sql) implements Operation {}
    public record CreateTable(String sql) implements Operation {}
    public record InitialAppend(String sql) implements Operation {}
    public record InitialRead(String sql, ReadOracle expected) implements Operation {}
    public record AddColumn(String sql) implements Operation {}
    public record EvolvedAppend(String sql) implements Operation {}
    public record EvolvedRead(String sql, ReadOracle expected) implements Operation {}
    public record SnapshotRead(String sql) implements Operation {}
}
