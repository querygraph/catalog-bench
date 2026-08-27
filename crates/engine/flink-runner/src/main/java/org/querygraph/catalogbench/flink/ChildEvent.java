package org.querygraph.catalogbench.flink;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.List;
import java.util.Map;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "event")
@JsonSubTypes({
    @JsonSubTypes.Type(value = ChildEvent.RuntimeReady.class, name = "runtime-ready"),
    @JsonSubTypes.Type(value = ChildEvent.CatalogReady.class, name = "catalog-ready"),
    @JsonSubTypes.Type(value = ChildEvent.FixturePreflight.class, name = "fixture-preflight"),
    @JsonSubTypes.Type(value = ChildEvent.NamespaceReady.class, name = "namespace-ready"),
    @JsonSubTypes.Type(value = ChildEvent.TableReady.class, name = "table-ready"),
    @JsonSubTypes.Type(value = ChildEvent.InitialAppended.class, name = "initial-appended"),
    @JsonSubTypes.Type(value = ChildEvent.InitialRead.class, name = "initial-read"),
    @JsonSubTypes.Type(value = ChildEvent.SchemaEvolved.class, name = "schema-evolved"),
    @JsonSubTypes.Type(value = ChildEvent.EvolvedAppended.class, name = "evolved-appended"),
    @JsonSubTypes.Type(value = ChildEvent.EvolvedRead.class, name = "evolved-read"),
    @JsonSubTypes.Type(value = ChildEvent.FinalTable.class, name = "final-table"),
    @JsonSubTypes.Type(value = ChildEvent.Completed.class, name = "completed"),
    @JsonSubTypes.Type(value = ChildEvent.Failed.class, name = "failed")
})
public sealed interface ChildEvent permits ChildEvent.RuntimeReady, ChildEvent.CatalogReady,
        ChildEvent.FixturePreflight, ChildEvent.NamespaceReady, ChildEvent.TableReady,
        ChildEvent.InitialAppended, ChildEvent.InitialRead, ChildEvent.SchemaEvolved,
        ChildEvent.EvolvedAppended, ChildEvent.EvolvedRead, ChildEvent.FinalTable,
        ChildEvent.Completed, ChildEvent.Failed {

    record RuntimeObservation(
            @JsonProperty("engine_version") String engineVersion,
            Map<String, String> dependencies,
            @JsonProperty("operating_system") String operatingSystem,
            String architecture) {}

    record FieldObservation(
            int id,
            String name,
            boolean required,
            @JsonProperty("field_type") Program.PrimitiveType fieldType) {}

    enum PropertyObservation {
        MATCH("match"),
        MISMATCH("mismatch");

        private final String wireName;

        PropertyObservation(String wireName) {
            this.wireName = wireName;
        }

        @JsonValue
        public String wireName() {
            return wireName;
        }
    }

    record TableObservation(
            @JsonProperty("table_uuid") String tableUuid,
            @JsonProperty("metadata_location") String metadataLocation,
            String location,
            @JsonProperty("format_version") int formatVersion,
            @JsonProperty("last_column_id") int lastColumnId,
            List<FieldObservation> schema,
            long snapshots,
            Map<String, PropertyObservation> properties) {}

    record ReadObservation(long rows, long bytes, String sha256) {
        static ReadObservation fromOracle(Program.ReadOracle oracle) {
            return new ReadObservation(oracle.rows(), oracle.bytes(), oracle.sha256());
        }
    }

    record RuntimeReady(RuntimeObservation runtime) implements ChildEvent {}
    record CatalogReady() implements ChildEvent {}
    record FixturePreflight(boolean absent) implements ChildEvent {}
    record NamespaceReady(@JsonProperty("listed_exactly") boolean listedExactly)
            implements ChildEvent {}
    record TableReady(TableObservation table) implements ChildEvent {}
    record InitialAppended(long snapshots) implements ChildEvent {}
    record InitialRead(ReadObservation read) implements ChildEvent {}
    record SchemaEvolved(TableObservation table) implements ChildEvent {}
    record EvolvedAppended(long snapshots) implements ChildEvent {}
    record EvolvedRead(ReadObservation read) implements ChildEvent {}
    record FinalTable(TableObservation table) implements ChildEvent {}
    record Completed() implements ChildEvent {}
    record Failed(Stage stage, FailureCategory category) implements ChildEvent {}

    enum Stage {
        VERIFY_RUNTIME("verify-runtime"),
        INITIALIZE_CATALOG("initialize-catalog"),
        PREFLIGHT_FIXTURE("preflight-fixture"),
        CREATE_NAMESPACE("create-namespace"),
        CREATE_TABLE("create-table"),
        APPEND_INITIAL("append-initial"),
        READ_INITIAL("read-initial"),
        EVOLVE_SCHEMA("evolve-schema"),
        APPEND_EVOLVED("append-evolved"),
        READ_EVOLVED("read-evolved"),
        OBSERVE_FINAL_TABLE("observe-final-table");

        private final String wireName;

        Stage(String wireName) {
            this.wireName = wireName;
        }

        @JsonValue
        public String wireName() {
            return wireName;
        }
    }

    enum FailureCategory {
        RUNTIME("runtime"),
        CONNECTOR("connector"),
        CATALOG("catalog"),
        DATA("data");

        private final String wireName;

        FailureCategory(String wireName) {
            this.wireName = wireName;
        }

        @JsonValue
        public String wireName() {
            return wireName;
        }
    }
}
