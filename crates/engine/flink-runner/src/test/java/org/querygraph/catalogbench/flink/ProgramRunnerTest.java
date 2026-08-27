package org.querygraph.catalogbench.flink;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ProgramRunnerTest {
    @Test
    void emitsTheCompleteSharedProtocolInOrder() throws Exception {
        Program program = program();
        FakeEffects effects = new FakeEffects();
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        int exit = new ProgramRunner(effects, sink(output)).run(program);

        assertEquals(ProgramRunner.SUCCESS_EXIT, exit);
        assertEquals(5, effects.executed.size());
        assertEquals(List.of(
                        "runtime-ready",
                        "catalog-ready",
                        "fixture-preflight",
                        "namespace-ready",
                        "table-ready",
                        "initial-appended",
                        "initial-read",
                        "schema-evolved",
                        "evolved-appended",
                        "evolved-read",
                        "final-table",
                        "completed"),
                eventNames(output));
        String evidence = output.toString(StandardCharsets.UTF_8);
        assertFalse(evidence.contains("private"));
        assertFalse(evidence.contains("Exception"));
    }

    @Test
    void collisionIsTerminalAndOpensNoMutation() throws Exception {
        FakeEffects effects = new FakeEffects();
        effects.absent = false;
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        int exit = new ProgramRunner(effects, sink(output)).run(program());

        assertEquals(ProgramRunner.FIXTURE_COLLISION_EXIT, exit);
        assertTrue(effects.executed.isEmpty());
        assertEquals(
                List.of("runtime-ready", "catalog-ready", "fixture-preflight"),
                eventNames(output));
        assertTrue(output.toString(StandardCharsets.UTF_8).contains("\"absent\":false"));
    }

    @Test
    void readMismatchUsesOnlyTheClosedDataFailure() throws Exception {
        FakeEffects effects = new FakeEffects();
        effects.mismatchRead = true;
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        int exit = new ProgramRunner(effects, sink(output)).run(program());

        assertEquals(ProgramRunner.FAILURE_EXIT, exit);
        String last = eventLines(output).get(eventLines(output).size() - 1);
        assertTrue(last.contains("\"event\":\"failed\""));
        assertTrue(last.contains("\"stage\":\"read-initial\""));
        assertTrue(last.contains("\"category\":\"data\""));
        assertFalse(last.contains("actual"));
    }

    @Test
    void namespaceObservationFailureCannotContinue() throws Exception {
        FakeEffects effects = new FakeEffects();
        effects.namespaceListed = false;
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        int exit = new ProgramRunner(effects, sink(output)).run(program());

        assertEquals(ProgramRunner.FAILURE_EXIT, exit);
        assertEquals(1, effects.executed.size());
        assertTrue(eventLines(output).get(eventLines(output).size() - 1)
                .contains("\"stage\":\"create-namespace\""));
    }

    @Test
    void eventSinkRejectsAnOversizedTypedEvent() {
        EventSink events = sink(new ByteArrayOutputStream());
        String oversized = "x".repeat(EventSink.MAX_EVENT_BYTES);
        ChildEvent.RuntimeObservation runtime = new ChildEvent.RuntimeObservation(
                oversized, Map.of(), "Linux", "aarch64");

        assertThrows(EventSink.EventFailure.class,
                () -> events.emit(new ChildEvent.RuntimeReady(runtime)));
    }

    private static Program program() throws ProgramCodec.ProgramViolation {
        return ProgramCodec.decode(
                ProgramCodecTest.validProgram().getBytes(StandardCharsets.UTF_8));
    }

    private static EventSink sink(ByteArrayOutputStream output) {
        return new EventSink(new PrintStream(output, true, StandardCharsets.UTF_8));
    }

    private static List<String> eventNames(ByteArrayOutputStream output) {
        return eventLines(output).stream()
                .map(line -> line.replaceFirst(".*\\\"event\\\":\\\"([^\\\"]+)\\\".*", "$1"))
                .toList();
    }

    private static List<String> eventLines(ByteArrayOutputStream output) {
        return output.toString(StandardCharsets.UTF_8).lines().toList();
    }

    private static final class FakeEffects implements EngineEffects {
        private final List<Class<?>> executed = new ArrayList<>();
        private boolean absent = true;
        private boolean namespaceListed = true;
        private boolean mismatchRead;
        private boolean evolved;
        private long snapshots;

        @Override
        public ChildEvent.RuntimeObservation runtimeObservation() {
            return new ChildEvent.RuntimeObservation(
                    "2.1.3",
                    Map.of("java", "17.0.20", "scala", "2.12.20"),
                    "Linux",
                    "aarch64");
        }

        @Override
        public void initializeCatalog(Program.CatalogSetup catalog) {}

        @Override
        public boolean fixtureAbsent(Program.FixtureTarget fixture) {
            return absent;
        }

        @Override
        public void execute(Program.Operation operation) {
            executed.add(operation.getClass());
            if (operation instanceof Program.AddColumn) {
                evolved = true;
            }
            if (operation instanceof Program.InitialAppend
                    || operation instanceof Program.EvolvedAppend) {
                snapshots++;
            }
        }

        @Override
        public boolean namespaceListedExactly(Program.FixtureTarget fixture) {
            return namespaceListed;
        }

        @Override
        public ChildEvent.TableObservation observeTable(
                Program.FixtureTarget fixture, Program.ObservationPolicy policy) {
            List<ChildEvent.FieldObservation> fields = policy.initialSchema().stream()
                    .map(field -> new ChildEvent.FieldObservation(
                            field.id(), field.name(), field.required(), field.type()))
                    .collect(java.util.stream.Collectors.toCollection(ArrayList::new));
            if (evolved) {
                Program.EvolutionField field = policy.evolvedField();
                fields.add(new ChildEvent.FieldObservation(
                        4, field.name(), field.required(), field.type()));
            }
            return new ChildEvent.TableObservation(
                    "00000000-0000-0000-0000-000000000001",
                    "s3://warehouse/ns/events/metadata/v1.metadata.json",
                    "s3://warehouse/ns/events",
                    2,
                    evolved ? 4 : 3,
                    fields,
                    snapshots,
                    Map.of("catalog-bench.owner", ChildEvent.PropertyObservation.MATCH));
        }

        @Override
        public ChildEvent.ReadObservation read(
                Program.Operation operation, Program.ReadOracle oracle) {
            if (mismatchRead) {
                return new ChildEvent.ReadObservation(
                        oracle.rows() + 1, oracle.bytes(), oracle.sha256());
            }
            return ChildEvent.ReadObservation.fromOracle(oracle);
        }

        @Override
        public long snapshotCount(Program.SnapshotRead operation) {
            return snapshots;
        }
    }
}
