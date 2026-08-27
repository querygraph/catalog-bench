package org.querygraph.catalogbench.flink;

import java.util.List;

final class ProgramRunner {
    static final int SUCCESS_EXIT = 0;
    static final int FAILURE_EXIT = 2;
    static final int FIXTURE_COLLISION_EXIT = 3;

    private final EngineEffects effects;
    private final EventSink events;

    ProgramRunner(EngineEffects effects, EventSink events) {
        this.effects = effects;
        this.events = events;
    }

    int run(Program program) {
        List<Program.Operation> operations = program.operations();
        if (!emitRuntime()) {
            return FAILURE_EXIT;
        }
        if (!effect(
                () -> effects.initializeCatalog(program.catalog()),
                new ChildEvent.CatalogReady(),
                ChildEvent.Stage.INITIALIZE_CATALOG,
                ChildEvent.FailureCategory.CONNECTOR)) {
            return FAILURE_EXIT;
        }
        final boolean absent;
        try {
            absent = effects.fixtureAbsent(program.fixture());
            events.emit(new ChildEvent.FixturePreflight(absent));
        } catch (EngineEffects.EffectFailure | EventSink.EventFailure failure) {
            return fail(
                    ChildEvent.Stage.PREFLIGHT_FIXTURE,
                    ChildEvent.FailureCategory.CATALOG);
        }
        if (!absent) {
            return FIXTURE_COLLISION_EXIT;
        }
        if (!createNamespace(program, (Program.CreateNamespace) operations.get(0))) {
            return FAILURE_EXIT;
        }
        if (!mutateAndObserve(
                (Program.CreateTable) operations.get(1),
                program,
                ChildEvent.Stage.CREATE_TABLE,
                ChildEvent.FailureCategory.CONNECTOR,
                ObservationEvent.TABLE_READY)) {
            return FAILURE_EXIT;
        }
        Program.SnapshotRead snapshots = (Program.SnapshotRead) operations.get(7);
        if (!append(
                (Program.InitialAppend) operations.get(2),
                snapshots,
                ChildEvent.Stage.APPEND_INITIAL,
                true)) {
            return FAILURE_EXIT;
        }
        if (!read(
                (Program.InitialRead) operations.get(3),
                ChildEvent.Stage.READ_INITIAL,
                true)) {
            return FAILURE_EXIT;
        }
        if (!mutateAndObserve(
                (Program.AddColumn) operations.get(4),
                program,
                ChildEvent.Stage.EVOLVE_SCHEMA,
                ChildEvent.FailureCategory.CONNECTOR,
                ObservationEvent.SCHEMA_EVOLVED)) {
            return FAILURE_EXIT;
        }
        if (!append(
                (Program.EvolvedAppend) operations.get(5),
                snapshots,
                ChildEvent.Stage.APPEND_EVOLVED,
                false)) {
            return FAILURE_EXIT;
        }
        if (!read(
                (Program.EvolvedRead) operations.get(6),
                ChildEvent.Stage.READ_EVOLVED,
                false)) {
            return FAILURE_EXIT;
        }
        try {
            ChildEvent.TableObservation table =
                    effects.observeTable(program.fixture(), program.observation());
            events.emit(new ChildEvent.FinalTable(table));
            events.emit(new ChildEvent.Completed());
            return SUCCESS_EXIT;
        } catch (EngineEffects.EffectFailure | EventSink.EventFailure failure) {
            return fail(
                    ChildEvent.Stage.OBSERVE_FINAL_TABLE,
                    ChildEvent.FailureCategory.CONNECTOR);
        }
    }

    private boolean emitRuntime() {
        try {
            events.emit(new ChildEvent.RuntimeReady(effects.runtimeObservation()));
            return true;
        } catch (EngineEffects.EffectFailure | EventSink.EventFailure failure) {
            fail(ChildEvent.Stage.VERIFY_RUNTIME, ChildEvent.FailureCategory.RUNTIME);
            return false;
        }
    }

    private boolean createNamespace(Program program, Program.CreateNamespace operation) {
        try {
            effects.execute(operation);
            boolean listedExactly = effects.namespaceListedExactly(program.fixture());
            if (!listedExactly) {
                fail(ChildEvent.Stage.CREATE_NAMESPACE, ChildEvent.FailureCategory.CATALOG);
                return false;
            }
            events.emit(new ChildEvent.NamespaceReady(true));
            return true;
        } catch (EngineEffects.EffectFailure | EventSink.EventFailure failure) {
            fail(ChildEvent.Stage.CREATE_NAMESPACE, ChildEvent.FailureCategory.CATALOG);
            return false;
        }
    }

    private boolean mutateAndObserve(
            Program.Operation operation,
            Program program,
            ChildEvent.Stage stage,
            ChildEvent.FailureCategory category,
            ObservationEvent event) {
        try {
            effects.execute(operation);
            ChildEvent.TableObservation table =
                    effects.observeTable(program.fixture(), program.observation());
            events.emit(event.create(table));
            return true;
        } catch (EngineEffects.EffectFailure | EventSink.EventFailure failure) {
            fail(stage, category);
            return false;
        }
    }

    private boolean append(
            Program.Operation operation,
            Program.SnapshotRead snapshots,
            ChildEvent.Stage stage,
            boolean initial) {
        try {
            effects.execute(operation);
            long count = effects.snapshotCount(snapshots);
            events.emit(initial
                    ? new ChildEvent.InitialAppended(count)
                    : new ChildEvent.EvolvedAppended(count));
            return true;
        } catch (EngineEffects.EffectFailure | EventSink.EventFailure failure) {
            fail(stage, ChildEvent.FailureCategory.DATA);
            return false;
        }
    }

    private boolean read(Program.Operation operation, ChildEvent.Stage stage, boolean initial) {
        Program.ReadOracle oracle = operation instanceof Program.InitialRead read
                ? read.expected()
                : ((Program.EvolvedRead) operation).expected();
        try {
            ChildEvent.ReadObservation observed = effects.read(operation, oracle);
            ChildEvent.ReadObservation expected = ChildEvent.ReadObservation.fromOracle(oracle);
            if (!expected.equals(observed)) {
                fail(stage, ChildEvent.FailureCategory.DATA);
                return false;
            }
            events.emit(initial
                    ? new ChildEvent.InitialRead(expected)
                    : new ChildEvent.EvolvedRead(expected));
            return true;
        } catch (EngineEffects.EffectFailure | EventSink.EventFailure failure) {
            fail(stage, ChildEvent.FailureCategory.DATA);
            return false;
        }
    }

    private boolean effect(
            CheckedEffect effect,
            ChildEvent success,
            ChildEvent.Stage stage,
            ChildEvent.FailureCategory category) {
        try {
            effect.run();
            events.emit(success);
            return true;
        } catch (EngineEffects.EffectFailure | EventSink.EventFailure failure) {
            fail(stage, category);
            return false;
        }
    }

    private int fail(ChildEvent.Stage stage, ChildEvent.FailureCategory category) {
        try {
            events.emit(new ChildEvent.Failed(stage, category));
        } catch (EventSink.EventFailure ignored) {
            // The parent maps a broken event stream to a protocol failure.
        }
        return FAILURE_EXIT;
    }

    @FunctionalInterface
    private interface CheckedEffect {
        void run() throws EngineEffects.EffectFailure;
    }

    private enum ObservationEvent {
        TABLE_READY {
            @Override
            ChildEvent create(ChildEvent.TableObservation table) {
                return new ChildEvent.TableReady(table);
            }
        },
        SCHEMA_EVOLVED {
            @Override
            ChildEvent create(ChildEvent.TableObservation table) {
                return new ChildEvent.SchemaEvolved(table);
            }
        };

        abstract ChildEvent create(ChildEvent.TableObservation table);
    }
}
