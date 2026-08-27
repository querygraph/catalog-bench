package org.querygraph.catalogbench.flink;

interface EngineEffects {
    ChildEvent.RuntimeObservation runtimeObservation() throws EffectFailure;

    void initializeCatalog(Program.CatalogSetup catalog) throws EffectFailure;

    boolean fixtureAbsent(Program.FixtureTarget fixture) throws EffectFailure;

    void execute(Program.Operation operation) throws EffectFailure;

    boolean namespaceListedExactly(Program.FixtureTarget fixture) throws EffectFailure;

    ChildEvent.TableObservation observeTable(
            Program.FixtureTarget fixture, Program.ObservationPolicy policy) throws EffectFailure;

    ChildEvent.ReadObservation read(Program.Operation operation, Program.ReadOracle oracle)
            throws EffectFailure;

    long snapshotCount(Program.SnapshotRead operation) throws EffectFailure;

    final class EffectFailure extends Exception {
        EffectFailure() {}

        EffectFailure(Throwable cause) {
            super(cause);
        }
    }
}
