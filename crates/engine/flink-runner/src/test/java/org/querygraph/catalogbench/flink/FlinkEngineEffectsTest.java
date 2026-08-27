package org.querygraph.catalogbench.flink;

import org.apache.iceberg.types.Type;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class FlinkEngineEffectsTest {
    @Test
    void anonymousCatalogPropertiesRemainSecretFree() throws Exception {
        Program.CatalogSetup setup = new Program.CatalogSetup(
                "bench",
                Map.of("type", "iceberg", "catalog-type", "rest"),
                new Program.Anonymous());

        Map<String, String> properties =
                FlinkEngineEffects.catalogProperties(setup, ignored -> null);

        assertEquals(setup.properties(), properties);
        assertFalse(properties.containsKey("credential"));
    }

    @Test
    void oauthSecretsEnterOnlyTheInMemoryStockCatalogProperties() throws Exception {
        Program.CatalogSetup setup = new Program.CatalogSetup(
                "bench",
                Map.of("type", "iceberg", "catalog-type", "rest"),
                new Program.OAuthClientCredentials("http://catalog/token", "PRINCIPAL_ROLE:ALL"));
        Map<String, String> environment = new HashMap<>();
        environment.put(FlinkEngineEffects.OAUTH_CLIENT_ID, "client");
        environment.put(FlinkEngineEffects.OAUTH_CLIENT_SECRET, "private-secret");

        Map<String, String> properties =
                FlinkEngineEffects.catalogProperties(setup, environment::get);

        assertEquals("client:private-secret", properties.get("credential"));
        assertEquals("http://catalog/token", properties.get("oauth2-server-uri"));
        assertEquals("PRINCIPAL_ROLE:ALL", properties.get("scope"));
        assertFalse(setup.properties().toString().contains("private-secret"));
    }

    @Test
    void missingOversizedOrAmbiguousCredentialsFailClosed() {
        Program.CatalogSetup setup = new Program.CatalogSetup(
                "bench",
                Map.of("type", "iceberg", "catalog-type", "rest"),
                new Program.OAuthClientCredentials("http://catalog/token", "scope"));

        assertThrows(EngineEffects.EffectFailure.class,
                () -> FlinkEngineEffects.catalogProperties(setup, ignored -> null));
        assertThrows(EngineEffects.EffectFailure.class,
                () -> FlinkEngineEffects.catalogProperties(
                        setup,
                        name -> name.equals(FlinkEngineEffects.OAUTH_CLIENT_ID)
                                ? "ambiguous:id"
                                : "secret"));
        assertThrows(EngineEffects.EffectFailure.class,
                () -> FlinkEngineEffects.catalogProperties(
                        setup,
                        name -> name.equals(FlinkEngineEffects.OAUTH_CLIENT_ID)
                                ? "client"
                                : "x".repeat(4097)));
    }

    @Test
    void observationProjectionAcceptsOnlyClosedTypesAndSafeFixtureRoutes() throws Exception {
        assertEquals(Program.PrimitiveType.LONG,
                FlinkEngineEffects.primitiveType(Type.TypeID.LONG));
        assertEquals(Program.PrimitiveType.STRING,
                FlinkEngineEffects.primitiveType(Type.TypeID.STRING));
        assertThrows(EngineEffects.EffectFailure.class,
                () -> FlinkEngineEffects.primitiveType(Type.TypeID.INTEGER));

        assertEquals("s3://warehouse/ns/events",
                FlinkEngineEffects.safeTableRoute("s3://warehouse/ns/events", "warehouse"));
        for (String unsafe : List.of(
                "s3://other/ns/events",
                "s3://user:private@warehouse/ns/events",
                "s3://warehouse/ns/events?secret=private",
                "s3://warehouse/ns/../private",
                "http://warehouse/ns/events")) {
            assertThrows(EngineEffects.EffectFailure.class,
                    () -> FlinkEngineEffects.safeTableRoute(unsafe, "warehouse"));
        }
    }
}
