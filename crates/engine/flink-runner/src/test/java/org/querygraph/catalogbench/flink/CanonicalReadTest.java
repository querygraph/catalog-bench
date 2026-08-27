package org.querygraph.catalogbench.flink;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class CanonicalReadTest {
    @Test
    void reproducesBothCrossEngineScenarioOracles() throws Exception {
        Program.ReadOracle initial = new Program.ReadOracle(
                346,
                List.of("id", "category", "amount_cents"),
                16,
                "e78b526d7e757090a9a90c80802c2a543cbf8166cfac6d6ed48c618926e85a15");
        CanonicalRead initialRead = new CanonicalRead(new ObjectMapper(), initial);
        for (long id = 0; id < 16; id++) {
            initialRead.add(Row.of(id, "category-" + id % 4, id * 100 + 7));
        }
        assertEquals(ChildEvent.ReadObservation.fromOracle(initial), initialRead.finish());

        Program.ReadOracle evolved = new Program.ReadOracle(
                570,
                List.of("id", "category", "amount_cents", "note"),
                20,
                "b2af6f475851e07d1ace3706d8867530c13dd5938bee90cfcc62d3939e01bea2");
        CanonicalRead evolvedRead = new CanonicalRead(new ObjectMapper(), evolved);
        for (long id = 0; id < 20; id++) {
            evolvedRead.add(Row.of(
                    id,
                    "category-" + id % 4,
                    id * 100 + 7,
                    id < 16 ? null : "evolved-" + id));
        }
        assertEquals(ChildEvent.ReadObservation.fromOracle(evolved), evolvedRead.finish());
    }

    @Test
    void matchesTheSharedCompactJsonLinesIdentity() throws Exception {
        byte[] payload = "[0,\"category-0\",7]\n[1,\"category-1\",107]\n"
                .getBytes(StandardCharsets.UTF_8);
        Program.ReadOracle oracle = new Program.ReadOracle(
                payload.length,
                List.of("id", "category", "amount_cents"),
                2,
                HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(payload)));
        CanonicalRead read = new CanonicalRead(new ObjectMapper(), oracle);

        read.add(Row.of(0L, "category-0", 7L));
        read.add(Row.of(1L, "category-1", 107L));

        assertEquals(ChildEvent.ReadObservation.fromOracle(oracle), read.finish());
    }

    @Test
    void rejectsNonInsertUnsupportedAndOracleExceedingRows() throws Exception {
        Program.ReadOracle oracle = new Program.ReadOracle(
                4, List.of("id"), 1, "0".repeat(64));

        Row update = Row.of(1L);
        update.setKind(RowKind.UPDATE_AFTER);
        assertThrows(EngineEffects.EffectFailure.class,
                () -> new CanonicalRead(new ObjectMapper(), oracle).add(update));
        assertThrows(EngineEffects.EffectFailure.class,
                () -> new CanonicalRead(new ObjectMapper(), oracle).add(Row.of(1)));
        assertThrows(EngineEffects.EffectFailure.class,
                () -> new CanonicalRead(new ObjectMapper(), oracle).add(Row.of(1L, 2L)));

        CanonicalRead tooMany = new CanonicalRead(new ObjectMapper(), oracle);
        tooMany.add(Row.of(1L));
        assertThrows(EngineEffects.EffectFailure.class, () -> tooMany.add(Row.of(2L)));
    }
}
