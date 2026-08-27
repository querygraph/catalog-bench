package org.querygraph.catalogbench.flink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

final class CanonicalRead {
    private final ObjectMapper mapper;
    private final MessageDigest digest;
    private final long maximumRows;
    private final long maximumBytes;
    private final int expectedArity;
    private long rows;
    private long bytes;

    CanonicalRead(ObjectMapper mapper, Program.ReadOracle oracle) throws EngineEffects.EffectFailure {
        this.mapper = mapper;
        this.maximumRows = oracle.rows();
        this.maximumBytes = oracle.bytes();
        this.expectedArity = oracle.columns().size();
        try {
            this.digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException failure) {
            throw new EngineEffects.EffectFailure(failure);
        }
    }

    void add(Row row) throws EngineEffects.EffectFailure {
        if (row.getKind() != RowKind.INSERT
                || row.getArity() != expectedArity
                || rows >= maximumRows) {
            throw new EngineEffects.EffectFailure();
        }
        List<Object> values = new ArrayList<>(row.getArity());
        for (int index = 0; index < row.getArity(); index++) {
            Object value = row.getField(index);
            if (value != null && !(value instanceof Long) && !(value instanceof String)) {
                throw new EngineEffects.EffectFailure();
            }
            if (value instanceof String text && text.length() > maximumBytes - bytes) {
                throw new EngineEffects.EffectFailure();
            }
            values.add(value);
        }
        final byte[] encoded;
        try {
            encoded = mapper.writeValueAsBytes(values);
        } catch (JsonProcessingException failure) {
            throw new EngineEffects.EffectFailure(failure);
        }
        long nextBytes;
        try {
            nextBytes = Math.addExact(bytes, Math.addExact(encoded.length, 1));
        } catch (ArithmeticException failure) {
            throw new EngineEffects.EffectFailure(failure);
        }
        if (nextBytes > maximumBytes) {
            throw new EngineEffects.EffectFailure();
        }
        digest.update(encoded);
        digest.update((byte) '\n');
        bytes = nextBytes;
        rows++;
    }

    ChildEvent.ReadObservation finish() {
        return new ChildEvent.ReadObservation(
                rows,
                bytes,
                HexFormat.of().formatHex(digest.digest()).toLowerCase(java.util.Locale.ROOT));
    }
}
