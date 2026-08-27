package org.querygraph.catalogbench.flink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

final class EventSink {
    static final String PREFIX = "CATALOG_BENCH_EVENT ";
    static final int MAX_EVENT_BYTES = 16 * 1024;

    private final ObjectMapper mapper;
    private final PrintStream output;

    EventSink(PrintStream output) {
        this.mapper = new ObjectMapper();
        this.output = output;
    }

    void emit(ChildEvent event) throws EventFailure {
        final String encoded;
        try {
            encoded = mapper.writeValueAsString(event);
        } catch (JsonProcessingException error) {
            throw new EventFailure(error);
        }
        if (encoded.getBytes(StandardCharsets.UTF_8).length > MAX_EVENT_BYTES) {
            throw new EventFailure();
        }
        output.println(PREFIX + encoded);
        output.flush();
        if (output.checkError()) {
            throw new EventFailure();
        }
    }

    static final class EventFailure extends Exception {
        private static final long serialVersionUID = 1L;

        EventFailure() {}

        EventFailure(Throwable cause) {
            super(cause);
        }
    }
}
