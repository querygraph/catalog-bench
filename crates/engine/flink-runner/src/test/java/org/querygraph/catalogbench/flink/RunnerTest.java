package org.querygraph.catalogbench.flink;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class RunnerTest {
    @Test
    void acceptsOnlyTheClosedProgramArgument() {
        assertEquals(Path.of("/private/program.json"),
                Runner.programPath(new String[] {"--program", "/private/program.json"}));
        assertThrows(IllegalArgumentException.class, () -> Runner.programPath(new String[0]));
        assertThrows(IllegalArgumentException.class,
                () -> Runner.programPath(new String[] {"--other", "/private/program.json"}));
        assertThrows(IllegalArgumentException.class,
                () -> Runner.programPath(new String[] {
                    "--program", "/private/program.json", "extra"
                }));
    }
}
