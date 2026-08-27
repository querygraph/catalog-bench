package org.querygraph.catalogbench.flink;

import java.nio.file.Path;

public final class Runner {
    private Runner() {}

    public static void main(String[] arguments) {
        int exit = ProgramRunner.FAILURE_EXIT;
        try {
            Path programPath = programPath(arguments);
            Program program = ProgramCodec.read(programPath);
            exit = new ProgramRunner(
                            new FlinkEngineEffects(),
                            new EventSink(System.out))
                    .run(program);
        } catch (ProgramCodec.ProgramViolation | RuntimeException | LinkageError ignored) {
            // The parent classifies a child that cannot establish the protocol.
        }
        System.exit(exit);
    }

    static Path programPath(String[] arguments) {
        if (arguments.length != 2 || !"--program".equals(arguments[0])) {
            throw new IllegalArgumentException("invalid child arguments");
        }
        return Path.of(arguments[1]);
    }
}
