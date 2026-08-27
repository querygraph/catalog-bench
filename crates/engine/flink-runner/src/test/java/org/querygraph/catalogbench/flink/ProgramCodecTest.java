package org.querygraph.catalogbench.flink;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ProgramCodecTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void decodesTheClosedRustWireShape() throws Exception {
        Path programFile = temporaryDirectory.resolve("program.json");
        Files.writeString(programFile, validProgram());

        Program program = ProgramCodec.read(programFile);

        assertEquals(1, program.parallelism());
        assertEquals("bench", program.catalog().name());
        assertInstanceOf(Program.Anonymous.class, program.catalog().authentication());
        assertEquals("events", program.fixture().table());
        assertEquals(8, program.operations().size());
        Program.InitialRead initialRead =
                assertInstanceOf(Program.InitialRead.class, program.operations().get(3));
        assertEquals(16, initialRead.expected().rows());
        assertEquals(346, initialRead.expected().bytes());
    }

    @Test
    void rejectsUnknownDuplicateTrailingAndReorderedInput() {
        assertViolation(validProgram().replaceFirst(
                "\"parallelism\":1", "\"parallelism\":1,\"unknown\":true"));
        assertViolation(validProgram().replaceFirst(
                "\"parallelism\":1", "\"parallelism\":1,\"parallelism\":1"));
        assertViolation(validProgram() + "{}");
        assertViolation(validProgram().replace(
                "{\"operation\":\"create-namespace\",\"sql\":\"CREATE DATABASE IF NOT EXISTS `ns`\"}",
                "{\"operation\":\"create-table\",\"sql\":\"CREATE TABLE `ns`.`events` (`id` BIGINT)\"}"));
    }

    @Test
    void rejectsCredentialPropertiesUnsafeFixturesAndInvalidOracles() {
        assertViolation(validProgram().replace(
                "\"prefix\":\"bench\"",
                "\"prefix\":\"bench\",\"credential\":\"private\""));
        assertViolation(validProgram().replace(
                "s3://warehouse/ns/events",
                "s3://other/ns/events"));
        assertViolation(validProgram().replace(
                "http://catalog:8181/api/catalog",
                "http://user:private@catalog:8181/api/catalog"));
        assertViolation(validProgram().replace(
                "e78b526d7e757090a9a90c80802c2a543cbf8166cfac6d6ed48c618926e85a15",
                "not-a-digest"));
        assertViolation(validProgram().replace(
                "INSERT INTO `ns`.`events` VALUES (1)",
                "DELETE FROM `ns`.`events`"));
    }

    @Test
    void rejectsEmptyOversizedAndSymlinkedFiles() throws IOException {
        Path empty = temporaryDirectory.resolve("empty.json");
        Files.write(empty, new byte[0]);
        assertThrows(ProgramCodec.ProgramViolation.class, () -> ProgramCodec.read(empty));

        Path oversized = temporaryDirectory.resolve("oversized.json");
        byte[] bytes = new byte[ProgramCodec.MAX_PROGRAM_BYTES + 1];
        Arrays.fill(bytes, (byte) ' ');
        Files.write(oversized, bytes);
        assertThrows(ProgramCodec.ProgramViolation.class, () -> ProgramCodec.read(oversized));

        Path target = temporaryDirectory.resolve("target.json");
        Files.writeString(target, validProgram());
        Path link = temporaryDirectory.resolve("link.json");
        Files.createSymbolicLink(link, target);
        assertThrows(ProgramCodec.ProgramViolation.class, () -> ProgramCodec.read(link));
    }

    private static void assertViolation(String json) {
        assertThrows(
                ProgramCodec.ProgramViolation.class,
                () -> ProgramCodec.decode(json.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    }

    private static String validProgram() {
        return """
                {
                  "parallelism":1,
                  "catalog":{
                    "name":"bench",
                    "properties":{
                      "catalog-type":"rest",
                      "io-impl":"org.apache.iceberg.aws.s3.S3FileIO",
                      "prefix":"bench",
                      "s3.endpoint":"http://minio:9000",
                      "s3.path-style-access":"true",
                      "s3.region":"us-east-1",
                      "type":"iceberg",
                      "uri":"http://catalog:8181/api/catalog"
                    },
                    "authentication":{"kind":"anonymous"}
                  },
                  "fixture":{
                    "namespace":"ns",
                    "table":"events",
                    "requested_location":"s3://warehouse/ns/events",
                    "bucket":"warehouse"
                  },
                  "observation":{
                    "format_version":2,
                    "initial_schema":[
                      {"id":1,"name":"id","required":true,"type":"long"},
                      {"id":2,"name":"category","required":false,"type":"string"},
                      {"id":3,"name":"amount_cents","required":true,"type":"long"}
                    ],
                    "evolved_field":{"name":"note","required":false,"type":"string"},
                    "properties":{"catalog-bench.owner":"catalog-bench"}
                  },
                  "operations":[
                    {"operation":"create-namespace","sql":"CREATE DATABASE IF NOT EXISTS `ns`"},
                    {"operation":"create-table","sql":"CREATE TABLE `ns`.`events` (`id` BIGINT)"},
                    {"operation":"initial-append","sql":"INSERT INTO `ns`.`events` VALUES (1)"},
                    {
                      "operation":"initial-read",
                      "sql":"SELECT `id` FROM `ns`.`events` ORDER BY `id`",
                      "expected":{
                        "bytes":346,
                        "columns":["id","category","amount_cents"],
                        "rows":16,
                        "sha256":"e78b526d7e757090a9a90c80802c2a543cbf8166cfac6d6ed48c618926e85a15"
                      }
                    },
                    {"operation":"add-column","sql":"ALTER TABLE `ns`.`events` ADD `note` STRING"},
                    {"operation":"evolved-append","sql":"INSERT INTO `ns`.`events` VALUES (2)"},
                    {
                      "operation":"evolved-read",
                      "sql":"SELECT `id` FROM `ns`.`events` ORDER BY `id`",
                      "expected":{
                        "bytes":570,
                        "columns":["id","category","amount_cents","note"],
                        "rows":20,
                        "sha256":"b2af6f475851e07d1ace3706d8867530c13dd5938bee90cfcc62d3939e01bea2"
                      }
                    },
                    {"operation":"snapshot-read","sql":"SELECT * FROM `ns`.`events$snapshots`"}
                  ]
                }
                """;
    }
}
