/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.importer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.eclipse.collections.api.factory.primitive.IntSets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.batchimport.api.DetailedProgressReport;
import org.neo4j.batchimport.api.DetailedProgressReportBase;
import org.neo4j.batchimport.api.UnsupportedFormatException;
import org.neo4j.batchimport.api.input.ApplicationMode;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.commandline.dbms.CannotWriteException;
import org.neo4j.configuration.Config;
import org.neo4j.importer.FileImporter.CsvImportException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.locker.FileLockException;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import picocli.CommandLine;
import picocli.CommandLine.ParameterException;

@TestDirectoryExtension
class ImportContextTest {

    private static final NormalizedDatabaseName DB = new NormalizedDatabaseName("foo");

    @Inject
    private TestDirectory testDir;

    @Inject
    private FileSystemAbstraction fs;

    private Config config;

    private Path importsDir;

    @BeforeEach
    void setup() {
        config = Config.defaults(neo4j_home, testDir.homePath());
        importsDir = config.get(logs_directory);
    }

    @Test
    void createContextDoesNotCreateDirectories() throws IOException {
        try (var importContext = ImportContext.create(fs, DB, config, null, false, true, true)) {
            assertThat(importContext.baseDir()).doesNotExist();
            assertThat(importContext.logPath()).doesNotExist();
            assertThat(importContext.progressReportingPath()).doesNotExist();

            try (var output = new ByteArrayOutputStream()) {
                importContext.preamble(new PrintStream(output));
                assertThat(output.toString())
                        .contains(
                                "Starting to import, the following output will be saved in the directory",
                                importContext.baseDir().toString(),
                                "Logging information:",
                                ImportContext.LOG_FILE_NAME,
                                "Detailed progress reporting (JSON formatted)",
                                ImportContext.PROGRESS_REPORTING_FILE_NAME)
                        .doesNotContain("NOTE this directory will be cleared on the completion of a successful import");
            }
        }

        assertThat(importsDir).doesNotExist();
    }

    @Test
    void contextClearedIfNotVerboseAndNotRetained() {
        try (var importContext = ImportContext.create(fs, DB, config, null, false, false, false)) {
            assertThat(importContext.baseDir()).doesNotExist();
            assertThat(importContext.logPath()).doesNotExist();
            assertThat(importContext.progressReportingPath()).doesNotExist();

            importContext.getLog("testing").info("some content");
            assertThat(importContext.baseDir()).exists();
            assertThat(importContext.logPath()).exists();
            assertThat(importContext.progressReportingPath()).doesNotExist();

            importContext.detailedProgressReport(progressReport());
            assertThat(importContext.progressReportingPath()).exists();
        }

        assertThat(importsDir).exists().isEmptyDirectory();
    }

    @ParameterizedTest
    @CsvSource(
            value = {
                "true,false,LOGGING",
                "false,true,LOGGING",
                "true,true,LOGGING",
                "true,false,REPORTING",
                "false,true,REPORTING",
                "true,true,REPORTING",
                "true,false,VIOLATION",
                "false,true,VIOLATION",
                "true,true,VIOLATION"
            })
    void contextNotClearedIfVerboseOrRetained(boolean retainForInstrumentation, boolean verbose, ContextAction action) {
        try (var importContext = ImportContext.create(fs, DB, config, null, false, retainForInstrumentation, verbose)) {
            switch (action) {
                case LOGGING -> importContext.getLog("testing").info("some content");
                case REPORTING -> importContext.detailedProgressReport(progressReport());
                case VIOLATION -> new PrintStream(importContext.collectorOutputStream()).println("bad tings");
            }
            assertThat(importContext.baseDir()).exists();
        }

        assertThat(importsDir)
                .exists()
                .isNotEmptyDirectory()
                .satisfies(dir -> assertThat(fs.listFiles(dir))
                        .hasSize(1)
                        .singleElement()
                        .satisfies(ImportContextTest::assertIsImportContextDir));
    }

    @ParameterizedTest
    @MethodSource
    void contextNotClearedOnLoggedErrors(Exception error, Class<? extends Exception> expectedErrorType)
            throws IOException {
        try (var importContext = ImportContext.create(fs, DB, config, null, false, false, false)) {
            try (var output = new ByteArrayOutputStream()) {
                importContext.preamble(new PrintStream(output));
                assertThat(output.toString())
                        .contains(
                                "Starting to import, the following output will be saved in the directory",
                                importContext.baseDir().toString(),
                                "Logging information:",
                                ImportContext.LOG_FILE_NAME,
                                "Detailed progress reporting (JSON formatted)",
                                ImportContext.PROGRESS_REPORTING_FILE_NAME,
                                "NOTE this directory will be cleared on the completion of a successful import");
            }

            importContext.detailedProgressReport(progressReport());
            var capturedError = importContext.captureError(error);
            assertThat(capturedError).isInstanceOf(expectedErrorType);
        }

        assertThat(importsDir)
                .exists()
                .isNotEmptyDirectory()
                .satisfies(dir -> assertThat(fs.listFiles(dir))
                        .hasSize(1)
                        .singleElement()
                        .satisfies(ImportContextTest::assertIsImportContextDir));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void contextNotClearedOnCollectorOutput(boolean addViolation) {
        try (var importContext = ImportContext.create(fs, DB, config, null, false, false, false)) {
            if (addViolation) {
                new PrintStream(importContext.collectorOutputStream()).println("bad tings");
                assertThat(importContext.baseDir()).exists();
            }
        }

        if (addViolation) {
            assertThat(importsDir)
                    .exists()
                    .isNotEmptyDirectory()
                    .satisfies(dir -> assertThat(fs.listFiles(dir))
                            .hasSize(1)
                            .singleElement()
                            .satisfies(ImportContextTest::assertIsImportContextDir));
        } else {
            assertThat(importsDir).doesNotExist();
        }
    }

    @Test
    void contextClearedWhenCollectorOutputOutside() {
        var reportFile = testDir.file("some.report");
        try (var importContext = ImportContext.create(fs, DB, config, reportFile, false, false, false)) {
            new PrintStream(importContext.collectorOutputStream()).println("bad tings");
            assertThat(importContext.baseDir()).doesNotExist();
        }

        assertThat(reportFile).exists().isNotEmptyFile();
        assertThat(importsDir).doesNotExist();
    }

    @Test
    void eachRunCreatesNewContext() {
        var content1 = "content1";
        var content2 = "content2";

        Path run1;
        try (var importContext = ImportContext.create(fs, DB, config, null, false, true, false)) {
            importContext.getLog("testing").info(content1);
            run1 = importContext.logPath();
        }

        Path run2;
        try (var importContext = ImportContext.create(fs, DB, config, null, false, true, false)) {
            importContext.getLog("testing").info(content2);
            run2 = importContext.logPath();
        }

        assertThat(run1).exists().content().contains(content1);
        assertThat(run2).exists().content().contains(content2);

        assertThat(importsDir)
                .exists()
                .isNotEmptyDirectory()
                .satisfies(dir -> assertThat(fs.listFiles(dir))
                        .hasSize(2)
                        .allSatisfy(ImportContextTest::assertIsImportContextDir));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void progressWithAndWithoutUpdates(boolean withUpdates) {
        try (var importContext = ImportContext.create(fs, DB, config, null, withUpdates, false, true)) {
            importContext.detailedProgressReport(progressReport());
        }

        assertThat(importsDir)
                .exists()
                .isNotEmptyDirectory()
                .satisfies(dir -> assertThat(fs.listFiles(dir))
                        .hasSize(1)
                        .singleElement()
                        .satisfies(contextDir -> {
                            assertIsImportContextDir(contextDir);

                            assertThat(contextDir.resolve(ImportContext.PROGRESS_REPORTING_FILE_NAME))
                                    .exists()
                                    .isNotEmptyFile()
                                    .content()
                                    .satisfies(content -> {
                                        if (withUpdates) {
                                            assertThat(content).contains("\"updated\"", "\"deleted\"");
                                        } else {
                                            assertThat(content).doesNotContain("\"updated\"", "\"deleted\"");
                                        }
                                    });
                        }));
    }

    private static void assertIsImportContextDir(Path importDir) {
        assertThat(importDir.getFileName().toString()).startsWith(DB.name() + "-admin-import-");
    }

    private static Stream<Arguments> contextNotClearedOnLoggedErrors() {
        var boom = "boom";
        var ioBoom = new IOException(boom);
        return Stream.of(
                Arguments.of(
                        new ParameterException(new CommandLine(new CommandLine.HelpCommand()), boom),
                        ParameterException.class),
                Arguments.of(new FileLockException(boom, ioBoom), CommandFailedException.class),
                Arguments.of(new CannotWriteException(Path.of(boom)), CommandFailedException.class),
                Arguments.of(new CsvImportException(boom, ioBoom), CommandFailedException.class),
                Arguments.of(new UnsupportedFormatException(boom), CommandFailedException.class),
                Arguments.of(new UncheckedIOException(ioBoom), CommandFailedException.class),
                Arguments.of(ioBoom, CommandFailedException.class));
    }

    private static DetailedProgressReport progressReport() {
        var reportBase = new DetailedProgressReportBase(42, 69, true);
        reportBase.registerNodeStats(ApplicationMode.CREATE, IntSets.immutable.of(1, 2));
        reportBase.registerNodeStats(ApplicationMode.CREATE, IntSets.immutable.of(3, 4));
        reportBase.registerNodeStats(ApplicationMode.UPDATE, IntSets.immutable.of(5));
        reportBase.registerNodeStats(ApplicationMode.DELETE, IntSets.immutable.of(6));
        reportBase.registerRelationshipStats(ApplicationMode.CREATE, 5);
        return reportBase.snapshot();
    }

    private enum ContextAction {
        LOGGING,
        REPORTING,
        VIOLATION
    }
}
