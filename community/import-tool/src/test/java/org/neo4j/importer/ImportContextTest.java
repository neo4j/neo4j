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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
        try (var importContext = ImportContext.create(fs, DB, config, null, List.of(), false, true, true)) {
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
        try (var importContext = ImportContext.create(fs, DB, config, null, List.of(), false, false, false)) {
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
        try (var importContext =
                ImportContext.create(fs, DB, config, null, List.of(), false, retainForInstrumentation, verbose)) {
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
        try (var importContext = ImportContext.create(fs, DB, config, null, List.of(), false, false, false)) {
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
        try (var importContext = ImportContext.create(fs, DB, config, null, List.of(), false, false, false)) {
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
        try (var importContext = ImportContext.create(fs, DB, config, reportFile, List.of(), false, false, false)) {
            new PrintStream(importContext.collectorOutputStream()).println("bad tings");
            assertThat(importContext.baseDir()).doesNotExist();
        }

        assertThat(reportFile).exists().isNotEmptyFile();
        assertThat(importsDir).doesNotExist();
    }

    @Test
    void cliArgsClearedWithRestOfContextWhenNotRetained() {
        var args = List.of("--nodes=foo.csv");
        Path cliArgsPath;
        try (var importContext = ImportContext.create(fs, DB, config, null, args, false, false, false)) {
            importContext.persistCliArgs();
            cliArgsPath = importContext.baseDir().resolve(ImportContext.CLI_ARGS_FILE_NAME);
            assertThat(cliArgsPath).exists().content().isEqualTo(String.join(" ", args));
        }

        assertThat(cliArgsPath).doesNotExist();
        assertThat(importsDir).exists().isEmptyDirectory();
    }

    @Test
    void cliArgsWithSingleArgumentContainNoSeparator() {
        var args = List.of("--nodes=foo.csv");
        Path cliArgsPath;
        try (var importContext = ImportContext.create(fs, DB, config, null, args, false, false, false)) {
            importContext.persistCliArgs();
            cliArgsPath = importContext.baseDir().resolve(ImportContext.CLI_ARGS_FILE_NAME);
            assertThat(cliArgsPath).exists().content().isEqualTo("--nodes=foo.csv");
        }

        assertThat(cliArgsPath).doesNotExist();
    }

    @Test
    void cliArgsRetainedWhenContextRetained() {
        var args = List.of("--nodes=foo.csv");
        try (var importContext = ImportContext.create(fs, DB, config, null, args, false, true, false)) {
            importContext.persistCliArgs();
        }

        assertThat(importsDir)
                .exists()
                .isNotEmptyDirectory()
                .satisfies(dir -> assertThat(fs.listFiles(dir))
                        .hasSize(1)
                        .singleElement()
                        .satisfies(contextDir -> assertThat(contextDir.resolve(ImportContext.CLI_ARGS_FILE_NAME))
                                .exists()
                                .content()
                                .isEqualTo(String.join(" ", args))));
    }

    @Test
    void recordOfTheAttemptIsWriteProtected() {
        try (var importContext =
                ImportContext.create(fs, DB, config, null, List.of("--nodes=foo.csv"), false, true, false)) {
            importContext.persistCliArgs();
            importContext.markSuccessful();

            for (var fileName : List.of(ImportContext.CLI_ARGS_FILE_NAME, ImportContext.SUCCESS_FILE_NAME)) {
                Path recorded = importContext.baseDir().resolve(fileName);
                assertThat(recorded).isReadable();
                assertThat(Files.isWritable(recorded))
                        .as("'%s' must not be editable by accident", fileName)
                        .isFalse();
            }
        }
    }

    @Test
    void writeProtectedRecordStillGetsClearedWithTheRestOfTheContext() {
        Path contextDir;
        try (var importContext =
                ImportContext.create(fs, DB, config, null, List.of("--nodes=foo.csv"), false, false, false)) {
            importContext.persistCliArgs();
            importContext.markSuccessful();
            contextDir = importContext.baseDir();
            assertThat(Files.isWritable(contextDir.resolve(ImportContext.CLI_ARGS_FILE_NAME)))
                    .isFalse();
        }

        assertThat(contextDir).doesNotExist();
        assertThat(importsDir).exists().isEmptyDirectory();
    }

    @Test
    void eachRunCreatesNewContext() {
        var content1 = "content1";
        var content2 = "content2";

        Path run1;
        try (var importContext = ImportContext.create(fs, DB, config, null, List.of(), false, true, false)) {
            importContext.getLog("testing").info(content1);
            run1 = importContext.logPath();
        }

        Path run2;
        try (var importContext = ImportContext.create(fs, DB, config, null, List.of(), false, true, false)) {
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

    @Test
    void successIsOnlyRecordedWhenMarked() {
        Path markedContextDir;
        try (var importContext = ImportContext.create(fs, DB, config, null, List.of(), false, true, false)) {
            assertThat(ImportContext.wasSuccessful(importContext.baseDir())).isFalse();
            importContext.markSuccessful();
            markedContextDir = importContext.baseDir();
        }

        assertThat(ImportContext.wasSuccessful(markedContextDir)).isTrue();
    }

    @Test
    void successMarkerClearedWithRestOfContextWhenNotRetained() {
        Path contextDir;
        try (var importContext = ImportContext.create(fs, DB, config, null, List.of(), false, false, false)) {
            importContext.markSuccessful();
            contextDir = importContext.baseDir();
        }

        assertThat(contextDir).doesNotExist();
        assertThat(ImportContext.wasSuccessful(contextDir)).isFalse();
    }

    @Test
    void mostRecentContextDirEmptyWhenNothingHasBeenImported() throws IOException {
        assertThat(importsDir).doesNotExist();

        assertThat(ImportContext.mostRecentContextDir(fs, importsDir, DB.name()))
                .isEmpty();
    }

    @Test
    void mostRecentContextDirEmptyWhenPreviousImportWasNotRetained() throws IOException {
        try (var importContext = ImportContext.create(fs, DB, config, null, List.of(), false, false, false)) {
            importContext.persistCliArgs();
        }

        assertThat(importsDir).exists().isEmptyDirectory();
        assertThat(ImportContext.mostRecentContextDir(fs, importsDir, DB.name()))
                .isEmpty();
    }

    @Test
    void mostRecentContextDirIgnoresOtherDatabases() throws IOException {
        try (var importContext = ImportContext.create(
                fs, new NormalizedDatabaseName("bar"), config, null, List.of(), false, true, false)) {
            importContext.persistCliArgs();
        }

        assertThat(importsDir).isNotEmptyDirectory();
        assertThat(ImportContext.mostRecentContextDir(fs, importsDir, DB.name()))
                .isEmpty();
    }

    @Test
    void mostRecentContextDirFindsLatestOfSeveralRetainedAttempts() throws IOException {
        var attemptArgs =
                List.of(List.of("--nodes=first.csv"), List.of("--nodes=second.csv"), List.of("--nodes=third.csv"));
        var contextDirs = new ArrayList<Path>();
        for (var args : attemptArgs) {
            try (var importContext = ImportContext.create(fs, DB, config, null, args, false, true, false)) {
                importContext.persistCliArgs();
                contextDirs.add(importContext.baseDir());
            }
        }

        assertThat(fs.listFiles(importsDir)).hasSize(attemptArgs.size());

        var latest = contextDirs.getLast();
        assertThat(ImportContext.mostRecentContextDir(fs, importsDir, DB.name()))
                .contains(latest);
        assertThat(ImportContext.readCliArgs(latest)).contains(attemptArgs.getLast());
    }

    @Test
    void mostRecentContextDirOrdersAttemptsFromTheSameSecondByPaddedCounter() throws IOException {
        var sameSecond = DB.name() + "-admin-import-2026-01-01.12.00.00";
        fs.mkdirs(importsDir.resolve(sameSecond));
        for (int counter = 2; counter <= 12; counter++) {
            fs.mkdirs(importsDir.resolve(sameSecond + ".%03d".formatted(counter)));
        }

        assertThat(ImportContext.mostRecentContextDir(fs, importsDir, DB.name()))
                .contains(importsDir.resolve(sameSecond + ".012"));
    }

    @Test
    void attemptsFromTheSameSecondGetAZeroPaddedCounter() {
        var names = new ArrayList<String>();
        for (int attempt = 0; attempt < 3; attempt++) {
            try (var importContext = ImportContext.create(fs, DB, config, null, List.of(), false, true, false)) {
                names.add(importContext.baseDir().getFileName().toString());
            }
        }

        assertThat(names)
                .allMatch(
                        name -> name.matches(DB.name() + "-admin-import-\\d{4}(-\\d{2}){2}(\\.\\d{2}){3}(\\.\\d{3})?"))
                .isSorted();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void progressWithAndWithoutUpdates(boolean withUpdates) {
        try (var importContext = ImportContext.create(fs, DB, config, null, List.of(), withUpdates, false, true)) {
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

    @Test
    void detailedProgressReportStandardIncludesAllFieldsInJson() {
        try (var importContext =
                ImportContext.create(fs, DB, config, null, Collections.emptyList(), false, false, false)) {
            importContext.detailedProgressReport(progressReport());

            assertThat(importsDir)
                    .exists()
                    .isNotEmptyDirectory()
                    .satisfies(logsDir -> assertThat(fs.listFiles(logsDir))
                            .hasSize(1)
                            .singleElement()
                            .satisfies(thisRunDir -> assertThat(fs.listFiles(thisRunDir))
                                    .hasSize(1)
                                    .singleElement()
                                    .satisfies(report -> {
                                        ObjectMapper objectMapper = new ObjectMapper();
                                        var json = objectMapper.readTree(report.toFile());
                                        assertThat(json.get("estimatedTotalNumberOfNodes"))
                                                .isNotNull();
                                        assertThat(json.get("estimatedTotalNumberOfRelationships"))
                                                .isNotNull();
                                        assertThat(json.get("nodeStats")).isNotNull();
                                        assertThat(json.get("relationshipStats"))
                                                .isNotNull();
                                        assertThat(json.get("nodePerLabelStats"))
                                                .isNotNull();
                                        assertThat(json.get("relationshipPerTypeStats"))
                                                .isNotNull();
                                        assertThat(json.get("nodeIndexStats")).isNotNull();
                                        assertThat(json.get("nodeConstraintStats"))
                                                .isNotNull();
                                        assertThat(json.get("nodeIndexPerLabelStats"))
                                                .isNotNull();
                                        assertThat(json.get("nodeConstraintPerLabelStats"))
                                                .isNotNull();
                                        assertThat(json.get("relationshipIndexStats"))
                                                .isNotNull();
                                        assertThat(json.get("relationshipConstraintStats"))
                                                .isNotNull();
                                        assertThat(json.get("relationshipIndexPerTypeStats"))
                                                .isNotNull();
                                        assertThat(json.get("relationshipConstraintPerTypeStats"))
                                                .isNotNull();
                                        assertThat(json.get("nodeImportDuration"))
                                                .isNotNull();
                                        assertThat(json.get("relationshipImportDuration"))
                                                .isNotNull();
                                        assertThat(json.get("schemaImportDuration"))
                                                .isNotNull();
                                    })));
        }
    }

    @Test
    void detailedProgressReportSkidbladnirIncludesAllFieldsInJson() {
        try (var importContext =
                ImportContext.create(fs, DB, config, null, Collections.emptyList(), false, false, false)) {
            var reportBase = new DetailedProgressReportBase(42, 69, true);
            reportBase.registerNodeStats(ApplicationMode.CREATE, IntSets.immutable.of(1, 2));
            reportBase.registerRelationshipStats(ApplicationMode.CREATE, 5);
            reportBase.nodeIdMappingTimer().start();
            reportBase.nodeIdMappingTimer().end();
            reportBase.relationshipRangeDivisionTimer().start();
            reportBase.relationshipRangeDivisionTimer().end();
            reportBase.storeApplyingTimer().start();
            reportBase.storeApplyingTimer().end();
            reportBase.schemaTimer().start();
            reportBase.schemaTimer().end();
            importContext.detailedProgressReport(reportBase.skidbladnirSnapshot());

            assertThat(importsDir)
                    .exists()
                    .isNotEmptyDirectory()
                    .satisfies(logsDir -> assertThat(fs.listFiles(logsDir))
                            .hasSize(1)
                            .singleElement()
                            .satisfies(thisRunDir -> assertThat(fs.listFiles(thisRunDir))
                                    .hasSize(1)
                                    .singleElement()
                                    .satisfies(report -> {
                                        ObjectMapper objectMapper = new ObjectMapper();
                                        var json = objectMapper.readTree(report.toFile());
                                        assertThat(json.get("estimatedTotalNumberOfNodes"))
                                                .isNotNull();
                                        assertThat(json.get("estimatedTotalNumberOfRelationships"))
                                                .isNotNull();
                                        assertThat(json.get("nodeStats")).isNotNull();
                                        assertThat(json.get("relationshipStats"))
                                                .isNotNull();
                                        assertThat(json.get("nodePerLabelStats"))
                                                .isNotNull();
                                        assertThat(json.get("relationshipPerTypeStats"))
                                                .isNotNull();
                                        assertThat(json.get("nodeIndexStats")).isNotNull();
                                        assertThat(json.get("nodeConstraintStats"))
                                                .isNotNull();
                                        assertThat(json.get("nodeIndexPerLabelStats"))
                                                .isNotNull();
                                        assertThat(json.get("nodeConstraintPerLabelStats"))
                                                .isNotNull();
                                        assertThat(json.get("relationshipIndexStats"))
                                                .isNotNull();
                                        assertThat(json.get("relationshipConstraintStats"))
                                                .isNotNull();
                                        assertThat(json.get("relationshipIndexPerTypeStats"))
                                                .isNotNull();
                                        assertThat(json.get("relationshipConstraintPerTypeStats"))
                                                .isNotNull();
                                        assertThat(json.get("nodeIdMappingDuration"))
                                                .isNotNull();
                                        assertThat(json.get("relationshipRangeDivisionDuration"))
                                                .isNotNull();
                                        assertThat(json.get("storeApplyingDuration"))
                                                .isNotNull();
                                        assertThat(json.get("schemaImportDuration"))
                                                .isNotNull();
                                    })));
        }
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
