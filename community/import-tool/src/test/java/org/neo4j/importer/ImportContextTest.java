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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.attribute.AclEntryPermission.APPEND_DATA;
import static java.nio.file.attribute.AclEntryPermission.WRITE_DATA;
import static java.nio.file.attribute.PosixFilePermission.GROUP_WRITE;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_WRITE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.PosixFileAttributeView;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
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
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.commandline.dbms.CannotWriteException;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.importer.FileImporter.CsvImportException;
import org.neo4j.internal.batchimport.input.BadCollector;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.ProblemReporters;
import org.neo4j.io.fs.DelegatingFileSystemAbstraction;
import org.neo4j.io.fs.DelegatingStoreChannel;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.fs.StoreChannel;
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
        try (var importContext =
                ImportContext.create(fs, DB, null, config, null, Collections.emptyList(), false, true, true)) {
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
        try (var importContext = getImportContext()) {
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
        try (var importContext = ImportContext.create(
                fs, DB, null, config, null, Collections.emptyList(), false, retainForInstrumentation, verbose)) {
            switch (action) {
                case LOGGING -> importContext.getLog("testing").info("some content");
                case REPORTING -> importContext.detailedProgressReport(progressReport());
                case VIOLATION -> writeViolation(importContext);
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
        try (var importContext = getImportContext()) {
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
        try (var importContext = getImportContext()) {
            if (addViolation) {
                writeViolation(importContext);
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
        try (var importContext = getImportContext(reportFile)) {
            writeViolation(importContext);
            assertThat(importContext.baseDir()).doesNotExist();
        }

        assertThat(reportFile).exists().isNotEmptyFile();
        assertThat(importsDir).doesNotExist();
    }

    @Test
    void collectorOutputTruncatedWhenReportFileReusedWithoutResume() {
        var reportFile = testDir.file("some.report");
        try (var importContext = getImportContext(reportFile)) {
            writeViolation(importContext, "first");
        }

        try (var importContext = getImportContext(reportFile)) {
            writeViolation(importContext, "second");
        }

        assertThat(reportFile).content().isEqualTo("second");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void collectorOutputAppendedOnResume(boolean reportFileOutsideContext) {
        var reportFile = reportFileOutsideContext ? testDir.file("some.report") : null;
        Path baseDir;
        try (var importContext = getRetainingImportContext(reportFile)) {
            writeViolation(importContext, "first");
            baseDir = importContext.baseDir();
        }

        try (var importContext = ImportContext.create(
                fs, DB, baseDir, config, reportFile, Collections.emptyList(), false, true, false)) {
            writeViolation(importContext, "second");
        }

        var reportPath = reportFile != null ? reportFile : baseDir.resolve(ImportContext.DEFAULT_REPORT_FILE_NAME);
        assertThat(reportPath).content().isEqualTo("firstsecond");
    }

    @Test
    void cliArgsClearedWithRestOfContextWhenNotRetained() {
        var args = List.of("--nodes=foo.csv");
        Path cliArgsPath;
        try (var importContext = getImportContext(args)) {
            importContext.persistCliArgs();
            cliArgsPath = importContext.baseDir().resolve(ImportContext.CLI_ARGS_FILE_NAME);
            assertThat(cliArgsPath).exists().content().isEqualTo(String.join("\n", args));
        }

        assertThat(cliArgsPath).doesNotExist();
        assertThat(importsDir).exists().isEmptyDirectory();
    }

    @Test
    void cliArgsWithSingleArgumentContainNoSeparator() {
        var args = List.of("--nodes=foo.csv");
        Path cliArgsPath;
        try (var importContext = getImportContext(args)) {
            importContext.persistCliArgs();
            cliArgsPath = importContext.baseDir().resolve(ImportContext.CLI_ARGS_FILE_NAME);
            assertThat(cliArgsPath).exists().content().isEqualTo("--nodes=foo.csv");
        }

        assertThat(cliArgsPath).doesNotExist();
    }

    @Test
    void cliArgsIsOverriddenOnResume() {
        var args = List.of("--nodes=foo.csv");
        Path baseDir;
        try (var importContext = getRetainingImportContext(args)) {
            importContext.persistCliArgs();
            baseDir = importContext.baseDir();
        }

        // resume
        var newArgs = List.of("--nodes=bar.csv", "--resume");
        try (var importContext = getResumingRetainingImportContext(baseDir, newArgs)) {
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
                                .isEqualTo(String.join("\n", newArgs))));
    }

    @Test
    void cliArgsRetainedWhenContextRetained() {
        var args = List.of("--nodes=foo.csv");
        try (var importContext = getRetainingImportContext(args)) {
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
                                .isEqualTo(String.join("\n", args))));
    }

    @Test
    void recordOfTheAttemptIsWriteProtected() throws IOException {
        try (var importContext = getRetainingImportContext(List.of("--nodes=foo.csv"))) {
            importContext.persistCliArgs();
            importContext.persistConfig();
            importContext.markSuccessful();

            for (var fileName : List.of(
                    ImportContext.CLI_ARGS_FILE_NAME,
                    ImportContext.CONFIG_FILE_NAME,
                    ImportContext.SUCCESS_FILE_NAME)) {
                Path recorded = importContext.baseDir().resolve(fileName);
                assertThat(recorded).isReadable();
                assertWriteProtected(recorded);
            }
        }
    }

    @Test
    void writeProtectedRecordOfARetainedAttemptCanStillBeDeleted() throws IOException {
        Path contextDir;
        try (var importContext = getRetainingImportContext(List.of("--nodes=foo.csv"))) {
            importContext.persistCliArgs();
            importContext.markSuccessful();
            contextDir = importContext.baseDir();
        }

        // a retained attempt outlives the import, so removing it is left to whoever no longer needs it - the write
        // protection guards the records against being edited, not against being thrown away
        FileUtils.deleteDirectory(contextDir);

        assertThat(contextDir).doesNotExist();
    }

    @Test
    void writeProtectedRecordStillGetsClearedWithTheRestOfTheContext() throws IOException {
        Path contextDir;
        try (var importContext = getImportContext(List.of("--nodes=foo.csv"))) {
            importContext.persistCliArgs();
            importContext.persistConfig();
            importContext.markSuccessful();
            contextDir = importContext.baseDir();
            assertWriteProtected(contextDir.resolve(ImportContext.CLI_ARGS_FILE_NAME));
            assertWriteProtected(contextDir.resolve(ImportContext.CONFIG_FILE_NAME));
        }

        assertThat(contextDir).doesNotExist();
        assertThat(importsDir).exists().isEmptyDirectory();
    }

    @Test
    void configPersistsResolvedValuesAndNotOnlyExplicitlySetOnes() {
        try (var importContext = getRetainingImportContext()) {
            importContext.persistConfig();

            assertThat(importContext.baseDir().resolve(ImportContext.CONFIG_FILE_NAME))
                    .exists()
                    .content()
                    .contains(
                            "%s=%s".formatted(neo4j_home.name(), testDir.homePath()),
                            "%s=%s".formatted(logs_directory.name(), importsDir));
        }
    }

    @Test
    void noSensitiveChangesWhenAttemptRanWithTheSameConfig() throws IOException {
        assertThat(ImportContext.resumeSensitiveChanges(persistedConfig(), config))
                .isEmpty();
    }

    @Test
    void noSensitiveChangesWhenAttemptRecordedNoConfigAtAll() throws IOException {
        Path contextDir = testDir.directory("attempt-from-before-config-was-recorded");

        assertThat(ImportContext.resumeSensitiveChanges(contextDir, config)).isEmpty();
    }

    @Test
    void aStateShapingSettingThatChangedIsReported() throws IOException {
        Path contextDir = persistedConfig();
        var directIo = Config.newBuilder()
                .fromConfig(config)
                .set(GraphDatabaseSettings.pagecache_direct_io, true)
                .build();

        assertThat(ImportContext.resumeSensitiveChanges(contextDir, directIo))
                .singleElement()
                .satisfies(change -> {
                    assertThat(change.setting()).isEqualTo(GraphDatabaseSettings.pagecache_direct_io);
                    assertThat(change.previous()).isEqualTo(false);
                    assertThat(change.current()).isEqualTo(true);
                });
    }

    @Test
    void aSettingOtherSettingsDeriveTheirDefaultFromIsReportedWithEverythingItMoved() throws IOException {
        Path contextDir = persistedConfig();
        var movedData = Config.newBuilder()
                .fromConfig(config)
                .set(GraphDatabaseSettings.data_directory, testDir.directory("elsewhere"))
                .build();

        // every directory that defaults to a location under the data one travels with it, so moving the data
        // directory is reported as the move of each of them too
        assertThat(ImportContext.resumeSensitiveChanges(contextDir, movedData))
                .extracting(change -> change.setting().name())
                .containsExactly(
                        GraphDatabaseInternalSettings.auth_store_directory.name(),
                        GraphDatabaseInternalSettings.databases_root_path.name(),
                        GraphDatabaseSettings.data_directory.name(),
                        GraphDatabaseSettings.database_dumps_root_path.name(),
                        GraphDatabaseSettings.script_root_path.name(),
                        GraphDatabaseSettings.transaction_logs_root_path.name());
    }

    @Test
    void aSettingTheStateOnDiskDoesNotDependOnIsIgnored() throws IOException {
        Path contextDir = persistedConfig();
        var reportingMoreOften = Config.newBuilder()
                .fromConfig(config)
                .set(GraphDatabaseInternalSettings.import_detailed_reporting_interval, Duration.ofSeconds(1))
                .build();

        assertThat(ImportContext.resumeSensitiveChanges(contextDir, reportingMoreOften))
                .isEmpty();
    }

    @Test
    void configClearedWithRestOfContextWhenNotRetained() {
        Path configPath;
        try (var importContext = getImportContext()) {
            importContext.persistConfig();
            configPath = importContext.baseDir().resolve(ImportContext.CONFIG_FILE_NAME);
            assertThat(configPath).exists().isNotEmptyFile();
        }

        assertThat(configPath).doesNotExist();
        assertThat(importsDir).exists().isEmptyDirectory();
    }

    @Test
    void nodesPerRangeClearedWithRestOfContextWhenNotRetained() {
        Path nodesPerRangePath;
        try (var importContext = getImportContext()) {
            importContext.persistNodesPerRange(42L);
            nodesPerRangePath = importContext.baseDir().resolve(ImportContext.NODES_PER_RANGE_FILE_NAME);
            assertThat(nodesPerRangePath).exists().content().isEqualTo("42");
        }

        assertThat(nodesPerRangePath).doesNotExist();
        assertThat(importsDir).exists().isEmptyDirectory();
    }

    @Test
    void nodesPerRangeRetainedWhenContextRetained() {
        try (var importContext = getRetainingImportContext()) {
            importContext.persistNodesPerRange(42L);
        }

        assertThat(importsDir)
                .exists()
                .isNotEmptyDirectory()
                .satisfies(dir -> assertThat(fs.listFiles(dir))
                        .hasSize(1)
                        .singleElement()
                        .satisfies(contextDir -> assertThat(contextDir.resolve(ImportContext.NODES_PER_RANGE_FILE_NAME))
                                .exists()
                                .content()
                                .isEqualTo("42")));
    }

    @Test
    void nodesPerRangeIsOverriddenOnResume() {
        Path baseDir;
        try (var importContext = getRetainingImportContext()) {
            importContext.persistNodesPerRange(42L);
            baseDir = importContext.baseDir();
        }

        // resume
        try (var importContext = getResumingRetainingImportContext(baseDir)) {
            importContext.persistNodesPerRange(43L);
        }

        assertThat(importsDir)
                .exists()
                .isNotEmptyDirectory()
                .satisfies(dir -> assertThat(fs.listFiles(dir))
                        .hasSize(1)
                        .singleElement()
                        .satisfies(contextDir -> assertThat(contextDir.resolve(ImportContext.NODES_PER_RANGE_FILE_NAME))
                                .exists()
                                .content()
                                .isEqualTo("43")));
    }

    @Test
    void eachRunCreatesNewContext() {
        var content1 = "content1";
        var content2 = "content2";

        Path run1;
        try (var importContext = getRetainingImportContext()) {
            importContext.getLog("testing").info(content1);
            run1 = importContext.logPath();
        }

        Path run2;
        try (var importContext = getRetainingImportContext()) {
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
        try (var importContext = getRetainingImportContext()) {
            assertThat(ImportContext.wasSuccessful(fs, importContext.baseDir())).isFalse();
            importContext.markSuccessful();
            markedContextDir = importContext.baseDir();
        }

        assertThat(ImportContext.wasSuccessful(fs, markedContextDir)).isTrue();
    }

    @Test
    void successMarkerClearedWithRestOfContextWhenNotRetained() {
        Path contextDir;
        try (var importContext = getImportContext()) {
            importContext.markSuccessful();
            contextDir = importContext.baseDir();
        }

        assertThat(contextDir).doesNotExist();
        assertThat(ImportContext.wasSuccessful(fs, contextDir)).isFalse();
    }

    @Test
    void mostRecentContextDirEmptyWhenNothingHasBeenImported() throws IOException {
        assertThat(importsDir).doesNotExist();

        assertThat(ImportContext.mostRecentContextDir(fs, importsDir, DB.name()))
                .isEmpty();
    }

    @Test
    void mostRecentContextDirEmptyWhenPreviousImportWasNotRetained() throws IOException {
        try (var importContext = getImportContext()) {
            importContext.persistCliArgs();
        }

        assertThat(importsDir).exists().isEmptyDirectory();
        assertThat(ImportContext.mostRecentContextDir(fs, importsDir, DB.name()))
                .isEmpty();
    }

    @Test
    void mostRecentContextDirIgnoresOtherDatabases() throws IOException {
        try (var importContext = ImportContext.create(
                fs,
                new NormalizedDatabaseName("bar"),
                null,
                config,
                null,
                Collections.emptyList(),
                false,
                true,
                false)) {
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
            try (var importContext = getRetainingImportContext(args)) {
                importContext.persistCliArgs();
                contextDirs.add(importContext.baseDir());
            }
        }

        assertThat(fs.listFiles(importsDir)).hasSize(attemptArgs.size());

        var latest = contextDirs.getLast();
        assertThat(ImportContext.mostRecentContextDir(fs, importsDir, DB.name()))
                .contains(latest);
        assertThat(ImportContext.readCliArgs(fs, latest)).contains(attemptArgs.getLast());
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
            try (var importContext = getRetainingImportContext()) {
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
        try (var importContext =
                ImportContext.create(fs, DB, null, config, null, Collections.emptyList(), withUpdates, false, true)) {
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
        try (var importContext = getImportContext()) {
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
        try (var importContext = getImportContext()) {
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

    @Test
    void detailedProgressReportIsAppendedToOnResume() {
        var report = new DetailedProgressReportBase(42, 69, true);
        Path baseDir;
        try (var importContext = getRetainingImportContext()) {
            baseDir = importContext.baseDir();

            report.registerNodeStats(ApplicationMode.CREATE, IntSets.immutable.of(1, 2));
            importContext.detailedProgressReport(report.snapshot());
        }

        // resume
        try (var importContext = getResumingRetainingImportContext(baseDir)) {
            report.registerNodeStats(ApplicationMode.CREATE, IntSets.immutable.of(1, 2));
            report.registerNodeStats(ApplicationMode.CREATE, IntSets.immutable.of(3, 4));
            importContext.detailedProgressReport(report.snapshot());
        }

        assertThat(importsDir)
                .exists()
                .isNotEmptyDirectory()
                .satisfies(logsDir -> assertThat(fs.listFiles(logsDir))
                        .hasSize(1)
                        .singleElement()
                        .satisfies(thisRunDir -> assertThat(fs.listFiles(thisRunDir))
                                .hasSize(1)
                                .singleElement()
                                .satisfies(reportPath -> {
                                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                                            Files.newInputStream(reportPath), StandardCharsets.UTF_8))) {
                                        ObjectMapper objectMapper =
                                                new ObjectMapper().disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
                                        var json1 = objectMapper.readTree(reader.readLine());
                                        assertThat(json1.get("nodeStats")
                                                        .get("created")
                                                        .asText())
                                                .isEqualTo("1");
                                        assertThat(json1.get("nodePerLabelStats")
                                                        .get("Label[3]"))
                                                .isNull();
                                        var json2 = objectMapper.readTree(reader.readLine());
                                        assertThat(json2.get("nodeStats")
                                                        .get("created")
                                                        .asText())
                                                .isEqualTo("3");
                                        assertThat(json2.get("nodePerLabelStats")
                                                        .get("Label[3]")
                                                        .get("created")
                                                        .asText())
                                                .isEqualTo("1");
                                    }
                                })));
    }

    @Test
    void logIsAppendedToOnResume() {
        Path baseDir;
        try (var importContext = getRetainingImportContext()) {
            baseDir = importContext.baseDir();

            importContext.getLog("foo").info("first run");
        }

        // resume
        try (var importContext = getResumingRetainingImportContext(baseDir)) {
            importContext.getLog("foo").info("second run");
        }

        assertThat(importsDir)
                .exists()
                .isNotEmptyDirectory()
                .satisfies(logsDir -> assertThat(fs.listFiles(logsDir))
                        .hasSize(1)
                        .singleElement()
                        .satisfies(thisRunDir -> assertThat(fs.listFiles(thisRunDir))
                                .hasSize(1)
                                .singleElement()
                                .satisfies(logPath -> {
                                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                                            Files.newInputStream(logPath), StandardCharsets.UTF_8))) {
                                        assertThat(reader.readLine()).contains("first run");
                                        assertThat(reader.readLine()).contains("second run");
                                    }
                                })));
    }

    @Test
    void collectorReportIsAppendedToOnResume() throws IOException {
        Groups groups = new Groups();
        Group group = groups.getOrCreate(null);
        Path baseDir;
        try (var importContext = getRetainingImportContext();
                Collector collector = BadCollector.create(
                        ProblemReporters.jsonOutputProblemHandler(importContext.collectorChannel()),
                        BadCollector.UNLIMITED_TOLERANCE,
                        BadCollector.COLLECT_ALL,
                        false)) {
            baseDir = importContext.baseDir();

            collector.collectDuplicateNode(0, 0, group, "source", 1L);
        }

        // resume
        try (var importContext = getResumingRetainingImportContext(baseDir);
                Collector collector = BadCollector.create(
                        ProblemReporters.jsonOutputProblemHandler(importContext.collectorChannel()),
                        BadCollector.UNLIMITED_TOLERANCE,
                        BadCollector.COLLECT_ALL,
                        false)) {
            collector.collectDuplicateNode(1, 1, group, "source", 2L);
        }

        assertThat(importsDir)
                .exists()
                .isNotEmptyDirectory()
                .satisfies(logsDir -> assertThat(fs.listFiles(logsDir))
                        .hasSize(1)
                        .singleElement()
                        .satisfies(thisRunDir -> assertThat(fs.listFiles(thisRunDir))
                                .hasSize(1)
                                .singleElement()
                                .satisfies(logPath -> {
                                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                                            Files.newInputStream(logPath), StandardCharsets.UTF_8))) {
                                        ObjectMapper objectMapper =
                                                new ObjectMapper().disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
                                        var json1 = objectMapper.readTree(reader.readLine());
                                        assertThat(json1.get("problem").asText())
                                                .isEqualTo("DuplicateNode");
                                        assertThat(json1.get("node").get("id").asText())
                                                .isEqualTo("0");
                                        var json2 = objectMapper.readTree(reader.readLine());
                                        assertThat(json2.get("problem").asText())
                                                .isEqualTo("DuplicateNode");
                                        assertThat(json2.get("node").get("id").asText())
                                                .isEqualTo("1");
                                    }
                                })));
    }

    @Test
    void cliArgsOfThePreviousAttemptSurviveAFailedRewriteOnResume() throws IOException {
        var args = List.of("--nodes=foo.csv");
        Path baseDir;
        try (var importContext = getRetainingImportContext(args)) {
            importContext.persistCliArgs();
            baseDir = importContext.baseDir();
        }
        // when the resumed attempt opens the record for writing but never gets its own arguments out
        var failing = new DelegatingFileSystemAbstraction(fs) {
            @Override
            public OutputStream openAsOutputStream(Path fileName, boolean append) throws IOException {
                OutputStream out = super.openAsOutputStream(fileName, append);
                if (!fileName.getFileName().toString().startsWith(ImportContext.CLI_ARGS_FILE_NAME)) {
                    return out;
                }
                return new FilterOutputStream(out) {
                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        throw new IOException("No space left on device");
                    }
                };
            }

            @Override
            public StoreChannel open(Path fileName, Set<OpenOption> options) throws IOException {
                StoreChannel out = super.open(fileName, options);
                if (!fileName.getFileName().toString().startsWith(ImportContext.CLI_ARGS_FILE_NAME)) {
                    return out;
                }
                return new DelegatingStoreChannel<>(out) {
                    @Override
                    public void writeAll(ByteBuffer src) throws IOException {
                        throw new IOException("No space left on device");
                    }
                };
            }
        };
        var resumedArgs = List.of("--nodes=bar.csv", "--resume");
        try (var importContext =
                ImportContext.create(failing, DB, baseDir, config, null, resumedArgs, false, true, false)) {
            assertThatExceptionOfType(UncheckedIOException.class).isThrownBy(importContext::persistCliArgs);
        }
        // then what the attempt being resumed was invoked with is still on record
        assertThat(ImportContext.readCliArgs(fs, baseDir)).contains(args);
    }

    /**
     * The context directory of an attempt that ran with the given configuration, as {@code --resume} would find it.
     */
    private Path persistedConfig() {
        try (var importContext = getRetainingImportContext()) {
            importContext.persistConfig();
            return importContext.baseDir();
        }
    }

    private ImportContext getImportContext() {
        return ImportContext.create(fs, DB, null, config, null, Collections.emptyList(), false, false, false);
    }

    private ImportContext getImportContext(Path reportFile) {
        return ImportContext.create(fs, DB, null, config, reportFile, Collections.emptyList(), false, false, false);
    }

    private ImportContext getImportContext(List<String> args) {
        return ImportContext.create(fs, DB, null, config, null, args, false, false, false);
    }

    private ImportContext getRetainingImportContext() {
        return ImportContext.create(fs, DB, null, config, null, Collections.emptyList(), false, true, false);
    }

    private ImportContext getRetainingImportContext(Path reportFile) {
        return ImportContext.create(fs, DB, null, config, reportFile, Collections.emptyList(), false, true, false);
    }

    private ImportContext getRetainingImportContext(List<String> args) {
        return ImportContext.create(fs, DB, null, config, null, args, false, true, false);
    }

    private ImportContext getResumingRetainingImportContext(Path baseDir) {
        return ImportContext.create(fs, DB, baseDir, config, null, Collections.emptyList(), false, true, false);
    }

    private ImportContext getResumingRetainingImportContext(Path baseDir, List<String> args) {
        return ImportContext.create(fs, DB, baseDir, config, null, args, false, true, false);
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

    /**
     * Asserts on what the permissions say rather than on {@link Files#isWritable(Path)}.
     * The latter reports whether this process may write. And superuser may, whatever the permissions are,
     * and CI runs some builds as root. Follows the two ways write protection is expressed, POSIX permissions where
     * there are any and an ACL entry denying the owner write access otherwise.
     */
    private static void assertWriteProtected(Path path) throws IOException {
        String as = "'%s' must not be editable by accident".formatted(path.getFileName());
        var posix = Files.getFileAttributeView(path, PosixFileAttributeView.class);
        if (posix != null) {
            assertThat(posix.readAttributes().permissions())
                    .as(as)
                    .doesNotContain(OWNER_WRITE, GROUP_WRITE, OTHERS_WRITE);
            return;
        }
        var acl = Files.getFileAttributeView(path, AclFileAttributeView.class);
        assertThat(acl)
                .as("write protection needs either POSIX permissions or an ACL")
                .isNotNull();
        var owner = acl.getOwner();
        assertThat(acl.getAcl())
                .as(as)
                .anyMatch(entry -> entry.type() == AclEntryType.DENY
                        && entry.principal().equals(owner)
                        && entry.permissions().containsAll(Set.of(WRITE_DATA, APPEND_DATA)));
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

    private static void writeViolation(ImportContext importContext) {
        writeViolation(importContext, "bad tings%n".formatted());
    }

    private static void writeViolation(ImportContext importContext, String content) {
        try {
            importContext.collectorChannel().writeAll(ByteBuffer.wrap(content.getBytes(UTF_8)));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
