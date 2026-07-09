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
package org.neo4j.commandline.dbms;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_TX_LOGS_ROOT_DIR_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.data_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.initial_default_database;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TX_SEQUENCE_NUMBER;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.mockito.ArgumentCaptor;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.cloud.storage.SchemeFileSystemAbstraction;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.ConfigUtils;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.archive.Dumper;
import org.neo4j.dbms.archive.Dumper.DumpOutput;
import org.neo4j.dbms.archive.Dumper.FileOutput;
import org.neo4j.dbms.archive.Dumper.StdoutOutput;
import org.neo4j.dbms.archive.Manifest;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.locker.Locker;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.locker.DatabaseLocker;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DisabledForRoot;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.SkipOnSpd;
import org.neo4j.test.utils.TestDirectory;
import picocli.CommandLine;

@Neo4jLayoutExtension
@SkipOnSpd(reason = "Plain dump/load tests do not work for SPD")
class DumpCommandIT {
    @Inject
    private TestDirectory testDirectory;

    @Inject
    FileSystemAbstraction fs;

    @Inject
    private Neo4jLayout neo4jLayout;

    private DatabaseLayout databaseLayout;

    private Path homeDir;
    private Path configDir;
    private Path dumpDir;
    private Path archive;
    private Dumper dumper;
    private Path databaseDirectory;

    @BeforeEach
    void setUp() {
        homeDir = testDirectory.homePath();
        configDir = testDirectory.directory("config-dir");
        dumpDir = testDirectory.directory("dump-dir");
        archive = dumpDir.resolve("foo.dump");
        dumper = mock(Dumper.class);
        databaseDirectory = neo4jLayout.databaseLayout("foo").databaseDirectory();
        putStoreInDirectory(buildConfig(), databaseDirectory);
    }

    private Config buildConfig() {
        Config config = Config.newBuilder()
                .fromFileNoThrow(configDir.resolve(Config.DEFAULT_CONFIG_FILE_NAME))
                .set(GraphDatabaseSettings.neo4j_home, homeDir.toAbsolutePath())
                .build();
        ConfigUtils.disableAllConnectors(config);
        return config;
    }

    @Test
    void shouldDumpTheDatabaseToTheArchive() throws Exception {
        execute("foo");
        var outputCaptor = ArgumentCaptor.forClass(DumpOutput.class);
        verify(dumper).dump(outputCaptor.capture(), any(), any());
        assertThat(outputCaptor.getValue()).isInstanceOfSatisfying(FileOutput.class, output -> {
            assertThat(output.path()).isEqualTo(archive);
            assertThat(output.fs()).isInstanceOf(SchemeFileSystemAbstraction.class);
        });
    }

    @Test
    void shouldCalculateTheDatabaseDirectoryFromConfig() throws Exception {
        Path dataDir = testDirectory.directory("some-other-path");
        Path txLogsDir = dataDir.resolve(DEFAULT_TX_LOGS_ROOT_DIR_NAME + "/foo");
        Path databaseDir = dataDir.resolve("databases/foo");
        Files.write(
                configDir.resolve(Config.DEFAULT_CONFIG_FILE_NAME),
                singletonList(formatProperty(data_directory, dataDir)));
        putStoreInDirectory(buildConfig(), databaseDir);

        execute("foo");
        var mf = Dumper.collectManifest(databaseDir, txLogsDir, DumpCommandIT::excludeDatabaseLock);
        verify(dumper).dump(any(), any(), eq(mf));
    }

    @Test
    void shouldCalculateTheTxLogDirectoryFromConfig() throws Exception {
        Path dataDir = testDirectory.directory("some-other-path");
        Path txlogsRoot = testDirectory.directory("txLogsPath");
        Path databaseDir = dataDir.resolve("databases/foo");
        Files.write(
                configDir.resolve(Config.DEFAULT_CONFIG_FILE_NAME),
                asList(
                        formatProperty(data_directory, dataDir),
                        formatProperty(transaction_logs_root_path, txlogsRoot)));
        putStoreInDirectory(buildConfig(), databaseDir);

        execute("foo");
        var mf = Dumper.collectManifest(databaseDir, txlogsRoot.resolve("foo"), DumpCommandIT::excludeDatabaseLock);
        verify(dumper).dump(any(), any(), eq(mf));
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void shouldHandleDatabaseSymlink() throws Exception {
        Path realDatabaseDir = testDirectory.directory("path-to-links/foo");

        Path dataDir = testDirectory.directory("some-other-path");
        Path databaseDir = dataDir.resolve("databases/foo");
        Path txLogsDir = dataDir.resolve(DEFAULT_TX_LOGS_ROOT_DIR_NAME + "/foo");

        Files.createDirectories(dataDir.resolve("databases"));

        Files.createSymbolicLink(databaseDir, realDatabaseDir);
        Files.write(
                configDir.resolve(Config.DEFAULT_CONFIG_FILE_NAME),
                singletonList(format(
                        "%s=%s", data_directory.name(), dataDir.toString().replace('\\', '/'))));
        putStoreInDirectory(buildConfig(), realDatabaseDir);

        execute("foo");
        var mf = Dumper.collectManifest(realDatabaseDir, txLogsDir, DumpCommandIT::excludeDatabaseLock);
        verify(dumper).dump(any(), any(), eq(mf));
    }

    @Test
    void shouldCalculateTheArchiveNameIfPassedAnExistingDirectory() throws Exception {
        Path to = testDirectory.directory("some-dir");
        execute("foo", to);
        var outputCaptor = ArgumentCaptor.forClass(DumpOutput.class);
        verify(dumper).dump(outputCaptor.capture(), any(), any());
        assertThat(outputCaptor.getValue()).isInstanceOfSatisfying(FileOutput.class, output -> {
            assertThat(output.path()).isEqualTo(to.resolve("foo.dump"));
        });
    }

    @Test
    void shouldNotCalculateTheArchiveNameIfPassedAnExistingFile() throws Exception {
        Files.createFile(archive);
        execute("foo");
        var outputCaptor = ArgumentCaptor.forClass(DumpOutput.class);
        verify(dumper).dump(outputCaptor.capture(), any(), any());
        assertThat(outputCaptor.getValue()).isInstanceOfSatisfying(FileOutput.class, output -> {
            assertThat(output.path()).isEqualTo(archive);
        });
    }

    @Test
    void shouldRespectTheDatabaseLock() throws Exception {
        Path databaseDirectory = homeDir.resolve("data/databases/foo");
        DatabaseLayout databaseLayout = DatabaseLayout.ofFlat(databaseDirectory);
        try (FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
                Locker locker = new DatabaseLocker(fileSystem, databaseLayout)) {
            locker.checkLock();

            assertThatThrownBy(() -> execute("foo"))
                    .isInstanceOf(CommandFailedException.class)
                    .hasMessageContaining("Dump failed for databases: 'foo'")
                    .hasMessageContaining("The database is in use. Stop database 'foo' and try again.");
        }
    }

    @Test
    void databaseThatRequireRecoveryIsNotDumpable() throws IOException {
        LogFiles logFiles = LogFilesBuilder.writeableBuilder(
                        databaseLayout,
                        testDirectory.getFileSystem(),
                        LatestVersions.LATEST_KERNEL_VERSION_PROVIDER,
                        LatestVersions.LATEST_LOG_FORMAT_PROVIDER)
                .withStoreId(new StoreId(1, 1, "engine-1", "format-1", 1, 1))
                .build();
        try (Lifespan ignored = new Lifespan(logFiles)) {
            LogFile logFile = logFiles.getLogFile();
            LogEntryWriter<?> writer = logFile.getTransactionLogWriter().getWriter();
            writer.writeStartEntry(
                    LatestVersions.LATEST_KERNEL_VERSION,
                    0x123456789ABCDEFL,
                    4,
                    4,
                    UNKNOWN_TX_SEQUENCE_NUMBER,
                    BASE_TX_CHECKSUM,
                    new byte[] {0});
            // Required to push envelopes to stream
            writer.getChannel().putChecksum();
        }
        assertThatThrownBy(() -> execute("foo"))
                .isInstanceOf(CommandFailedException.class)
                .hasMessageContaining("Dump failed for databases: 'foo'")
                .hasMessageContaining("Active logical log detected, this might be a source of inconsistencies.");
    }

    @Test
    void shouldReleaseTheDatabaseLockAfterDumping() throws Exception {
        execute("foo");
        assertCanLockDatabase(databaseDirectory);
    }

    @Test
    void shouldReleaseTheDatabaseLockEvenIfThereIsAnError() throws Exception {
        Dumper dumper1 = doThrow(IOException.class).when(dumper);
        dumper1.dump(any(), any(), any());
        assertThatThrownBy(() -> execute("foo")).isInstanceOf(CommandFailedException.class);
        assertCanLockDatabase(databaseDirectory);
    }

    @Test
    void shouldNotAccidentallyCreateTheDatabaseDirectoryAsASideEffectOfDatabaseLocking() throws Exception {
        Path databaseDirectory = homeDir.resolve("data/databases/accident");

        doAnswer(ignored -> {
                    assertThat(Files.exists(databaseDirectory)).isEqualTo(false);
                    return null;
                })
                .when(dumper)
                .dump(any(), any(), any());

        execute("foo");
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    @DisabledForRoot
    void shouldReportAHelpfulErrorIfWeDontHaveWritePermissionsForLock() throws Exception {
        DatabaseLayout databaseLayout = DatabaseLayout.ofFlat(databaseDirectory);
        Path file = databaseLayout.databaseLockFile();
        try (Closeable ignored = withPermissions(file, emptySet())) {
            assertThatThrownBy(() -> execute("foo"))
                    .isInstanceOf(CommandFailedException.class)
                    .hasMessageContaining("Dump failed for databases: 'foo'")
                    .hasMessageContaining("You do not have permission to dump the database.");
        }
    }

    @Test
    void shouldExcludeTheStoreLockFromTheArchiveToAvoidProblemsWithReadingLockedFilesOnWindows() throws Exception {
        Path lockFile = DatabaseLayout.ofFlat(Path.of(".")).databaseLockFile();
        doAnswer(invocation -> {
                    Manifest mf = invocation.getArgument(2);
                    assertThat(mf.files())
                            .noneMatch(record -> record instanceof Manifest.FileRecord fr
                                    && fr.source().getFileName().equals(lockFile));
                    assertThat(mf.files()).isNotEmpty();
                    return null;
                })
                .when(dumper)
                .dump(any(), any(), any());

        execute("foo");
    }

    @Test
    void shouldDefaultToGraphDB() throws Exception {
        Path dataDir = testDirectory.directory("some-other-path");
        Path txLogsDir = dataDir.resolve(DEFAULT_TX_LOGS_ROOT_DIR_NAME + "/" + DEFAULT_DATABASE_NAME);
        Path databaseDir = dataDir.resolve("databases/" + DEFAULT_DATABASE_NAME);
        Files.write(
                configDir.resolve(Config.DEFAULT_CONFIG_FILE_NAME),
                singletonList(formatProperty(data_directory, dataDir)));
        putStoreInDirectory(buildConfig(), databaseDir);

        execute(DEFAULT_DATABASE_NAME);
        var mf = Dumper.collectManifest(databaseDir, txLogsDir, DumpCommandIT::excludeDatabaseLock);
        verify(dumper).dump(any(), any(), eq(mf));
    }

    @Test
    void shouldGiveAClearErrorIfTheArchiveAlreadyExists() throws Exception {
        doThrow(new FileAlreadyExistsException("the-archive-path")).when(dumper).dump(any(), any(), any());
        assertThatThrownBy(() -> execute("foo"))
                .isInstanceOf(CommandFailedException.class)
                .hasMessageContaining("Dump failed for databases: 'foo'")
                .hasMessageContaining("Archive already exists: the-archive-path");
    }

    @Test
    void shouldGiveAClearMessageIfTheDatabaseDoesntExist() {
        assertThatThrownBy(() -> execute("bobo"))
                .isInstanceOf(CommandFailedException.class)
                .hasMessageContaining("Dump failed for databases: 'bobo'")
                .hasMessageContaining("Database does not exist: bobo");
    }

    @Test
    void shouldGiveAClearMessageIfTheArchivesParentDoesntExist() throws Exception {
        doThrow(new NoSuchFileException(archive.getParent().toString()))
                .when(dumper)
                .dump(any(), any(), any());
        assertThatThrownBy(() -> execute("foo"))
                .isInstanceOf(CommandFailedException.class)
                .hasMessageContaining("Dump failed for databases: 'foo'")
                .hasMessageContaining("NoSuchFileException: " + archive.getParent());
    }

    @Test
    void shouldWrapIOExceptionsCarefullyBecauseCriticalInformationIsOftenEncodedInTheirNameButMissingFromTheirMessage()
            throws Exception {
        doThrow(new IOException("the-message")).when(dumper).dump(any(), any(), any());
        assertThatThrownBy(() -> execute("foo"))
                .isInstanceOf(CommandFailedException.class)
                .hasMessageContaining("Dump failed for databases: 'foo'")
                .hasMessageContaining("IOException: the-message");
    }

    @Test
    void shouldDumpTheDatabaseToTheStdOut() throws Exception {
        var ctx = new ExecutionContext(
                homeDir, configDir, mock(PrintStream.class), mock(PrintStream.class), testDirectory.getFileSystem());
        var command = new DumpCommand(ctx) {
            @Override
            protected Dumper createDumper(FileSystemAbstraction fs, PrintStream out) {
                return dumper;
            }
        };
        CommandLine.populateCommand(command, "--to-stdout", "foo");
        command.execute();

        var mf = Dumper.collectManifest(
                homeDir.resolve("data/databases/foo"),
                homeDir.resolve("data/transactions/foo"),
                DumpCommandIT::excludeDatabaseLock);
        verify(dumper).dump(eq(new StdoutOutput(ctx)), any(), eq(mf));
        verifyNoMoreInteractions(dumper);
    }

    @Test
    void shouldDumpTheDatabaseToTheStdOutEvenWithVerboseEnabled() throws Exception {
        final var databaseName = "foo";
        final var out = new Output();
        final var err = new Output();
        final var ctx = new ExecutionContext(
                homeDir, configDir, out.printStream, err.printStream, testDirectory.getFileSystem());
        final var command = new DumpCommand(ctx) {
            @Override
            protected Dumper createDumper(FileSystemAbstraction fs, PrintStream out) {
                return dumper;
            }
        };
        CommandLine.populateCommand(command, "--to-stdout", "--verbose", databaseName);
        command.execute();

        var mf = Dumper.collectManifest(
                homeDir.resolve("data/databases/foo"),
                homeDir.resolve("data/transactions/foo"),
                DumpCommandIT::excludeDatabaseLock);
        verify(dumper).dump(eq(new StdoutOutput(ctx)), any(), eq(mf));
        verifyNoMoreInteractions(dumper);

        assertThat(out.toString()).doesNotContain("Starting dump of database", databaseName);
        assertThat(err.toString()).contains("Starting dump of database", databaseName);
    }

    @Test
    void shouldNotAllowDatabaseNameGlobbingWithStdOut() {
        var ctx = new ExecutionContext(
                homeDir, configDir, mock(PrintStream.class), mock(PrintStream.class), testDirectory.getFileSystem());
        var command = new DumpCommand(ctx) {
            @Override
            protected Dumper createDumper(FileSystemAbstraction fs, PrintStream out) {
                return dumper;
            }
        };
        CommandLine.populateCommand(command, "foo*", "--to-stdout");
        assertThatThrownBy(command::execute)
                .isInstanceOf(CommandFailedException.class)
                .hasMessageContaining("Globbing in database name can not be used in combination with standard output. "
                        + "Specify a directory as destination or a single target database");
    }

    @Test
    void shouldNotAllowSpecifiedFile() throws IOException {
        Files.createFile(archive);
        assertThatThrownBy(() -> execute("foo*", archive))
                .isInstanceOf(CommandFailedException.class)
                .hasMessageContaining(archive + " is not an existing directory");
    }

    @Test
    void shouldUseDumpCommandConfigIfAvailable() throws Exception {
        // Checking that the command is unhappy about an invalid value is enough to verify
        // that the command-specific config is being taken into account.
        Files.writeString(
                configDir.resolve("neo4j-admin-database-dump.conf"), pagecache_memory.name() + "=some nonsense");

        assertThatThrownBy(() -> execute("foo"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'some nonsense' is not a valid size");
    }

    @Test
    void shouldNotSelectSplitBackupsByDefault() throws IOException {
        execute("foo", dumpDir);
        var outputCaptor = ArgumentCaptor.forClass(Dumper.DumpOutput.class);
        verify(dumper).dump(outputCaptor.capture(), any(), any());
        assertThat(outputCaptor.getValue()).isInstanceOfSatisfying(Dumper.FileOutput.class, output -> {
            assertThat(output.fs()).isInstanceOf(SchemeFileSystemAbstraction.class);
            assertThat(output.path()).isEqualTo(dumpDir.resolve("foo.dump"));
        });
    }

    @Test
    void shouldSelectSplitBackupsWhenRequestedFromCli() throws IOException {
        execute("foo", dumpDir, "--experimental-split-size=5gb");
        var outputCaptor = ArgumentCaptor.forClass(Dumper.DumpOutput.class);
        verify(dumper).dump(outputCaptor.capture(), any(), any());
        assertThat(outputCaptor.getValue()).isInstanceOfSatisfying(Dumper.SplitFileOutput.class, output -> {
            assertThat(output.fs()).isInstanceOf(SchemeFileSystemAbstraction.class);
            assertThat(output.baseArtifact()).isEqualTo(dumpDir.resolve("foo.dump"));
            assertThat(output.maxArtifactSize()).isEqualTo(ByteUnit.gibiBytes(5));
        });
    }

    @Test
    void shouldNotAllowSplitBackupsTooSmall() {
        assertThatCode(() -> execute("foo", dumpDir, "--experimental-split-size=5kb"))
                .isInstanceOf(CommandFailedException.class)
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("Can't split archive in sizes smaller than 1.000GiB");
    }

    @Test
    void shouldOverrideSplitBackupSizeWhenRequestedFromConfig() throws IOException {
        Files.write(
                configDir.resolve(Config.DEFAULT_CONFIG_FILE_NAME),
                List.of(GraphDatabaseInternalSettings.split_archive_file_size.name() + "=5gb"));
        putStoreInDirectory(buildConfig(), databaseDirectory);
        execute("foo", dumpDir);
        var outputCaptor = ArgumentCaptor.forClass(Dumper.DumpOutput.class);
        verify(dumper).dump(outputCaptor.capture(), any(), any());
        assertThat(outputCaptor.getValue()).isInstanceOfSatisfying(Dumper.SplitFileOutput.class, output -> {
            assertThat(output.fs()).isInstanceOf(SchemeFileSystemAbstraction.class);
            assertThat(output.baseArtifact()).isEqualTo(dumpDir.resolve("foo.dump"));
            assertThat(output.maxArtifactSize()).isEqualTo(ByteUnit.gibiBytes(5));
        });
    }

    private void execute(String database) {
        execute(database, dumpDir);
    }

    private void execute(String database, Path to, String... args) {
        final ExecutionContext ctx = new ExecutionContext(
                homeDir, configDir, mock(PrintStream.class), mock(PrintStream.class), testDirectory.getFileSystem());
        final var command = new DumpCommand(ctx) {
            @Override
            protected Dumper createDumper(FileSystemAbstraction fs, PrintStream out) {
                return dumper;
            }
        };
        String[] options = new String[args.length + 2];
        options[0] = database;
        options[1] = "--to-path=" + to.toAbsolutePath();
        for (int i = 0; i < args.length; ++i) {
            options[2 + i] = args[i];
        }
        CommandLine.populateCommand(command, options);

        command.execute();
    }

    private static void assertCanLockDatabase(Path databaseDirectory) throws IOException {
        try (FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
                Locker locker = new DatabaseLocker(fileSystem, DatabaseLayout.ofFlat(databaseDirectory))) {
            locker.checkLock();
        }
    }

    private void putStoreInDirectory(Config config, Path databaseDirectory) {
        String databaseName = databaseDirectory.getFileName().toString();
        try (DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder(
                        databaseDirectory.getParent().getParent().getParent())
                .setConfig(config)
                .setConfig(initial_default_database, databaseName)
                .build()) {
            databaseLayout = ((GraphDatabaseAPI) managementService.database(databaseName)).databaseLayout();
        }
    }

    private static boolean excludeDatabaseLock(Path path) {
        return path.getFileName().toString().equals("database_lock");
    }

    private static Closeable withPermissions(Path file, Set<PosixFilePermission> permissions) throws IOException {
        Set<PosixFilePermission> originalPermissions = Files.getPosixFilePermissions(file);
        Files.setPosixFilePermissions(file, permissions);
        return () -> Files.setPosixFilePermissions(file, originalPermissions);
    }

    private static String formatProperty(Setting setting, Path path) {
        return format("%s=%s", setting.name(), path.toString().replace('\\', '/'));
    }

    private static class Output {
        final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        final PrintStream printStream = new PrintStream(buffer);

        public boolean containsMessage(String message) {
            return toString().contains(message);
        }

        @Override
        public String toString() {
            return buffer.toString(StandardCharsets.UTF_8);
        }
    }
}
