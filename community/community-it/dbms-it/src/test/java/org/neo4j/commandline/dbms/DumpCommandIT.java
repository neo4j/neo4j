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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import java.util.Set;
import java.util.function.Predicate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.ConfigUtils;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.archive.Dumper;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.locker.Locker;
import org.neo4j.kernel.impl.transaction.SimpleAppendIndexProvider;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
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
import org.neo4j.test.utils.TestDirectory;
import picocli.CommandLine;

@Neo4jLayoutExtension
class DumpCommandIT {
    @Inject
    private TestDirectory testDirectory;

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
        verify(dumper).openForDump(eq(archive), eq(false));
        verify(dumper)
                .dump(
                        eq(homeDir.resolve("data/databases/foo")),
                        eq(homeDir.resolve("data/transactions/foo")),
                        any(),
                        any(),
                        any());
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
        verify(dumper).dump(eq(databaseDir), eq(txLogsDir), any(), any(), any());
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
        verify(dumper).dump(eq(databaseDir), eq(txlogsRoot.resolve("foo")), any(), any(), any());
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
        verify(dumper).dump(eq(realDatabaseDir), eq(txLogsDir), any(), any(), any());
    }

    @Test
    void shouldCalculateTheArchiveNameIfPassedAnExistingDirectory() throws Exception {
        Path to = testDirectory.directory("some-dir");
        execute("foo", to);
        Dumper dumper1 = verify(dumper);
        dumper1.openForDump(eq(to.resolve("foo.dump")), eq(false));
    }

    @Test
    void shouldNotCalculateTheArchiveNameIfPassedAnExistingFile() throws Exception {
        Files.createFile(archive);
        execute("foo");
        verify(dumper).openForDump(eq(archive), eq(false));
    }

    @Test
    void shouldRespectTheDatabaseLock() throws Exception {
        Path databaseDirectory = homeDir.resolve("data/databases/foo");
        DatabaseLayout databaseLayout = DatabaseLayout.ofFlat(databaseDirectory);
        try (FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
                Locker locker = new DatabaseLocker(fileSystem, databaseLayout)) {
            locker.checkLock();

            CommandFailedException commandFailed = assertThrows(CommandFailedException.class, () -> execute("foo"));
            assertThat(commandFailed.getMessage()).isEqualTo("Dump failed for databases: 'foo'");
            assertThat(commandFailed.getCause().getMessage())
                    .isEqualTo("The database is in use. Stop database 'foo' and try again.");
        }
    }

    @Test
    void databaseThatRequireRecoveryIsNotDumpable() throws IOException {
        LogFiles logFiles = LogFilesBuilder.builder(
                        databaseLayout, testDirectory.getFileSystem(), LatestVersions.LATEST_KERNEL_VERSION_PROVIDER)
                .withLogVersionRepository(new SimpleLogVersionRepository())
                .withTransactionIdStore(new SimpleTransactionIdStore())
                .withAppendIndexProvider(new SimpleAppendIndexProvider())
                .withStoreId(new StoreId(1, 1, "engine-1", "format-1", 1, 1))
                .build();
        try (Lifespan ignored = new Lifespan(logFiles)) {
            LogFile logFile = logFiles.getLogFile();
            LogEntryWriter<?> writer = logFile.getTransactionLogWriter().getWriter();
            writer.writeStartEntry(
                    LatestVersions.LATEST_KERNEL_VERSION,
                    0x123456789ABCDEFL,
                    4,
                    logFile.getLogFileInformation().getLastEntryId() + 1,
                    BASE_TX_CHECKSUM,
                    new byte[] {0});
        }
        CommandFailedException commandFailed = assertThrows(CommandFailedException.class, () -> execute("foo"));
        assertThat(commandFailed.getMessage()).isEqualTo("Dump failed for databases: 'foo'");
        assertThat(commandFailed.getCause().getMessage())
                .startsWith("Active logical log detected, this might be a source of inconsistencies.");
    }

    @Test
    void shouldReleaseTheDatabaseLockAfterDumping() throws Exception {
        execute("foo");
        assertCanLockDatabase(databaseDirectory);
    }

    @Test
    void shouldReleaseTheDatabaseLockEvenIfThereIsAnError() throws Exception {
        Dumper dumper1 = doThrow(IOException.class).when(dumper);
        dumper1.dump(any(), any(), any(), any(), any());
        assertThrows(CommandFailedException.class, () -> execute("foo"));
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
                .dump(any(), any(), any(), any(), any());

        execute("foo");
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    @DisabledForRoot
    void shouldReportAHelpfulErrorIfWeDontHaveWritePermissionsForLock() throws Exception {
        DatabaseLayout databaseLayout = DatabaseLayout.ofFlat(databaseDirectory);
        Path file = databaseLayout.databaseLockFile();
        try (Closeable ignored = withPermissions(file, emptySet())) {
            CommandFailedException commandFailed = assertThrows(CommandFailedException.class, () -> execute("foo"));
            assertThat(commandFailed.getMessage()).isEqualTo("Dump failed for databases: 'foo'");
            assertThat(commandFailed.getCause().getMessage())
                    .isEqualTo("You do not have permission to dump the database.");
        }
    }

    @Test
    void shouldExcludeTheStoreLockFromTheArchiveToAvoidProblemsWithReadingLockedFilesOnWindows() throws Exception {
        Path lockFile = DatabaseLayout.ofFlat(Path.of(".")).databaseLockFile();
        doAnswer(invocation -> {
                    Predicate<Path> exclude = invocation.getArgument(4);
                    assertThat(exclude.test(lockFile.getFileName())).isEqualTo(true);
                    assertThat(exclude.test(Path.of("some-other-file"))).isEqualTo(false);
                    return null;
                })
                .when(dumper)
                .dump(any(), any(), any(), any(), any());

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
        verify(dumper).dump(eq(databaseDir), eq(txLogsDir), any(), any(), any());
    }

    @Test
    void shouldGiveAClearErrorIfTheArchiveAlreadyExists() throws Exception {
        doThrow(new FileAlreadyExistsException("the-archive-path"))
                .when(dumper)
                .dump(any(), any(), any(), any(), any());
        CommandFailedException commandFailed = assertThrows(CommandFailedException.class, () -> execute("foo"));
        assertThat(commandFailed.getMessage()).isEqualTo("Dump failed for databases: 'foo'");
        assertThat(commandFailed.getCause().getMessage()).isEqualTo("Archive already exists: the-archive-path");
    }

    @Test
    void shouldGiveAClearMessageIfTheDatabaseDoesntExist() {
        CommandFailedException commandFailed = assertThrows(CommandFailedException.class, () -> execute("bobo"));
        assertThat(commandFailed.getMessage()).isEqualTo("Dump failed for databases: 'bobo'");
        assertThat(commandFailed.getCause().getMessage()).isEqualTo("Database does not exist: bobo");
    }

    @Test
    void shouldGiveAClearMessageIfTheArchivesParentDoesntExist() throws Exception {
        doThrow(new NoSuchFileException(archive.getParent().toString()))
                .when(dumper)
                .dump(any(), any(), any(), any(), any());
        CommandFailedException commandFailed = assertThrows(CommandFailedException.class, () -> execute("foo"));
        assertThat(commandFailed.getMessage()).isEqualTo("Dump failed for databases: 'foo'");
        assertThat(commandFailed.getCause().getMessage())
                .isEqualTo("Unable to dump database: NoSuchFileException: " + archive.getParent());
    }

    @Test
    void shouldWrapIOExceptionsCarefullyBecauseCriticalInformationIsOftenEncodedInTheirNameButMissingFromTheirMessage()
            throws Exception {
        doThrow(new IOException("the-message")).when(dumper).dump(any(), any(), any(), any(), any());
        CommandFailedException commandFailed = assertThrows(CommandFailedException.class, () -> execute("foo"));
        assertThat(commandFailed.getMessage()).isEqualTo("Dump failed for databases: 'foo'");
        assertThat(commandFailed.getCause().getMessage())
                .isEqualTo("Unable to dump database: IOException: the-message");
    }

    @Test
    void shouldDumpTheDatabaseToTheStdOut() throws Exception {
        var out = mock(PrintStream.class);
        var ctx = new ExecutionContext(homeDir, configDir, out, mock(PrintStream.class), testDirectory.getFileSystem());
        var command = new DumpCommand(ctx) {
            @Override
            protected Dumper createDumper(FileSystemAbstraction fs, PrintStream out) {
                return dumper;
            }
        };
        CommandLine.populateCommand(command, "--to-stdout", "foo");
        command.execute();

        verify(dumper)
                .dump(
                        eq(homeDir.resolve("data/databases/foo")),
                        eq(homeDir.resolve("data/transactions/foo")),
                        eq(out),
                        any(),
                        any());
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

        verify(dumper)
                .dump(
                        eq(homeDir.resolve("data/databases/foo")),
                        eq(homeDir.resolve("data/transactions/foo")),
                        eq(out.printStream),
                        any(),
                        any());
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
        CommandFailedException commandFailed = assertThrows(CommandFailedException.class, command::execute);
        assertThat(commandFailed.getMessage())
                .isEqualTo("Globbing in database name can not be used in combination with standard output. "
                        + "Specify a directory as destination or a single target database");
    }

    @Test
    void shouldNotAllowSpecifiedFile() throws IOException {
        Files.createFile(archive);
        CommandFailedException commandFailed =
                assertThrows(CommandFailedException.class, () -> execute("foo*", archive));
        assertThat(commandFailed.getMessage()).isEqualTo(archive + " is not an existing directory");
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

    private void execute(String database) {
        execute(database, dumpDir);
    }

    private void execute(String database, Path to) {
        final ExecutionContext ctx = new ExecutionContext(
                homeDir, configDir, mock(PrintStream.class), mock(PrintStream.class), testDirectory.getFileSystem());
        final var command = new DumpCommand(ctx) {
            @Override
            protected Dumper createDumper(FileSystemAbstraction fs, PrintStream out) {
                return dumper;
            }
        };

        CommandLine.populateCommand(command, database, "--to-path=" + to.toAbsolutePath());

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
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder(
                        databaseDirectory.getParent().getParent().getParent())
                .setConfig(config)
                .setConfig(initial_default_database, databaseName)
                .build();
        databaseLayout = ((GraphDatabaseAPI) managementService.database(databaseName)).databaseLayout();
        managementService.shutdown();
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
