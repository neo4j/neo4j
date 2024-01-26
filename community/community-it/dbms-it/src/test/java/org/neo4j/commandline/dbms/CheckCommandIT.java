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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.cli.AbstractAdminCommand.COMMAND_CONFIG_FILE_NAME_PATTERN;
import static org.neo4j.cli.CommandTestUtils.withSuppressedOutput;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.consistency.CheckCommand;
import org.neo4j.consistency.CheckNativeDatabase;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.ConsistencyFlags;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.dbms.archive.CheckDatabase;
import org.neo4j.dbms.archive.CheckDump;
import org.neo4j.dbms.archive.Dumper;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.locker.FileLockException;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.utils.TestDirectory;
import picocli.CommandLine;

@Neo4jLayoutExtension
class CheckCommandIT {
    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction filesytem;

    @Inject
    private Neo4jLayout neo4jLayout;

    private Path homeDir;
    private Path confPath;
    private String dbName;

    @BeforeEach
    void setUp() {
        homeDir = testDirectory.homePath();
        confPath = testDirectory.directory("conf");
        dbName = "mydb";
        prepareDatabase(neo4jLayout.databaseLayout(dbName));
    }

    @Test
    void printUsageHelp() {
        final var baos = new ByteArrayOutputStream();
        final var checkCommand = new CheckCommand(new ExecutionContext(Path.of("."), Path.of(".")));
        final var cmd = new CommandLine(checkCommand).setUsageHelpWidth(120);
        try (var out = new PrintStream(baos)) {
            cmd.usage(new PrintStream(out), CommandLine.Help.Ansi.OFF);
        }
        assertThat(baos.toString().trim())
                .isEqualToIgnoringNewLines(
                        """
                        Check the consistency of a database.

                        USAGE

                        check [-h] [--expand-commands] [--force] [--verbose] [--check-counts[=true|false]] [--check-graph[=true|false]]
                              [--check-indexes[=true|false]] [--check-property-owners[=true|false]] [--additional-config=<file>]
                              [--max-off-heap-memory=<size>] [--report-path=<path>] [--threads=<number of threads>] [[--from-path-data=<path>
                              --from-path-txn=<path>] | [--from-path=<path> [--temp-path=<path>]]] <database>

                        DESCRIPTION

                        This command allows for checking the consistency of a database, or a dump or backup thereof.
                        It cannot be used with a database which is currently in use.

                        Some checks can be quite expensive, so it may be useful to turn some of them off
                        for very large databases. Increasing the heap size can also be a good idea.
                        See 'neo4j-admin help' for details.

                        PARAMETERS

                              <database>             Name of the database to check.

                        OPTIONS

                              --verbose              Enable verbose output.
                          -h, --help                 Show this help message and exit.
                              --expand-commands      Allow command expansion in config value evaluation.
                              --additional-config=<file>
                                                     Configuration file with additional configuration.
                              --force                Force a consistency check to be run, despite resources, and may run a more thorough check.
                              --check-indexes[=true|false]
                                                     Perform consistency checks on indexes.
                                                       Default: true
                              --check-graph[=true|false]
                                                     Perform consistency checks between nodes, relationships, properties, types, and tokens.
                                                       Default: true
                              --check-counts[=true|false]
                                                     Perform consistency checks on the counts. Requires <check-graph>, and may implicitly
                                                       enable <check-graph> if it were not explicitly disabled.
                                                       Default: <check-graph>
                              --check-property-owners[=true|false]
                                                     Perform consistency checks on the ownership of properties. Requires <check-graph>, and may
                                                       implicitly enable <check-graph> if it were not explicitly disabled.
                                                       Default: false
                              --report-path=<path>   Path to where a consistency report will be written. Interpreted as a directory, unless it
                                                       has an extension of '.report'.
                                                       Default: .
                              --max-off-heap-memory=<size>
                                                     Maximum memory that neo4j-admin can use for page cache and various caching data structures
                                                       to improve performance. Value can be plain numbers, like 10000000 or e.g. 20G for 20
                                                       gigabytes, or even e.g. 70%, which will amount to 70% of currently free memory on the
                                                       machine.
                                                       Default: 90%
                              --threads=<number of threads>
                                                     Number of threads used to check the consistency. Default: The number of CPUs on the
                                                       machine.
                              --from-path-data=<path>
                                                     Path to the databases directory, containing the database directory to source from.
                                                       Default: <config: server.directories.data>/databases
                              --from-path-txn=<path> Path to the transactions directory, containing the transaction directory for the database
                                                       to source from.
                                                       Default: <config: server.directories.transaction.logs.root>
                              --from-path=<path>     Path to the directory containing dump/backup artifacts that need to be checked for
                                                       consistency. If the directory contains multiple backups, it will select the most recent
                                                       backup chain, based on the transaction IDs found, to perform the consistency check.
                              --temp-path=<path>     Path to directory to be used as a staging area to extract dump/backup artifacts, if needed.
                                                       Default:  <from-path>""");
    }

    @Test
    void correctCheckDatabasesFound() {
        final var checkDatabases = CheckDatabase.all().stream()
                .map(CheckDatabase::getClass)
                .collect(Collectors.<Class<? extends CheckDatabase>>toUnmodifiableSet());
        assertThat(checkDatabases).containsExactlyInAnyOrder(CheckNativeDatabase.class, CheckDump.class);
    }

    @Test
    void runsConsistencyChecker() {
        final var databaseLayout = neo4jLayout.databaseLayout(dbName);

        final var consistencyCheckService =
                new TrackingConsistencyCheckService(ConsistencyCheckService.Result.success(null, null));

        final var checkCommand = new CheckCommand(new ExecutionContext(homeDir, confPath), consistencyCheckService);
        CommandLine.populateCommand(checkCommand, dbName);
        checkCommand.execute();

        verifyCheckableLayout(consistencyCheckService, databaseLayout);
    }

    @Test
    void consistencyCheckerRespectDatabaseLock() throws CannotWriteException, IOException {
        final var databaseLayout = neo4jLayout.databaseLayout(dbName);
        filesytem.mkdirs(databaseLayout.databaseDirectory());

        final var consistencyCheckService =
                new TrackingConsistencyCheckService(ConsistencyCheckService.Result.success(null, null));

        final var checkCommand = new CheckCommand(new ExecutionContext(homeDir, confPath), consistencyCheckService);
        CommandLine.populateCommand(checkCommand, "--verbose", dbName);

        try (var ignored = LockChecker.checkDatabaseLock(databaseLayout)) {
            final var assertFailure =
                    assertThatThrownBy(checkCommand::execute).isInstanceOf(CommandFailedException.class);
            assertFailure.cause().isInstanceOf(FileLockException.class);
            assertFailure.hasMessageContainingAll("The database is in use", "Stop database", dbName, "and try again");
        }
    }

    @Test
    void enablesVerbosity() {
        final var consistencyCheckService =
                new TrackingConsistencyCheckService(ConsistencyCheckService.Result.success(null, null));

        final var checkCommand = new CheckCommand(new ExecutionContext(homeDir, confPath), consistencyCheckService);
        CommandLine.populateCommand(checkCommand, "--verbose", dbName);
        checkCommand.execute();

        consistencyCheckService.verifyArgument(Boolean.class, true);
    }

    @Test
    void failsWhenInconsistenciesAreFound() {
        final var reportPath = Path.of("/the/report/path");

        final var consistencyCheckService = new TrackingConsistencyCheckService(
                ConsistencyCheckService.Result.failure(reportPath, new ConsistencySummaryStatistics()));

        final var checkCommand = new CheckCommand(new ExecutionContext(homeDir, confPath), consistencyCheckService);
        CommandLine.populateCommand(checkCommand, "--verbose", dbName);

        assertThatThrownBy(checkCommand::execute)
                .isInstanceOf(CommandFailedException.class)
                .hasMessageContaining(reportPath.toString());
    }

    @Test
    void shouldWriteReportFileToCurrentDirectoryByDefault() throws CommandFailedException {
        final var consistencyCheckService =
                new TrackingConsistencyCheckService(ConsistencyCheckService.Result.success(null, null));

        final var checkCommand = new CheckCommand(new ExecutionContext(homeDir, confPath), consistencyCheckService);
        CommandLine.populateCommand(checkCommand, dbName);
        checkCommand.execute();

        consistencyCheckService.verifyArgument(Path.class, Path.of(""));
    }

    @Test
    void shouldWriteReportFileToSpecifiedPath() throws CommandFailedException {
        final var consistencyCheckService =
                new TrackingConsistencyCheckService(ConsistencyCheckService.Result.success(null, null));

        final var checkCommand = new CheckCommand(new ExecutionContext(homeDir, confPath), consistencyCheckService);
        CommandLine.populateCommand(checkCommand, "--report-path=some-dir-or-other", dbName);
        checkCommand.execute();

        consistencyCheckService.verifyArgument(Path.class, Path.of("some-dir-or-other"));
    }

    @Test
    void shouldCanonicalizeReportPath() throws CommandFailedException {
        final var consistencyCheckService =
                new TrackingConsistencyCheckService(ConsistencyCheckService.Result.success(null, null));

        final var checkCommand = new CheckCommand(new ExecutionContext(homeDir, confPath), consistencyCheckService);
        CommandLine.populateCommand(checkCommand, "--report-path=" + Path.of("..", "bar"), dbName);
        checkCommand.execute();

        consistencyCheckService.verifyArgument(Path.class, Path.of("../bar"));
    }

    @Test
    void passesOnCheckParameters() {
        final var consistencyCheckService =
                new TrackingConsistencyCheckService(ConsistencyCheckService.Result.success(null, null));

        final var checkCommand = new CheckCommand(new ExecutionContext(homeDir, confPath), consistencyCheckService);
        CommandLine.populateCommand(
                checkCommand, "--check-indexes=false", "--check-graph=false", "--check-counts=false", dbName);
        checkCommand.execute();

        consistencyCheckService.verifyArgument(
                ConsistencyFlags.class,
                ConsistencyFlags.DEFAULT
                        .withoutCheckIndexes()
                        .withoutCheckGraph()
                        .withoutCheckCounts());
    }

    @Test
    void passesOnImplicitCheckGraph() {
        final var consistencyCheckService =
                new TrackingConsistencyCheckService(ConsistencyCheckService.Result.success(null, null));

        final var checkCommand = new CheckCommand(new ExecutionContext(homeDir, confPath), consistencyCheckService);
        CommandLine.populateCommand(checkCommand, "--check-counts=true", dbName);

        assertThatCode(checkCommand::execute).doesNotThrowAnyException();
        consistencyCheckService.verifyArgument(ConsistencyFlags.class, ConsistencyFlags.DEFAULT.withCheckCounts());
    }

    @Test
    void failsOnExplicitNoCheckGraphWithAGraphCheck() {
        final var consistencyCheckService =
                new TrackingConsistencyCheckService(ConsistencyCheckService.Result.success(null, null));

        final var checkCommand = new CheckCommand(new ExecutionContext(homeDir, confPath), consistencyCheckService);
        CommandLine.populateCommand(checkCommand, "--check-graph=false", "--check-counts=true", dbName);

        assertThatThrownBy(checkCommand::execute)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll(
                        "check-counts", "cannot be true if", "check-graph", "is explicitly set to false");
    }

    @Test
    void shouldUseCheckCommandConfigIfAvailable() throws Exception {
        // Checking that the command is unhappy about an invalid value is enough to verify
        // that the command-specific config is being taken into account.

        final var nonsense = "some nonsense";
        final var commandConfigFile = confPath.resolve(COMMAND_CONFIG_FILE_NAME_PATTERN.formatted("database-check"));
        try (var outputStream = filesytem.openAsOutputStream(commandConfigFile, true);
                var out = new PrintStream(outputStream)) {
            out.printf("%s=%s%n", pagecache_memory.name(), nonsense);
        }

        final var consistencyCheckService =
                new TrackingConsistencyCheckService(ConsistencyCheckService.Result.success(null, null));

        final var checkCommand = new CheckCommand(new ExecutionContext(homeDir, confPath), consistencyCheckService);
        CommandLine.populateCommand(checkCommand, dbName);

        assertThatThrownBy(checkCommand::execute)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll(nonsense, "is not a valid size");
    }

    @Test
    void dumpNeedsToBePath() {
        final var dumpPath = homeDir.resolve("dir/does/not/exist");

        final var consistencyCheckService =
                new TrackingConsistencyCheckService(ConsistencyCheckService.Result.success(null, null));

        final var checkCommand = new CheckCommand(new ExecutionContext(homeDir, confPath), consistencyCheckService);
        CommandLine.populateCommand(checkCommand, "--from-path=" + dumpPath, dbName);

        assertThatThrownBy(checkCommand::execute)
                .isInstanceOf(CommandFailedException.class)
                .hasMessageContainingAll(
                        "Could not find a valid", "dump", "named", dbName, "to check at path", dumpPath.toString());
    }

    @Test
    void canRunOnOtherNativeDatabase() throws Exception {
        final var consistencyCheckService =
                new TrackingConsistencyCheckService(ConsistencyCheckService.Result.success(null, null));

        final var other = testDirectory.directory("other");
        final var layout = Neo4jLayout.of(other).databaseLayout("other");
        final var dataPath = layout.getNeo4jLayout().databasesDirectory();
        final var txnPath = layout.getNeo4jLayout().transactionLogsRootDirectory();
        removeAndReprepareDatabase(layout);

        final var checkCommand = new CheckCommand(new ExecutionContext(homeDir, confPath), consistencyCheckService);
        CommandLine.populateCommand(
                checkCommand, "--from-path-data=" + dataPath, "--from-path-txn=" + txnPath, layout.getDatabaseName());
        checkCommand.execute();

        verifyCheckableLayout(consistencyCheckService, layout);
    }

    @Test
    void checkThisNativeDatabase() {
        withSuppressedOutput(homeDir, confPath, filesytem, ctx -> {
            final var checkCommand = new CheckCommand(ctx);
            CommandLine.populateCommand(checkCommand, dbName);
            assertThatCode(checkCommand::execute).doesNotThrowAnyException();
        });
    }

    @Test
    void checkOtherNativeDatabase() throws IOException {
        final var other = testDirectory.directory("other");
        final var layout = Neo4jLayout.of(other).databaseLayout(dbName);
        final var dataPath = layout.getNeo4jLayout().databasesDirectory();
        final var txnPath = layout.getNeo4jLayout().transactionLogsRootDirectory();
        removeAndReprepareDatabase(layout);

        withSuppressedOutput(homeDir, confPath, filesytem, ctx -> {
            final var checkCommand = new CheckCommand(ctx);
            CommandLine.populateCommand(
                    checkCommand, "--from-path-data=" + dataPath, "--from-path-txn=" + txnPath, dbName);
            assertThatCode(checkCommand::execute).doesNotThrowAnyException();
        });
    }

    @Test
    void checkDump() {
        final var dump = testDirectory.directory("dump");
        createDump(dump);

        withSuppressedOutput(homeDir, confPath, filesytem, ctx -> {
            final var checkCommand = new CheckCommand(ctx);
            CommandLine.populateCommand(checkCommand, "--from-path=" + dump, dbName);
            assertThatCode(checkCommand::execute).doesNotThrowAnyException();
        });
    }

    @Test
    void checkStagingAreaGetsClearedAwayForDump() {
        final var dump = testDirectory.directory("dump");
        createDump(dump);

        // This will do all the setup but skip the actual consistency checking.
        // Our temporary staging area should exist when the ConsistencyCheckService#runFullConsistencyCheck is called.
        // Should have used the from-path as staging area when nothing was supplied.
        AtomicReference<Path> tempDir = new AtomicReference<>();
        final var consistencyCheckService = getConsistencyCheckServiceWithTempDirChecking(
                dump, tempDir, ConsistencyCheckService.Result.success(null, null));
        withSuppressedOutput(homeDir, confPath, filesytem, ctx -> {
            final var checkCommand = new CheckCommand(ctx, consistencyCheckService);
            CommandLine.populateCommand(checkCommand, "--from-path=" + dump, dbName);
            checkCommand.execute();
        });

        // After the command has finished the staging area should have been cleared away
        assertFalse(testDirectory.getFileSystem().fileExists(tempDir.get()));
    }

    @Test
    void checkStagingAreaGetsClearedAwayForDumpEvenIfCheckFails() {
        final var dump = testDirectory.directory("dump");
        createDump(dump);

        // This will do all the setup but skip the actual consistency checking.
        // Our temporary staging area should exist when the ConsistencyCheckService#runFullConsistencyCheck is called.
        // Should have used the from-path as staging area when nothing was supplied.
        AtomicReference<Path> tempDir = new AtomicReference<>();
        final var consistencyCheckService = getConsistencyCheckServiceWithTempDirChecking(
                dump, tempDir, ConsistencyCheckService.Result.failure(null, null));
        withSuppressedOutput(homeDir, confPath, filesytem, ctx -> {
            final var checkCommand = new CheckCommand(ctx, consistencyCheckService);
            CommandLine.populateCommand(checkCommand, "--from-path=" + dump, dbName);
            assertThatThrownBy(checkCommand::execute).isInstanceOf(CommandFailedException.class);
        });

        // After the command has finished the staging area should have been cleared away
        assertFalse(testDirectory.getFileSystem().fileExists(tempDir.get()));
    }

    @Test
    void checkTempPathRespectedAsRootForStagingArea() {
        final var dump = testDirectory.directory("dump");
        createDump(dump);

        // This will do all the setup but skip the actual consistency checking.
        // Our temporary staging area should exist when the ConsistencyCheckService#runFullConsistencyCheck is called.
        final var reqTempDir = testDirectory.directory("tempRoot");
        AtomicReference<Path> tempDir = new AtomicReference<>();
        final var consistencyCheckService = getConsistencyCheckServiceWithTempDirChecking(
                reqTempDir, tempDir, ConsistencyCheckService.Result.success(null, null));
        withSuppressedOutput(homeDir, confPath, filesytem, ctx -> {
            final var checkCommand = new CheckCommand(ctx, consistencyCheckService);
            CommandLine.populateCommand(checkCommand, "--from-path=" + dump, "--temp-path=" + reqTempDir, dbName);
            checkCommand.execute();
        });

        // After the command has finished the staging area should have been cleared away and the temp-path should still
        // exist
        assertFalse(testDirectory.getFileSystem().fileExists(tempDir.get()));
        assertTrue(testDirectory.getFileSystem().fileExists(reqTempDir));
    }

    private void removeAndReprepareDatabase(DatabaseLayout databaseLayout) throws IOException {
        filesytem.deleteRecursively(homeDir);
        prepareDatabase(databaseLayout);
    }

    private static void prepareDatabase(DatabaseLayout databaseLayout) {
        new TestDatabaseManagementServiceBuilder(databaseLayout).build().shutdown();
    }

    private void verifyCheckableLayout(TrackingConsistencyCheckService service, DatabaseLayout plainLayout) {
        final var neo4jLayout = plainLayout.getNeo4jLayout();

        final var assertPlainLayout = service.assertArgument(DatabaseLayout.class);
        assertPlainLayout
                .extracting(DatabaseLayout::getDatabaseName, InstanceOfAssertFactories.STRING)
                .isEqualTo(plainLayout.getDatabaseName());

        final var assertNeo4jLayout = assertPlainLayout.extracting(DatabaseLayout::getNeo4jLayout);
        assertNeo4jLayout
                .extracting(Neo4jLayout::databasesDirectory, InstanceOfAssertFactories.PATH)
                .isEqualTo(neo4jLayout.databasesDirectory());
        assertNeo4jLayout
                .extracting(Neo4jLayout::transactionLogsRootDirectory, InstanceOfAssertFactories.PATH)
                .isEqualTo(neo4jLayout.transactionLogsRootDirectory());
    }

    private TrackingConsistencyCheckService getConsistencyCheckServiceWithTempDirChecking(
            Path expectedTempRoot, AtomicReference<Path> tempDir, ConsistencyCheckService.Result returnValue) {
        return new TrackingConsistencyCheckService(returnValue, (service) -> {
            DatabaseLayout layout = (DatabaseLayout) service.arguments.get(DatabaseLayout.class);
            tempDir.set(layout.getNeo4jLayout().homeDirectory().toAbsolutePath());
            assertTrue(testDirectory.getFileSystem().fileExists(tempDir.get()));
            assertThat(tempDir.get().getParent().toAbsolutePath()).isEqualTo(expectedTempRoot.toAbsolutePath());
        });
    }

    private void createDump(Path dump) {
        withSuppressedOutput(homeDir, confPath, filesytem, ctx -> {
            final var dumpCommand = new DumpCommand(ctx, new Dumper(ctx.out()));
            CommandLine.populateCommand(dumpCommand, "--to-path=" + dump, dbName);
            assertThatCode(dumpCommand::execute).doesNotThrowAnyException();
        });
    }

    private static class TrackingConsistencyCheckService extends ConsistencyCheckService {
        private final Map<Class<?>, Object> arguments;
        private final Result result;
        private final Consumer<TrackingConsistencyCheckService> runnable;

        TrackingConsistencyCheckService(Result result) {
            this(result, null);
        }

        TrackingConsistencyCheckService(Result result, Consumer<TrackingConsistencyCheckService> runnable) {
            super(null);
            this.result = result;
            this.arguments = new HashMap<>();
            this.runnable = runnable;
        }

        TrackingConsistencyCheckService(TrackingConsistencyCheckService from) {
            super(null);
            this.result = from.result;
            this.arguments = from.arguments;
            this.runnable = from.runnable;
        }

        @Override
        public ConsistencyCheckService with(Config config) {
            arguments.put(Config.class, config);
            super.with(config);
            return new TrackingConsistencyCheckService(this);
        }

        @Override
        public ConsistencyCheckService with(Date timestamp) {
            arguments.put(Date.class, timestamp);
            super.with(timestamp);
            return new TrackingConsistencyCheckService(this);
        }

        @Override
        public ConsistencyCheckService with(DatabaseLayout layout) {
            arguments.put(DatabaseLayout.class, layout);
            super.with(layout);
            return new TrackingConsistencyCheckService(this);
        }

        @Override
        public ConsistencyCheckService with(OutputStream progressOutput) {
            arguments.put(OutputStream.class, progressOutput);
            super.with(progressOutput);
            return new TrackingConsistencyCheckService(this);
        }

        @Override
        public ConsistencyCheckService with(InternalLogProvider logProvider) {
            arguments.put(InternalLogProvider.class, logProvider);
            super.with(logProvider);
            return new TrackingConsistencyCheckService(this);
        }

        @Override
        public ConsistencyCheckService with(FileSystemAbstraction fileSystem) {
            arguments.put(FileSystemAbstraction.class, fileSystem);
            super.with(fileSystem);
            return new TrackingConsistencyCheckService(this);
        }

        @Override
        public ConsistencyCheckService with(PageCache pageCache) {
            arguments.put(PageCache.class, pageCache);
            super.with(pageCache);
            return new TrackingConsistencyCheckService(this);
        }

        @Override
        public ConsistencyCheckService verbose(boolean verbose) {
            arguments.put(Boolean.class, verbose);
            super.verbose(verbose);
            return new TrackingConsistencyCheckService(this);
        }

        @Override
        public ConsistencyCheckService with(Path reportPath) {
            arguments.put(Path.class, reportPath);
            super.with(reportPath);
            return new TrackingConsistencyCheckService(this);
        }

        @Override
        public ConsistencyCheckService with(ConsistencyFlags consistencyFlags) {
            arguments.put(ConsistencyFlags.class, consistencyFlags);
            super.with(consistencyFlags);
            return new TrackingConsistencyCheckService(this);
        }

        @Override
        public ConsistencyCheckService with(PageCacheTracer pageCacheTracer) {
            arguments.put(PageCacheTracer.class, pageCacheTracer);
            super.with(pageCacheTracer);
            return new TrackingConsistencyCheckService(this);
        }

        @Override
        public ConsistencyCheckService with(MemoryTracker memoryTracker) {
            arguments.put(MemoryTracker.class, memoryTracker);
            super.with(memoryTracker);
            return new TrackingConsistencyCheckService(this);
        }

        @Override
        public ConsistencyCheckService with(CursorContextFactory contextFactory) {
            arguments.put(CursorContextFactory.class, contextFactory);
            super.with(contextFactory);
            return new TrackingConsistencyCheckService(this);
        }

        @Override
        public ConsistencyCheckService withMaxOffHeapMemory(long maxOffHeapMemory) {
            arguments.put(Long.TYPE, maxOffHeapMemory);
            super.withMaxOffHeapMemory(maxOffHeapMemory);
            return new TrackingConsistencyCheckService(this);
        }

        @Override
        public ConsistencyCheckService withNumberOfThreads(int numberOfThreads) {
            arguments.put(Integer.TYPE, numberOfThreads);
            super.withNumberOfThreads(numberOfThreads);
            return new TrackingConsistencyCheckService(this);
        }

        @Override
        public Result runFullConsistencyCheck() {
            if (runnable != null) {
                runnable.accept(this);
            }
            return result;
        }

        <T> ObjectAssert<T> assertArgument(Class<T> type) {
            final var arg = arguments.get(type);
            assertThat(arg).isInstanceOf(type);
            return assertThat((T) arg);
        }

        void verifyArgument(Class<?> type, Object expectedValue) {
            assertArgument(type).isEqualTo(expectedValue);
        }
    }
}
