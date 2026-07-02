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
import static org.neo4j.dbms.archive.Dumper.DUMP_EXTENSION;
import static org.neo4j.internal.helpers.Strings.joinAsLines;
import static org.neo4j.kernel.recovery.Recovery.isRecoveryRequired;
import static picocli.CommandLine.Command;
import static picocli.CommandLine.Help.Visibility.ALWAYS;
import static picocli.CommandLine.Option;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;
import org.neo4j.cli.AbstractAdminCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.Converters;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.cloud.storage.SchemeFileSystemAbstraction;
import org.neo4j.commandline.Util;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.DatabaseNamePattern;
import org.neo4j.dbms.archive.DumpFormatSelector;
import org.neo4j.dbms.archive.Dumper;
import org.neo4j.dbms.archive.Dumper.DumpOutput;
import org.neo4j.dbms.archive.Dumper.FileOutput;
import org.neo4j.dbms.archive.Dumper.StdoutOutput;
import org.neo4j.dbms.archive.Manifest;
import org.neo4j.internal.helpers.ArrayUtil;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.locker.FileLockException;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.storemigration.StoreMigrator;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.NullLog;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryPools;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.DeprecatedFormatWarning;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.time.Clocks;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Help.Visibility;
import picocli.CommandLine.Parameters;

@Command(
        name = "dump",
        header = "Dump a database into a single-file archive.",
        description = "Dump a database into a single-file archive. The archive can be used by the load command. "
                + "<to-path> should be a directory (in which case a file called <database>.dump will "
                + "be created), or --to-stdout can be supplied to use standard output. "
                + "If neither --to-path or --to-stdout is supplied `server.directories.dumps.root` setting will "
                + "be used as destination. "
                + "It is not possible to dump a database that is mounted in a running Neo4j server.")
public class DumpCommand extends AbstractAdminCommand {
    @Parameters(
            arity = "1",
            description = "Name of the database to dump. Can contain * and ? for globbing. "
                    + "Note that * and ? have special meaning in some shells "
                    + "and might need to be escaped or used with quotes.",
            converter = Converters.DatabaseNamePatternConverter.class)
    private DatabaseNamePattern database;

    @ArgGroup()
    private TargetOption target = new TargetOption();

    private static class TargetOption {
        @Option(
                names = "--to-path",
                paramLabel = "<path>",
                description = "Destination folder of a database dump.\n"
                        + "It is possible to dump databases into AWS S3 buckets, Google Cloud storage buckets, and "
                        + "Azure buckets using the appropriate URI as the path.")
        private String toDir;

        @Option(names = "--to-stdout", description = "Use standard output as the destination for the database dump.")
        private boolean toStdout;
    }

    @Option(
            names = "--overwrite-destination",
            arity = "0..1",
            paramLabel = "true|false",
            fallbackValue = "true",
            showDefaultValue = ALWAYS,
            description = "Overwrite any existing dump file in the destination folder.")
    private boolean overwriteDestination;

    @Option(
            names = "--experimental-split-size",
            arity = "1",
            paramLabel = "<splitsize>",
            hidden = true,
            showDefaultValue = Visibility.NEVER,
            description = "Split archive at certain size intervals",
            converter = Converters.ByteUnitConverter.class)
    private long overrideArchiveSplitSize = 0;

    private InternalLog log;

    public DumpCommand(ExecutionContext ctx) {
        super(ctx);
    }

    @Override
    protected Optional<String> commandConfigName() {
        return Optional.of("database-dump");
    }

    @Override
    public void execute() {
        final var config = createConfig();

        final var dumpToStdOut = target.toStdout;
        final var logStream = dumpToStdOut ? ctx.err() : ctx.out();
        try (var logProvider = Util.configuredLogProvider(logStream, verbose);
                var fs = new SchemeFileSystemAbstraction(ctx.fs(), config, logProvider)) {
            final var dumper = createDumper(fs, logStream);

            Path storagePath = null;
            if (target.toDir != null) {
                storagePath = normalizeAndValidateIfStoragePathDirectory(fs.resolve(target.toDir));
                if (!fs.isDirectory(storagePath)) {
                    throw new CommandFailedException(target.toDir + " is not an existing directory");
                }
            }

            if (dumpToStdOut && database.containsPattern()) {
                throw new CommandFailedException(
                        "Globbing in database name can not be used in combination with standard "
                                + "output. Specify a directory as destination or a single target database");
            }

            if (storagePath == null && !dumpToStdOut) {
                storagePath = createDefaultDumpsDir(fs, config);
            }

            log = logProvider.getLog(getClass());

            List<FailedDump> failedDumps = new ArrayList<>();

            var memoryTracker = EmptyMemoryTracker.INSTANCE;

            for (String databaseName : getDbNames(config, fs, database)) {
                try {
                    log.info(format("Starting dump of database '%s'", databaseName));

                    DatabaseLayout databaseLayout = Neo4jLayout.of(config).databaseLayout(databaseName);

                    try {
                        Validators.CONTAINS_EXISTING_DATABASE.validate(databaseLayout.databaseDirectory());
                    } catch (IllegalArgumentException e) {
                        throw new CommandFailedException("Database does not exist: " + databaseName, e);
                    }

                    if (fs.fileExists(databaseLayout.path(StoreMigrator.MIGRATION_DIRECTORY))) {
                        throw new CommandFailedException(
                                "Store migration folder detected - A dump can not be taken during a store migration. Make sure "
                                        + "store migration is completed before trying again.");
                    }

                    try (Closeable ignored = LockChecker.checkDatabaseLock(databaseLayout)) {
                        checkDbState(fs, databaseLayout, config, memoryTracker, databaseName, log);
                        logFormatDeprecationWarning(log, databaseLayout, config, fs);
                        dump(dumper, config, databaseLayout, databaseName, storagePath, fs);
                    } catch (FileLockException e) {
                        throw new CommandFailedException(
                                "The database is in use. Stop database '" + databaseName + "' and try again.", e);
                    } catch (CannotWriteException e) {
                        throw new CommandFailedException("You do not have permission to dump the database.", e);
                    }
                } catch (Exception e) {
                    log.error("Failed to dump database '" + databaseName + "': " + e.getMessage());
                    failedDumps.add(new FailedDump(databaseName, e));
                }
            }

            if (failedDumps.isEmpty()) {
                log.info("Dump completed successfully");
            } else {
                StringJoiner failedDbs = new StringJoiner("', '", "Dump failed for databases: '", "'");
                Exception exceptions = null;
                for (FailedDump failedDump : failedDumps) {
                    failedDbs.add(failedDump.dbName);
                    exceptions = Exceptions.chain(exceptions, failedDump.e);
                }
                log.error(failedDbs.toString());
                throw new CommandFailedException(failedDbs.toString(), exceptions);
            }
        } catch (IOException e) {
            wrapIOException(e);
        }
    }

    protected Dumper createDumper(FileSystemAbstraction fs, PrintStream out) {
        return new Dumper(fs, out);
    }

    private Path createDefaultDumpsDir(SchemeFileSystemAbstraction fs, Config config) {
        final var defaultDumpPath = config.get(GraphDatabaseSettings.database_dumps_root_path);
        try {
            fs.mkdirs(defaultDumpPath);
        } catch (IOException e) {
            throw new CommandFailedException(
                    format(
                            "Unable to create default dumps directory at '%s': %s",
                            defaultDumpPath, e.getClass().getSimpleName()),
                    e);
        }
        return defaultDumpPath;
    }

    private Config createConfig() {
        return createPrefilledConfigBuilder()
                .set(GraphDatabaseSettings.read_only_database_default, true)
                .build();
    }

    private static void logFormatDeprecationWarning(
            InternalLog log, DatabaseLayout databaseLayout, Config config, FileSystemAbstraction fs) {
        try (JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
                PageCache pageCache = getPageCache(config, fs, jobScheduler)) {
            StorageEngineFactory storageEngineFactory =
                    StorageEngineFactory.selectStorageEngine(fs, databaseLayout, config);
            String format = storageEngineFactory
                    .retrieveStoreId(fs, databaseLayout, pageCache, CursorContext.NULL_CONTEXT)
                    .getFormatName();
            if (storageEngineFactory.isDeprecated(format)) {
                log.warn(DeprecatedFormatWarning.getFormatWarning(databaseLayout.getDatabaseName(), format));
            }
        } catch (IOException e) {
            throw new CommandFailedException("Unable to find store id");
        }
    }

    private static PageCache getPageCache(Config config, FileSystemAbstraction fs, JobScheduler jobScheduler) {
        ConfiguringPageCacheFactory pageCacheFactory = new ConfiguringPageCacheFactory(
                fs,
                config,
                PageCacheTracer.NULL,
                NullLog.getInstance(),
                jobScheduler,
                Clocks.nanoClock(),
                new MemoryPools());
        return pageCacheFactory.getOrCreatePageCache();
    }

    record FailedDump(String dbName, Exception e) {}

    private DumpOutput openDumpStream(
            SchemeFileSystemAbstraction fs, Config config, String databaseName, Path storagePath) throws IOException {
        if (storagePath == null) {
            return new StdoutOutput(ctx);
        }

        final var archive = storagePath.resolve(databaseName + DUMP_EXTENSION).toAbsolutePath();
        long splitSize = Dumper.SplitFileOutput.determineSplitArtifactSize(config, overrideArchiveSplitSize);

        if (splitSize > 0) {
            // TODO(split-backups): Do pruning of archives
            // TODO(split-backups): Validate split size
            return new Dumper.SplitFileOutput(fs, archive, splitSize);
        }

        // Allow "overwriting" of existing dumps.
        if (fs.fileExists(archive) && overwriteDestination) {
            fs.delete(archive);
        }
        return FileOutput.of(fs, archive);
    }

    private void dump(
            Dumper dumper,
            Config config,
            DatabaseLayout databaseLayout,
            String databaseName,
            Path storagePath,
            SchemeFileSystemAbstraction fs) {
        Path databasePath = databaseLayout.databaseDirectory();
        try {
            var format = DumpFormatSelector.selectWriteFormat(ctx.err());
            var lockFile = databaseLayout.databaseLockFile().getFileName().toString();
            var quarantineMarkerFile =
                    databaseLayout.quarantineFile().getFileName().toString();
            // this is closed inside the dump call
            Manifest mf = Dumper.collectManifest(
                    databasePath,
                    databaseLayout.getTransactionLogsDirectory(),
                    path -> oneOf(path, lockFile, quarantineMarkerFile));
            dumper.dump(openDumpStream(fs, config, databaseName, storagePath), format, mf);
        } catch (FileAlreadyExistsException e) {
            throw new CommandFailedException(format("Archive already exists: %s", e.getMessage()), e);
        } catch (NoSuchFileException e) {
            if (Paths.get(e.getMessage()).toAbsolutePath().equals(databasePath)) {
                throw new CommandFailedException("Database does not exist: " + databaseLayout.getDatabaseName(), e);
            }
            wrapIOException(e);
        } catch (IOException e) {
            wrapIOException(e);
        }
    }

    private static boolean oneOf(Path path, String... names) {
        return ArrayUtil.contains(names, path.getFileName().toString());
    }

    protected void checkDbState(
            FileSystemAbstraction fs,
            DatabaseLayout databaseLayout,
            Config additionalConfiguration,
            MemoryTracker memoryTracker,
            String databaseName,
            InternalLog log) {
        if (checkRecoveryState(fs, databaseLayout, additionalConfiguration, memoryTracker)) {
            throw new CommandFailedException(joinAsLines(
                    "Active logical log detected, this might be a source of inconsistencies.",
                    "Please recover database before running the dump.",
                    "To perform recovery please start database and perform clean shutdown."));
        }
    }

    private static boolean checkRecoveryState(
            FileSystemAbstraction fs,
            DatabaseLayout databaseLayout,
            Config additionalConfiguration,
            MemoryTracker memoryTracker) {
        try {
            return isRecoveryRequired(fs, databaseLayout, additionalConfiguration, memoryTracker);
        } catch (Exception e) {
            throw new CommandFailedException("Failure when checking for recovery state", e);
        }
    }

    private static void wrapIOException(IOException e) {
        throw new CommandFailedException(
                format("Unable to dump database%n%s: %s", e.getClass().getSimpleName(), e.getMessage()), e);
    }
}
