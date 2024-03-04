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
package org.neo4j.consistency;

import static org.neo4j.kernel.recovery.Recovery.isRecoveryRequired;
import static picocli.CommandLine.Command;
import static picocli.CommandLine.Help.Visibility.NEVER;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import org.neo4j.cli.AbstractAdminCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.Converters.DatabaseNameConverter;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.cli.ExitCode;
import org.neo4j.cli.PathOptions;
import org.neo4j.commandline.Util;
import org.neo4j.commandline.dbms.CannotWriteException;
import org.neo4j.commandline.dbms.LockChecker;
import org.neo4j.configuration.Config;
import org.neo4j.consistency.ConsistencyCheckService.Result;
import org.neo4j.consistency.checking.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.ConsistencyFlags;
import org.neo4j.dbms.archive.CheckDatabase;
import org.neo4j.dbms.archive.CheckDatabase.Source;
import org.neo4j.dbms.archive.CheckDatabase.Source.DataTxnSource;
import org.neo4j.dbms.archive.CheckDatabase.Source.PathSource;
import org.neo4j.io.IOUtils.AutoCloseables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.locker.FileLockException;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.kernel.impl.util.TransactionLogChecker;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.VisibleForTesting;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(
        name = "check",
        header = "Check the consistency of a database.",
        description =
                """
                This command allows for checking the consistency of a database, or a dump or backup thereof.
                It cannot be used with a database which is currently in use.

                Some checks can be quite expensive, so it may be useful to turn some of them off
                for very large databases. Increasing the heap size can also be a good idea.
                See 'neo4j-admin help' for details.""",
        sortOptions = false)
public class CheckCommand extends AbstractAdminCommand {
    private final ConsistencyCheckService consistencyCheckService;

    @Parameters(index = "0", description = "Name of the database to check.", converter = DatabaseNameConverter.class)
    private NormalizedDatabaseName database;

    @Option(
            names = "--force",
            fallbackValue = "true",
            description = "Force a consistency check to be run, despite resources, and may run a more thorough check.")
    private boolean force;

    @Option(
            names = "--check-tx-logs",
            fallbackValue = "false",
            hidden = true,
            description = "Hidden option to be used by upgrade-tests. "
                    + "Checks that the transaction log content and headers are correct in respect to version changes "
                    + "and rotations.")
    private boolean additionalTxLogCheck;

    @Mixin
    private ConsistencyCheckOptions options;

    @ArgGroup
    private SourceOptions sourceOptions;

    protected Config config;
    private ConsistencyFlags flags;
    private Source source;

    private static final class SourceOptions {
        @ArgGroup(exclusive = false)
        private PathOptions.SourceOptions sourceOptions;

        @ArgGroup(exclusive = false)
        private FromAndTemp fromAndTemp;

        public Source toSource() {
            return fromAndTemp != null
                    ? new PathSource(fromAndTemp.fromPath(), fromAndTemp.tempPath())
                    : new DataTxnSource(sourceOptions.dataPath(), sourceOptions.txnPath());
        }

        private static final class FromAndTemp {
            @Option(
                    names = "--from-path",
                    paramLabel = "<path>",
                    required = true,
                    description =
                            "Path to the directory containing dump/backup artifacts that need to be checked for consistency. "
                                    + "If the directory contains multiple backups, it will select the most recent backup chain, "
                                    + "based on the transaction IDs found, to perform the consistency check. ")
            private Path fromPath;

            public Path fromPath() {
                return fromPath.toAbsolutePath().normalize();
            }

            @Option(
                    names = "--temp-path",
                    paramLabel = "<path>",
                    showDefaultValue = NEVER, // manually handled
                    description = "Path to directory to be used as a staging area to extract dump/backup artifacts, "
                            + "if needed.%n  Default:  <from-path>")
            private Path tempPath;

            public Path tempPath() {
                return tempPath != null ? tempPath.toAbsolutePath().normalize() : fromPath();
            }
        }
    }

    public CheckCommand(ExecutionContext ctx) {
        this(ctx, new ConsistencyCheckService(null));
    }

    @VisibleForTesting
    public CheckCommand(ExecutionContext ctx, ConsistencyCheckService consistencyCheckService) {
        super(ctx);
        this.consistencyCheckService = consistencyCheckService;
    }

    @Override
    protected Optional<String> commandConfigName() {
        return Optional.of("database-check");
    }

    @Override
    public void execute() {
        validateAndConstructArgs();

        final var result = checkWith(config, EmptyMemoryTracker.INSTANCE);
        if (!result.isSuccessful()) {
            throw new CommandFailedException(
                    "Inconsistencies found. See '%s' for details.".formatted(result.reportFile()), ExitCode.FAIL);
        }
    }

    protected void validateAndConstructArgs() {
        config = configBuilder().build();
        flags = options.toFlags();
        source = sourceOptions != null ? sourceOptions.toSource() : new DataTxnSource(config);
    }

    protected Config.Builder configBuilder() {
        return createPrefilledConfigBuilder();
    }

    protected Result checkWith(Config config, MemoryTracker memoryTracker) {
        try (var autoClosables = new AutoCloseables<>(IOException::new);
                var logProvider = Util.configuredLogProvider(ctx.out(), verbose)) {
            final DatabaseLayout layout;
            try {
                layout = CheckDatabase.selectAndExtract(
                        ctx.fs(), source, database, logProvider, verbose, config, force, autoClosables);
            } catch (IOException e) {
                throw new CommandFailedException(
                        "Failed to prepare for consistency check: " + e.getMessage(), e, ExitCode.IOERR);
            } catch (UnsupportedOperationException e) {
                throw new CommandFailedException(e.getMessage(), ExitCode.USAGE);
            } catch (Exception e) {
                throw new CommandFailedException(
                        "Failed to prepare for consistency check: " + e.getMessage(), e, ExitCode.SOFTWARE);
            }

            try (var ignored = LockChecker.checkDatabaseLock(layout)) {
                checkDbState(ctx.fs(), layout, config, memoryTracker);
                try {
                    Result result = consistencyCheckService
                            .with(layout)
                            .with(config)
                            .with(ctx.out())
                            .with(logProvider)
                            .with(ctx.fs())
                            .verbose(verbose)
                            .with(options.reportPath())
                            .with(flags)
                            .withMaxOffHeapMemory(options.maxOffHeapMemory())
                            .withNumberOfThreads(options.numberOfThreads())
                            .runFullConsistencyCheck();

                    if (result.isSuccessful() && additionalTxLogCheck) {
                        TransactionLogChecker.verifyCorrectTransactionLogUpgrades(ctx.fs(), layout);
                    }
                    return result;
                } catch (ConsistencyCheckIncompleteException e) {
                    throw new CommandFailedException(
                            "Consistency checking failed. " + e.getMessage(), e, ExitCode.SOFTWARE);
                }

            } catch (FileLockException e) {
                throw new CommandFailedException(
                        "The database is in use. Stop database '%s' and try again.".formatted(layout.getDatabaseName()),
                        e,
                        ExitCode.FAIL);
            } catch (CannotWriteException e) {
                throw new CommandFailedException(
                        "You do not have permission to check database consistency.", e, ExitCode.NOPERM);
            }

        } catch (CommandFailedException e) {
            throw e;
        } catch (Exception e) {
            throw new CommandFailedException("Consistency checking failed. " + e.getMessage(), e, ExitCode.SOFTWARE);
        }
    }

    private static void checkDbState(
            FileSystemAbstraction fs,
            DatabaseLayout databaseLayout,
            Config additionalConfiguration,
            MemoryTracker memoryTracker) {

        if (checkRecoveryState(fs, databaseLayout, additionalConfiguration, memoryTracker)) {
            throw new CommandFailedException(
                    """
                    Active logical log detected, this might be a source of inconsistencies.
                    Please recover database before running the consistency check.
                    To perform recovery please start database and perform clean shutdown.""",
                    ExitCode.FAIL);
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
            throw new CommandFailedException(
                    "Failure when checking for recovery state: " + e.getMessage(), e, ExitCode.IOERR);
        }
    }
}
