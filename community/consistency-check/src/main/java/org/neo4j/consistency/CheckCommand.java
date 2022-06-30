/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency;

import static org.neo4j.kernel.recovery.Recovery.isRecoveryRequired;
import static picocli.CommandLine.ArgGroup;
import static picocli.CommandLine.Command;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import org.neo4j.cli.AbstractAdminCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.Converters.DatabaseNameConverter;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.cli.ExitCode;
import org.neo4j.commandline.Util;
import org.neo4j.commandline.dbms.CannotWriteException;
import org.neo4j.commandline.dbms.LockChecker;
import org.neo4j.configuration.Config;
import org.neo4j.consistency.checking.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.ConsistencyFlags;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.locker.FileLockException;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.VisibleForTesting;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

@Command(
        name = "check",
        header = "Check the consistency of a database.",
        description =
                """
                This command allows for checking the consistency of a database or a backup thereof.
                It cannot be used with a database which is currently in use.

                All checks except 'check-graph' can be quite expensive so it may be useful to turn them off
                for very large databases. Increasing the heap size can also be a good idea.
                See 'neo4j-admin help' for details.""")
public class CheckCommand extends AbstractAdminCommand {
    private final ConsistencyCheckService consistencyCheckService;

    @ArgGroup(multiplicity = "1")
    private TargetOption target = new TargetOption();

    private static class TargetOption {
        @Option(
                names = "--database",
                description = "Name of the database to check.",
                converter = DatabaseNameConverter.class)
        private NormalizedDatabaseName database;

        @Option(
                names = "--backup",
                paramLabel = "<path>",
                description = "Path to backup to check consistency of. Cannot be used together with --database.")
        private Path backup;
    }

    @Mixin
    private ConsistencyCheckOptions options;

    protected Config config;
    private ConsistencyFlags flags;

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

        if (target.backup != null) {
            target.backup = target.backup.toAbsolutePath();

            if (!ctx.fs().isDirectory(target.backup)) {
                throw new CommandFailedException(
                        "Report directory path doesn't exist or not a directory: " + target.backup);
            }
        }
    }

    protected Config.Builder configBuilder() {
        return createPrefilledConfigBuilder();
    }

    protected ConsistencyCheckService.Result checkWith(Config config, MemoryTracker memoryTracker) {
        final var layout = Optional.ofNullable(target.backup) // Consistency checker only supports Record format for now
                .map(RecordDatabaseLayout::ofFlat)
                .orElseGet(() -> RecordDatabaseLayout.of(Neo4jLayout.of(config), target.database.name()));

        checkDatabaseExistence(layout);
        try (var ignored = LockChecker.checkDatabaseLock(layout)) {
            checkDbState(ctx.fs(), layout, config, memoryTracker);
            // Only output progress indicator if a console receives the output
            final var processOut = System.console() != null ? System.out : null;

            try (var logProvider = Util.configuredLogProvider(ctx.out(), verbose)) {
                return consistencyCheckService
                        .with(layout)
                        .with(config)
                        .with(processOut)
                        .with(logProvider)
                        .with(ctx.fs())
                        .verbose(verbose)
                        .with(options.getReportDir().normalize())
                        .with(flags)
                        .runFullConsistencyCheck();
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
        } catch (IOException e) {
            throw new CommandFailedException("Consistency checking failed. " + e.getMessage(), e, ExitCode.IOERR);
        }
    }

    private static void checkDatabaseExistence(DatabaseLayout layout) {
        try {
            Validators.CONTAINS_EXISTING_DATABASE.validate(layout.databaseDirectory());
        } catch (IllegalArgumentException e) {
            throw new CommandFailedException(
                    "Database does not exist: " + layout.getDatabaseName(), e, ExitCode.NOINPUT);
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
