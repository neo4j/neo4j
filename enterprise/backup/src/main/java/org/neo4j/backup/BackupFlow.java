/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.backup;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

/**
 * Controls the outcome of the backup tool.
 * Iterates over multiple backup strategies and stops when a backup was successful, there was a critical failure or
 * when none of the backups worked.
 * Also handles the consistency check
 */
class BackupFlow
{
    private static final int STATUS_CC_ERROR = 2;
    private static final int STATUS_CC_INCONSISTENT = 3;

    private ConsistencyCheckService consistencyCheckService;
    private final OutsideWorld outsideWorld;
    private final LogProvider logProvider;
    private final ProgressMonitorFactory progressMonitorFactory;
    private final List<BackupStrategyWrapper> strategies;

    BackupFlow( ConsistencyCheckService consistencyCheckService, OutsideWorld outsideWorld, LogProvider logProvider,
            ProgressMonitorFactory progressMonitorFactory, List<BackupStrategyWrapper> strategies )
    {
        this.consistencyCheckService = consistencyCheckService;
        this.outsideWorld = outsideWorld;
        this.logProvider = logProvider;
        this.progressMonitorFactory = progressMonitorFactory;
        this.strategies = strategies;
    }

    /**
     * Iterate over all the provided strategies trying to perform a successful backup.
     * Will also do consistency checks if specified in {@link OnlineBackupContext}
     *
     * @param onlineBackupContext filesystem, command arguments and configuration
     * @throws CommandFailed when backup failed or there were issues with consistency checks
     */
    void performBackup( OnlineBackupContext onlineBackupContext ) throws CommandFailed
    {
        // Convenience
        OnlineBackupRequiredArguments requiredArgs = onlineBackupContext.getRequiredArguments();
        File destination = onlineBackupContext.getResolvedLocationFromName();
        ConsistencyFlags consistencyFlags = onlineBackupContext.getConsistencyFlags();

        PotentiallyErroneousState<BackupStrategyOutcome> throwableWithState = null;
        List<Throwable> causesOfFailure = new ArrayList<>();
        for ( BackupStrategyWrapper backupStrategy : strategies )
        {
            throwableWithState = backupStrategy.doBackup( onlineBackupContext );
            if ( throwableWithState.getState() == BackupStrategyOutcome.SUCCESS )
            {
                break;
            }
            if ( throwableWithState.getState() == BackupStrategyOutcome.CORRECT_STRATEGY_FAILED )
            {
                throw commandFailedWithCause( throwableWithState ).get();
            }
            throwableWithState.getCause().ifPresent( causesOfFailure::add );
        }
        if ( throwableWithState == null || !BackupStrategyOutcome.SUCCESS.equals( throwableWithState.getState() ) )
        {
            CommandFailed commandFailed = new CommandFailed( "Failed to run a backup using the available strategies." );
            causesOfFailure.forEach( commandFailed::addSuppressed );
            throw commandFailed;
        }
        if ( requiredArgs.isDoConsistencyCheck() )
        {
            performConsistencyCheck( onlineBackupContext.getConfig(), requiredArgs, consistencyFlags, destination );
        }
    }

    private static Supplier<CommandFailed> commandFailedWithCause( PotentiallyErroneousState<BackupStrategyOutcome> cause )
    {
        if ( cause.getCause().isPresent() )
        {
            return () -> new CommandFailed( "Execution of backup failed", cause.getCause().get() );
        }
        return () -> new CommandFailed( "Execution of backup failed" );
    }

    private void performConsistencyCheck( Config config, OnlineBackupRequiredArguments requiredArgs, ConsistencyFlags consistencyFlags, File destination )
            throws CommandFailed
    {
        try
        {
            ConsistencyCheckService.Result ccResult =
                    consistencyCheckService.runFullConsistencyCheck( destination, config, progressMonitorFactory, logProvider, outsideWorld.fileSystem(), false,
                            requiredArgs.getReportDir().toFile(), consistencyFlags );

            if ( !ccResult.isSuccessful() )
            {
                throw new CommandFailed( format( "Inconsistencies found. See '%s' for details.", ccResult.reportFile() ), STATUS_CC_INCONSISTENT );
            }
        }
        catch ( Throwable e )
        {
            if ( e instanceof CommandFailed )
            {
                throw (CommandFailed) e;
            }
            throw new CommandFailed( "Failed to do consistency check on backup: " + e.getMessage(), e, STATUS_CC_ERROR );
        }
    }
}
