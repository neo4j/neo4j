/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.backup.impl;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

/**
 * Controls the outcome of the backup tool.
 * Iterates over multiple backup strategies and stops when a backup was successful, there was a critical failure or
 * when none of the backups worked.
 * Also handles the consistency check
 */
class BackupStrategyCoordinator
{
    private static final int STATUS_CC_ERROR = 2;
    private static final int STATUS_CC_INCONSISTENT = 3;

    private final ConsistencyCheckService consistencyCheckService;
    private final ForceCheckpointBackupService forceCheckpointBackupService;
    private final OutsideWorld outsideWorld;
    private final LogProvider logProvider;
    private final ProgressMonitorFactory progressMonitorFactory;
    private final List<BackupStrategyWrapper> strategies;

    BackupStrategyCoordinator( ConsistencyCheckService consistencyCheckService, ForceCheckpointBackupService forceCheckpointBackupService,
            OutsideWorld outsideWorld, LogProvider logProvider, ProgressMonitorFactory progressMonitorFactory, List<BackupStrategyWrapper> strategies )
    {
        this.consistencyCheckService = consistencyCheckService;
        this.forceCheckpointBackupService = forceCheckpointBackupService;
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
    public void performBackup( OnlineBackupContext onlineBackupContext ) throws CommandFailed
    {
        // Convenience
        OnlineBackupRequiredArguments requiredArgs = onlineBackupContext.getRequiredArguments();
        Path destination = onlineBackupContext.getResolvedLocationFromName();
        ConsistencyFlags consistencyFlags = onlineBackupContext.getConsistencyFlags();

        Fallible<BackupStrategyOutcome> throwableWithState = null;
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
        removeUnnecessaryTransactionLogs( destination.toFile(), outsideWorld.fileSystem() );
        if ( requiredArgs.isDoConsistencyCheck() )
        {
            performConsistencyCheck( onlineBackupContext.getConfig(), requiredArgs, consistencyFlags, destination );
        }
    }

    private void removeUnnecessaryTransactionLogs( File backupDirectory, FileSystemAbstraction fileSystemAbstraction )
    {
        LifeSupport lifeSupport = new LifeSupport();
        lifeSupport.add( forceCheckpointBackupService );

        lifeSupport.init();
        lifeSupport.start();
        forceCheckpointBackupService.forceRotation();
        forceCheckpointBackupService.forceCheckpoint();
        forceCheckpointBackupService.forcePrune( );
        lifeSupport.stop();
        lifeSupport.shutdown();
    }

    private static Supplier<CommandFailed> commandFailedWithCause( Fallible<BackupStrategyOutcome> cause )
    {
        if ( cause.getCause().isPresent() )
        {
            return () -> new CommandFailed( "Execution of backup failed", cause.getCause().get() );
        }
        return () -> new CommandFailed( "Execution of backup failed" );
    }

    private void performConsistencyCheck(
            Config config, OnlineBackupRequiredArguments requiredArgs, ConsistencyFlags consistencyFlags,
            Path destination ) throws CommandFailed
    {
        try
        {
            File storeDir = destination.toFile();
            boolean verbose = false;
            File reportDir = requiredArgs.getReportDir().toFile();
            ConsistencyCheckService.Result ccResult = consistencyCheckService.runFullConsistencyCheck(
                    storeDir,
                    config,
                    progressMonitorFactory,
                    logProvider,
                    outsideWorld.fileSystem(),
                    verbose,
                    reportDir,
                    consistencyFlags );

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
