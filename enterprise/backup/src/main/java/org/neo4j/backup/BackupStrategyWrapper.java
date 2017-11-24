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

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.helpers.OptionalHostnamePort;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Individual backup strategies can perform incremental backups and full backups. The logic of how and when to perform full/incremental is identical.
 * This class describes the behaviour of a single strategy and is used to wrap an interface providing incremental/full backup functionality
 */
class BackupStrategyWrapper
{
    private final BackupStrategy backupStrategy;
    private final BackupCopyService backupCopyService;
    private final BackupRecoveryService backupRecoveryService;
    private final Log log;

    private final PageCache pageCache;
    private final Config config;

    BackupStrategyWrapper( BackupStrategy backupStrategy, BackupCopyService backupCopyService, PageCache pageCache, Config config,
            BackupRecoveryService backupRecoveryService, LogProvider logProvider )
    {
        this.backupStrategy = backupStrategy;
        this.backupCopyService = backupCopyService;
        this.pageCache = pageCache;
        this.config = config;
        this.backupRecoveryService = backupRecoveryService;
        this.log = logProvider.getLog( BackupStrategyWrapper.class );
    }

    /**
     * Try to do a backup using the given strategy (ex. BackupProtocol). This covers all stages (starting with incremental and falling back to a a full backup).
     * The end result of this method will either be a successful backup or any other return type with the reason why the backup wasn't successful
     *
     * @param onlineBackupContext the command line arguments, configuration, flags
     * @return the ultimate outcome of trying to do a backup with the given strategy
     */
    PotentiallyErroneousState<BackupStrategyOutcome> doBackup( OnlineBackupContext onlineBackupContext )
    {
        LifeSupport lifeSupport = new LifeSupport();
        lifeSupport.add( backupStrategy );
        lifeSupport.start();
        PotentiallyErroneousState<BackupStrategyOutcome> state = performBackupWithoutLifecycle( onlineBackupContext );
        lifeSupport.shutdown();
        return state;
    }

    private PotentiallyErroneousState<BackupStrategyOutcome> performBackupWithoutLifecycle( OnlineBackupContext onlineBackupContext )
    {
        File backupLocation = onlineBackupContext.getResolvedLocationFromName();
        final File userSpecifiedBackupLocation = onlineBackupContext.getResolvedLocationFromName();
        final OptionalHostnamePort userSpecifiedAddress = onlineBackupContext.getRequiredArguments().getAddress();
        final Config config = onlineBackupContext.getConfig();

        boolean previousBackupExists = backupCopyService.backupExists( backupLocation );
        if ( previousBackupExists )
        {
            log.info( "Previous backup found, trying incremental backup." );
            PotentiallyErroneousState<BackupStageOutcome> state =
                    backupStrategy.performIncrementalBackup( userSpecifiedBackupLocation, config, userSpecifiedAddress );
            boolean fullBackupWontWork = BackupStageOutcome.WRONG_PROTOCOL.equals( state.getState() );
            boolean incrementalWasSuccessful = BackupStageOutcome.SUCCESS.equals( state.getState() );

            if ( fullBackupWontWork || incrementalWasSuccessful )
            {
                backupCopyService.clearLogs( backupLocation );
                return describeOutcome( state );
            }
            if ( !onlineBackupContext.getRequiredArguments().isFallbackToFull() )
            {
                return describeOutcome( state );
            }
        }
        if ( onlineBackupContext.getRequiredArguments().isFallbackToFull() )
        {
            if ( !previousBackupExists )
            {
                log.info( "Previous backup not found, a new full backup will be performed." );
            }
            return describeOutcome( fullBackupWithTemporaryFolderResolutions( onlineBackupContext ) );
        }
        return new PotentiallyErroneousState<>( BackupStrategyOutcome.INCORRECT_STRATEGY, null );
    }

    /**
     * This will perform a full backup with some directory renaming if necessary.
     * <p>
     * If there is no existing backup, then no renaming will occur.
     * Otherwise the full backup will be done into a temporary directory and renaming
     * will occur if everything was successful.
     * </p>
     *
     * @param onlineBackupContext command line arguments, config etc.
     * @return outcome of full backup
     */
    private PotentiallyErroneousState<BackupStageOutcome> fullBackupWithTemporaryFolderResolutions( OnlineBackupContext onlineBackupContext )
    {
        final File userSpecifiedBackupLocation = onlineBackupContext.getResolvedLocationFromName();
        File temporaryFullBackupLocation = backupCopyService.findAnAvailableLocationForNewFullBackup( userSpecifiedBackupLocation );
        PotentiallyErroneousState<BackupStageOutcome> state = backupStrategy.performFullBackup( temporaryFullBackupLocation, onlineBackupContext.getConfig(),
                onlineBackupContext.getRequiredArguments().getAddress() );

        boolean aBackupAlreadyExisted =
                userSpecifiedBackupLocation.equals( temporaryFullBackupLocation ); // NOTE temporaryFullBackupLocation can be equal to desired

        if ( BackupStageOutcome.SUCCESS.equals( state.getState() ) )
        {
            backupRecoveryService.recoverWithDatabase( temporaryFullBackupLocation, pageCache, config );
            if ( !aBackupAlreadyExisted )
            {
                try
                {
                    renameTemporaryBackupToExpected( temporaryFullBackupLocation, userSpecifiedBackupLocation );
                }
                catch ( CommandFailed commandFailed )
                {
                    return new PotentiallyErroneousState<>( BackupStageOutcome.UNRECOVERABLE_FAILURE, commandFailed );
                }
            }
            backupCopyService.clearLogs( userSpecifiedBackupLocation );
        }
        return state;
    }

    private void renameTemporaryBackupToExpected( File temporaryFullBackupLocation, File userSpecifiedBackupLocation ) throws CommandFailed
    {
        File newBackupLocationForPreExistingBackup = backupCopyService.findNewBackupLocationForBrokenExisting( userSpecifiedBackupLocation );
        backupCopyService.moveBackupLocation( userSpecifiedBackupLocation, newBackupLocationForPreExistingBackup );
        backupCopyService.moveBackupLocation( temporaryFullBackupLocation, userSpecifiedBackupLocation );
    }

    private PotentiallyErroneousState<BackupStrategyOutcome> describeOutcome( PotentiallyErroneousState<BackupStageOutcome> strategyStageOutcome )
    {
        BackupStageOutcome stageOutcome = strategyStageOutcome.getState();
        if ( stageOutcome == BackupStageOutcome.SUCCESS )
        {
            return new PotentiallyErroneousState<>( BackupStrategyOutcome.SUCCESS, null );
        }
        if ( stageOutcome == BackupStageOutcome.WRONG_PROTOCOL )
        {
            return new PotentiallyErroneousState<>( BackupStrategyOutcome.INCORRECT_STRATEGY, strategyStageOutcome.getCause().orElse( null ) );
        }
        if ( stageOutcome == BackupStageOutcome.FAILURE )
        {
            return new PotentiallyErroneousState<>( BackupStrategyOutcome.CORRECT_STRATEGY_FAILED, strategyStageOutcome.getCause().orElse( null ) );
        }
        if ( stageOutcome == BackupStageOutcome.UNRECOVERABLE_FAILURE )
        {
            return new PotentiallyErroneousState<>( BackupStrategyOutcome.ABSOLUTE_FAILURE, strategyStageOutcome.getCause().orElse( null ) );
        }
        throw new RuntimeException( "Not all enums covered: " + stageOutcome );
    }
}
