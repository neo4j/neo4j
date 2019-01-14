/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.OptionalHostnamePort;
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
    Fallible<BackupStrategyOutcome> doBackup( OnlineBackupContext onlineBackupContext )
    {
        LifeSupport lifeSupport = new LifeSupport();
        lifeSupport.add( backupStrategy );
        lifeSupport.start();
        Fallible<BackupStrategyOutcome> state = performBackupWithoutLifecycle( onlineBackupContext );
        lifeSupport.shutdown();
        return state;
    }

    private Fallible<BackupStrategyOutcome> performBackupWithoutLifecycle( OnlineBackupContext onlineBackupContext )
    {
        Path backupLocation = onlineBackupContext.getResolvedLocationFromName();
        OptionalHostnamePort userSpecifiedAddress = onlineBackupContext.getRequiredArguments().getAddress();
        log.debug( "User specified address is %s:%s", userSpecifiedAddress.getHostname().toString(), userSpecifiedAddress.getPort().toString() );
        Config config = onlineBackupContext.getConfig();

        boolean previousBackupExists = backupCopyService.backupExists( backupLocation );
        if ( previousBackupExists )
        {
            log.info( "Previous backup found, trying incremental backup." );
            Fallible<BackupStageOutcome> state = backupStrategy.performIncrementalBackup( backupLocation, config, userSpecifiedAddress );
            boolean fullBackupWontWork = BackupStageOutcome.WRONG_PROTOCOL.equals( state.getState() );
            boolean incrementalWasSuccessful = BackupStageOutcome.SUCCESS.equals( state.getState() );
            if ( incrementalWasSuccessful )
            {
                backupRecoveryService.recoverWithDatabase( backupLocation, pageCache, config );
            }

            if ( fullBackupWontWork || incrementalWasSuccessful )
            {
                clearIdFiles( backupLocation );
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
        return new Fallible<>( BackupStrategyOutcome.INCORRECT_STRATEGY, null );
    }

    private void clearIdFiles( Path backupLocation )
    {
        try
        {
            backupCopyService.clearIdFiles( backupLocation );
        }
        catch ( IOException e )
        {
            log.warn( "Failed to delete some or all id files.", e );
        }
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
    private Fallible<BackupStageOutcome> fullBackupWithTemporaryFolderResolutions( OnlineBackupContext onlineBackupContext )
    {
        Path userSpecifiedBackupLocation = onlineBackupContext.getResolvedLocationFromName();
        Path temporaryFullBackupLocation = backupCopyService.findAnAvailableLocationForNewFullBackup( userSpecifiedBackupLocation );

        OptionalHostnamePort address = onlineBackupContext.getRequiredArguments().getAddress();
        Fallible<BackupStageOutcome> state = backupStrategy.performFullBackup( temporaryFullBackupLocation, config, address );

        // NOTE temporaryFullBackupLocation can be equal to desired
        boolean backupWasMadeToATemporaryLocation = !userSpecifiedBackupLocation.equals( temporaryFullBackupLocation );

        if ( BackupStageOutcome.SUCCESS.equals( state.getState() ) )
        {
            backupRecoveryService.recoverWithDatabase( temporaryFullBackupLocation, pageCache, config );
            if ( backupWasMadeToATemporaryLocation )
            {
                try
                {
                    renameTemporaryBackupToExpected( temporaryFullBackupLocation, userSpecifiedBackupLocation );
                }
                catch ( IOException e )
                {
                    return new Fallible<>( BackupStageOutcome.UNRECOVERABLE_FAILURE, e );
                }
            }
            clearIdFiles( userSpecifiedBackupLocation );
        }
        return state;
    }

    private void renameTemporaryBackupToExpected( Path temporaryFullBackupLocation, Path userSpecifiedBackupLocation ) throws IOException
    {
        Path newBackupLocationForPreExistingBackup = backupCopyService.findNewBackupLocationForBrokenExisting( userSpecifiedBackupLocation );
        backupCopyService.moveBackupLocation( userSpecifiedBackupLocation, newBackupLocationForPreExistingBackup );
        backupCopyService.moveBackupLocation( temporaryFullBackupLocation, userSpecifiedBackupLocation );
    }

    private Fallible<BackupStrategyOutcome> describeOutcome( Fallible<BackupStageOutcome> strategyStageOutcome )
    {
        BackupStageOutcome stageOutcome = strategyStageOutcome.getState();
        if ( stageOutcome == BackupStageOutcome.SUCCESS )
        {
            return new Fallible<>( BackupStrategyOutcome.SUCCESS, null );
        }
        if ( stageOutcome == BackupStageOutcome.WRONG_PROTOCOL )
        {
            return new Fallible<>( BackupStrategyOutcome.INCORRECT_STRATEGY, strategyStageOutcome.getCause().orElse( null ) );
        }
        if ( stageOutcome == BackupStageOutcome.FAILURE )
        {
            return new Fallible<>( BackupStrategyOutcome.CORRECT_STRATEGY_FAILED, strategyStageOutcome.getCause().orElse( null ) );
        }
        if ( stageOutcome == BackupStageOutcome.UNRECOVERABLE_FAILURE )
        {
            return new Fallible<>( BackupStrategyOutcome.ABSOLUTE_FAILURE, strategyStageOutcome.getCause().orElse( null ) );
        }
        throw new RuntimeException( "Not all enums covered: " + stageOutcome );
    }
}
