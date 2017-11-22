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

import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.helpers.OptionalHostnamePort;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifeSupport;

class BackupStrategyWrapper
{
    private final BackupStrategy backupStrategy;
    private final BackupCopyService backupCopyService;
    private final BackupRecoveryService backupRecoveryService;

    private final PageCache pageCache;
    private final Config config;

    BackupStrategyWrapper( BackupStrategy backupStrategy, BackupCopyService backupCopyService, PageCache pageCache, Config config,
            BackupRecoveryService backupRecoveryService )
    {
        this.backupStrategy = backupStrategy;
        this.backupCopyService = backupCopyService;
        this.pageCache = pageCache;
        this.config = config;
        this.backupRecoveryService = backupRecoveryService;
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

    private Fallible<BackupStrategyOutcome> performBackupWithoutLifecycle(
            OnlineBackupContext onlineBackupContext )
    {
        Path backupLocation = onlineBackupContext.getResolvedLocationFromName();
        Path userSpecifiedBackupLocation = onlineBackupContext.getResolvedLocationFromName();
        OptionalHostnamePort userSpecifiedAddress = onlineBackupContext.getRequiredArguments().getAddress();
        Config config = onlineBackupContext.getConfig();

        if ( backupCopyService.backupExists( backupLocation ) )
        {
            Fallible<BackupStageOutcome> state =
                    backupStrategy.performIncrementalBackup( userSpecifiedBackupLocation, config, userSpecifiedAddress );
            boolean fullBackupWontWork = BackupStageOutcome.WRONG_PROTOCOL.equals( state.getState() );
            boolean incrementalWasSuccessful = BackupStageOutcome.SUCCESS.equals( state.getState() );

            if ( fullBackupWontWork || incrementalWasSuccessful )
            {
                return describeOutcome( state );
            }
            if ( !onlineBackupContext.getRequiredArguments().isFallbackToFull() )
            {
                return describeOutcome( state );
            }
        }
        return describeOutcome( fullBackupWithTemporaryFolderResolutions( onlineBackupContext ) );
    }

    private Fallible<BackupStageOutcome> fullBackupWithTemporaryFolderResolutions(
            OnlineBackupContext onlineBackupContext )
    {
        Path userSpecifiedBackupLocation = onlineBackupContext.getResolvedLocationFromName();
        Path temporaryFullBackupLocation;
        try
        {
            temporaryFullBackupLocation = backupCopyService.findAnAvailableLocationForNewFullBackup( userSpecifiedBackupLocation );
        }
        catch ( IOException e )
        {
            return new Fallible<>( BackupStageOutcome.UNRECOVERABLE_FAILURE, e );
        }

        Config config = onlineBackupContext.getConfig();
        OptionalHostnamePort address = onlineBackupContext.getRequiredArguments().getAddress();
        Fallible<BackupStageOutcome> state = backupStrategy.performFullBackup( temporaryFullBackupLocation, config, address );

        // NOTE temporaryFullBackupLocation can be equal to desired
        boolean aBackupAlreadyExisted = userSpecifiedBackupLocation.equals( temporaryFullBackupLocation );

        if ( BackupStageOutcome.SUCCESS.equals( state.getState() ) )
        {
            backupRecoveryService.recoverWithDatabase( temporaryFullBackupLocation, pageCache, this.config );
            if ( !aBackupAlreadyExisted )
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
