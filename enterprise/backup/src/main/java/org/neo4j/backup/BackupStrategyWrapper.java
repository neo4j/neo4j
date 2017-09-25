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
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifeSupport;

class BackupStrategyWrapper
{
    private final BackupStrategy backupStrategy;
    private final BackupCopyService backupCopyService;

    BackupStrategyWrapper( BackupStrategy backupStrategy, BackupCopyService backupCopyService )
    {
        this.backupStrategy = backupStrategy;
        this.backupCopyService = backupCopyService;
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
        PotentiallyErroneousState<BackupStageOutcome> state;
        final File userSpecifiedBackupLocation = onlineBackupContext.getResolvedLocationFromName();
        final OptionalHostnamePort userSpecifiedAddress = onlineBackupContext.getRequiredArguments().getAddress();
        final Config config = onlineBackupContext.getConfig();
        if ( backupCopyService.backupExists( backupLocation ) )
        {
            state = backupStrategy.performIncrementalBackup( userSpecifiedBackupLocation, config, userSpecifiedAddress );
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

    private PotentiallyErroneousState<BackupStageOutcome> fullBackupWithTemporaryFolderResolutions( OnlineBackupContext onlineBackupContext )
    {
        final File userSpecifiedBackupLocation = onlineBackupContext.getResolvedLocationFromName();
        File temporaryFullBackupLocation = backupCopyService.findAnAvailableLocationForNewFullBackup( userSpecifiedBackupLocation );
        PotentiallyErroneousState<BackupStageOutcome> state =
                backupStrategy.performFullBackup( temporaryFullBackupLocation, onlineBackupContext.getConfig(),
                        onlineBackupContext.getRequiredArguments().getAddress() );

        boolean aBackupAlreadyExisted = userSpecifiedBackupLocation.equals( temporaryFullBackupLocation );
        if ( BackupStageOutcome.SUCCESS.equals( state.getState() ) && !aBackupAlreadyExisted )
        {
            File newBackupLocationForPreExistingBackup = backupCopyService.findNewBackupLocationForBrokenExisting( userSpecifiedBackupLocation );
            try
            {
                backupCopyService.moveBackupLocation( userSpecifiedBackupLocation, newBackupLocationForPreExistingBackup );
                backupCopyService.moveBackupLocation( temporaryFullBackupLocation, userSpecifiedBackupLocation );
            }
            catch ( CommandFailed commandFailed )
            {
                return new PotentiallyErroneousState<>( BackupStageOutcome.UNRECOVERABLE_FAILURE, commandFailed );
            }
        }
        return state;
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
