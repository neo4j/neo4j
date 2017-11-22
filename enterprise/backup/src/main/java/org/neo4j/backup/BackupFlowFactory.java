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

import java.util.Arrays;
import java.util.List;

import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;

/**
 * Backup flows are iterate through backup strategies and make sure at least one of them is a valid backup. Handles
 * cases when that isn't possible.
 * This factory helps in the construction of them.
 */
class BackupFlowFactory
{
    private final LogProvider logProvider;
    private final ConsistencyCheckService consistencyCheckService;
    private final AddressResolver addressResolver;
    private final BackupCopyService backupCopyService;
    private final OutsideWorld outsideWorld;

    BackupFlowFactory( BackupModule backupModule )
    {
        this.logProvider = backupModule.getLogProvider();
        this.outsideWorld = backupModule.getOutsideWorld();
        this.backupCopyService = backupModule.getBackupCopyService();

        this.consistencyCheckService = new ConsistencyCheckService();
        this.addressResolver = new AddressResolver();
    }

    BackupFlow backupFlow( OnlineBackupContext onlineBackupContext, BackupProtocolService backupProtocolService,
                           BackupDelegator backupDelegator, PageCache pageCache )
    {
        ProgressMonitorFactory progressMonitorFactory = ProgressMonitorFactory.textual( outsideWorld.errorStream() );
        BackupRecoveryService recoveryService = new BackupRecoveryService();
        long timeout = onlineBackupContext.getRequiredArguments().getTimeout();
        Config config = onlineBackupContext.getConfig();

        BackupStrategy ccStrategy = new CausalClusteringBackupStrategy( backupDelegator, addressResolver );
        BackupStrategy haStrategy = new HaBackupStrategy( backupProtocolService, addressResolver, timeout );

        List<BackupStrategyWrapper> strategies = Arrays.asList(
                wrap( ccStrategy, pageCache, config, recoveryService ),
                wrap( haStrategy, pageCache, config, recoveryService ) );

        return new BackupFlow( consistencyCheckService, outsideWorld, logProvider, progressMonitorFactory, strategies );
    }

    private BackupStrategyWrapper wrap( BackupStrategy strategy, PageCache pageCache, Config config,
                                        BackupRecoveryService recoveryService )
    {
        return new BackupStrategyWrapper( strategy, backupCopyService, pageCache, config, recoveryService ) ;
    }
}
