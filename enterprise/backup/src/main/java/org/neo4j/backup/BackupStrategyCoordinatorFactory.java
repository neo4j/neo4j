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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.com.storecopy.FileMoveProvider;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.logging.LogProvider;

/*
 * Backup strategy coordinators iterate through backup strategies and make sure at least one of them can perform a valid backup.
 * Handles cases when individual backups aren't  possible.
 */
class BackupStrategyCoordinatorFactory
{
    private final LogProvider logProvider;
    private final ConsistencyCheckService consistencyCheckService;
    private final AddressResolutionHelper addressResolutionHelper;
    private final OutsideWorld outsideWorld;

    BackupStrategyCoordinatorFactory( BackupModuleResolveAtRuntime backupModuleResolveAtRuntime )
    {
        this.logProvider = backupModuleResolveAtRuntime.getLogProvider();
        this.outsideWorld = backupModuleResolveAtRuntime.getOutsideWorld();

        this.consistencyCheckService = new ConsistencyCheckService();
        this.addressResolutionHelper = new AddressResolutionHelper();
    }

    /**
     * Construct a wrapper of supported backup strategies
     *
     * @param onlineBackupContext the input of the backup tool, such as CLI arguments, config etc.
     * @param backupProtocolService the underlying backup implementation for HA and single node instances
     * @param backupDelegator the backup implementation used for CC backups
     * @param pageCache the page cache used moving files
     * @return strategy coordinator that handles the which backup strategies are tried and establishes if a backup was successful or not
     */
    BackupStrategyCoordinator backupStrategyCoordinator( OnlineBackupContext onlineBackupContext, BackupProtocolService backupProtocolService,
            BackupDelegator backupDelegator, PageCache pageCache )
    {
        BackupCopyService backupCopyService = new BackupCopyService( pageCache, new FileMoveProvider( pageCache ) );
        ProgressMonitorFactory progressMonitorFactory = ProgressMonitorFactory.textual( outsideWorld.errorStream() );

        List<BackupStrategyWrapper> strategies = Stream.of( new CausalClusteringBackupStrategy( backupDelegator, addressResolutionHelper ),
                new HaBackupStrategy( backupProtocolService, addressResolutionHelper, onlineBackupContext.getRequiredArguments().getTimeout() ) )
                .map( strategy -> new BackupStrategyWrapper( strategy, backupCopyService, pageCache, onlineBackupContext.getConfig(),
                        new BackupRecoveryService(), logProvider ) )
                .collect( Collectors.toList() );

        return new BackupStrategyCoordinator( consistencyCheckService, outsideWorld, logProvider, progressMonitorFactory, strategies );
    }
}
