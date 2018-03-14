/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.core;

import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.CatchupServer;
import org.neo4j.causalclustering.catchup.CheckpointerSupplier;
import org.neo4j.causalclustering.core.state.CoreSnapshotService;
import org.neo4j.causalclustering.handlers.PipelineWrapper;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;

public class TransactionBackupServiceProvider
{
    private final LogProvider logProvider;
    private final LogProvider userLogProvider;
    private final Supplier<StoreId> localDatabaseStoreIdSupplier;
    private final Dependencies dependencies;
    private final Monitors monitors;
    private final PageCache pageCache;
    private final StoreCopyCheckPointMutex storeCopyCheckPointMutex;
    private final Supplier<NeoStoreDataSource> localDatabaseDataSourceSupplier;
    private final BooleanSupplier localDatabaseIsAvailable;
    private final CoreSnapshotService coreSnapshotService;
    private final FileSystemAbstraction fileSystem;
    private final PipelineWrapper serverPipelineWrapper;
    private final TransactionBackupServiceAddressResolver transactionBackupServiceAddressResolver;

    public TransactionBackupServiceProvider( LogProvider logProvider, LogProvider userLogProvider, Supplier<StoreId> localDatabaseStoreIdSupplier,
            Dependencies dependencies, Monitors monitors, PageCache pageCache, StoreCopyCheckPointMutex storeCopyCheckPointMutex,
            Supplier<NeoStoreDataSource> localDatabaseDataSourceSupplier, BooleanSupplier localDatabaseIsAvailable, CoreSnapshotService coreSnapshotService,
            FileSystemAbstraction fileSystem, PipelineWrapper serverPipelineWrapper )
    {
        this.logProvider = logProvider;
        this.userLogProvider = userLogProvider;
        this.localDatabaseStoreIdSupplier = localDatabaseStoreIdSupplier;
        this.dependencies = dependencies;
        this.monitors = monitors;
        this.pageCache = pageCache;
        this.storeCopyCheckPointMutex = storeCopyCheckPointMutex;
        this.localDatabaseDataSourceSupplier = localDatabaseDataSourceSupplier;
        this.localDatabaseIsAvailable = localDatabaseIsAvailable;
        this.coreSnapshotService = coreSnapshotService;
        this.fileSystem = fileSystem;
        this.serverPipelineWrapper = serverPipelineWrapper;
        this.transactionBackupServiceAddressResolver = new TransactionBackupServiceAddressResolver();
    }

    public TransactionBackupServiceProvider( LogProvider logProvider, LogProvider userLogProvider, Supplier<StoreId> localDatabaseStoreIdSupplier,
            PlatformModule platformModule, Supplier<NeoStoreDataSource> localDatabaseDataSourceSupplier, BooleanSupplier localDatabaseIsAvailable,
            CoreSnapshotService coreSnapshotService, FileSystemAbstraction fileSystem, PipelineWrapper serverPipelineWrapper )
    {
        this( logProvider, userLogProvider, localDatabaseStoreIdSupplier, platformModule.dependencies, platformModule.monitors, platformModule.pageCache,
                platformModule.storeCopyCheckPointMutex, localDatabaseDataSourceSupplier, localDatabaseIsAvailable, coreSnapshotService, fileSystem,
                serverPipelineWrapper );
    }

    public Optional<CatchupServer> resolveIfBackupEnabled( Config config )
    {
        if ( config.get( OnlineBackupSettings.online_backup_enabled ) )
        {
            return Optional.of(
                    new CatchupServer( logProvider, userLogProvider, localDatabaseStoreIdSupplier, dependencies.provideDependency( TransactionIdStore.class ),
                            dependencies.provideDependency( LogicalTransactionStore.class ), localDatabaseDataSourceSupplier, localDatabaseIsAvailable,
                            coreSnapshotService, monitors, new CheckpointerSupplier( dependencies ), fileSystem, pageCache,
                            transactionBackupServiceAddressResolver.backupAddressForTxProtocol( config ), storeCopyCheckPointMutex, serverPipelineWrapper ) );
        }
        else
        {
            return Optional.empty();
        }
    }
}
