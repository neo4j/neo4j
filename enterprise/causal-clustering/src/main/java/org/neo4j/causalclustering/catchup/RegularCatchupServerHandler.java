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
package org.neo4j.causalclustering.catchup;

import io.netty.channel.ChannelHandler;

import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.storecopy.GetIndexSnapshotRequestHandler;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreFileRequestHandler;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreIdRequestHandler;
import org.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyFilesProvider;
import org.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyRequestHandler;
import org.neo4j.causalclustering.catchup.storecopy.StoreFileStreamingProtocol;
import org.neo4j.causalclustering.catchup.tx.TxPullRequestHandler;
import org.neo4j.causalclustering.core.state.CoreSnapshotService;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshotRequestHandler;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.messaging.LoggingEventHandlerProvider;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;

public class RegularCatchupServerHandler implements CatchupServerHandler
{
    private final GetStoreIdRequestHandler storeIdRequestHandler;
    private final GetStoreFileRequestHandler storeFileRequestHandler;
    private final GetIndexSnapshotRequestHandler indexSnapshotRequestHandler;
    private final CoreSnapshotRequestHandler snapshotHandler;
    private final PrepareStoreCopyRequestHandler storeListingRequestHandler;
    private final TxPullRequestHandler txPullRequestHandler;

    public RegularCatchupServerHandler( CatchupServerProtocol protocol, Monitors monitors, LogProvider logProvider, Supplier<StoreId> storeIdSupplier,
            Supplier<TransactionIdStore> transactionIdStoreSupplier, Supplier<LogicalTransactionStore> logicalTransactionStoreSupplier,
            Supplier<NeoStoreDataSource> dataSourceSupplier, BooleanSupplier dataSourceAvailabilitySupplier, FileSystemAbstraction fs, PageCache pageCache,
            StoreCopyCheckPointMutex storeCopyCheckPointMutex, CoreSnapshotService snapshotService, Supplier<CheckPointer> checkPointerSupplier )
    {
        this.snapshotHandler = (snapshotService != null) ? new CoreSnapshotRequestHandler( protocol, snapshotService ) : null;
        this.txPullRequestHandler = new TxPullRequestHandler( protocol, storeIdSupplier, dataSourceAvailabilitySupplier, transactionIdStoreSupplier,
                logicalTransactionStoreSupplier, monitors, new LoggingEventHandlerProvider( logProvider.getLog( TxPullRequestHandler.class ) ) );
        this.storeIdRequestHandler = new GetStoreIdRequestHandler( protocol, storeIdSupplier );
        PrepareStoreCopyFilesProvider prepareStoreCopyFilesProvider = new PrepareStoreCopyFilesProvider( pageCache, fs );
        this.storeListingRequestHandler = new PrepareStoreCopyRequestHandler( protocol, checkPointerSupplier, storeCopyCheckPointMutex, dataSourceSupplier,
                prepareStoreCopyFilesProvider, new LoggingEventHandlerProvider( logProvider.getLog( PrepareStoreCopyRequestHandler.class ) ) );
        this.storeFileRequestHandler = new GetStoreFileRequestHandler( protocol, dataSourceSupplier, checkPointerSupplier, new StoreFileStreamingProtocol(),
                pageCache, fs, new LoggingEventHandlerProvider( logProvider.getLog( GetStoreFileRequestHandler.class ) ) );
        this.indexSnapshotRequestHandler =
                new GetIndexSnapshotRequestHandler( protocol, dataSourceSupplier, checkPointerSupplier, new StoreFileStreamingProtocol(), pageCache, fs,
                        new LoggingEventHandlerProvider( logProvider.getLog( GetIndexSnapshotRequestHandler.class ) ) );
    }

    @Override
    public ChannelHandler txPullRequestHandler()
    {
        return txPullRequestHandler;
    }

    @Override
    public ChannelHandler getStoreIdRequestHandler()
    {
        return storeIdRequestHandler;
    }

    @Override
    public ChannelHandler storeListingRequestHandler()
    {
        return storeListingRequestHandler;
    }

    @Override
    public ChannelHandler getStoreFileRequestHandler()
    {
        return storeFileRequestHandler;
    }

    @Override
    public ChannelHandler getIndexSnapshotRequestHandler()
    {
        return indexSnapshotRequestHandler;
    }

    @Override
    public Optional<ChannelHandler> snapshotHandler()
    {
        return Optional.ofNullable( snapshotHandler );
    }
}
