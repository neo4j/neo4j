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

import org.neo4j.causalclustering.catchup.storecopy.GetStoreIdRequestHandler;
import org.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyFilesProvider;
import org.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyRequestHandler;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyRequestHandler;
import org.neo4j.causalclustering.catchup.storecopy.StoreFileStreamingProtocol;
import org.neo4j.causalclustering.catchup.tx.TxPullRequestHandler;
import org.neo4j.causalclustering.core.state.CoreSnapshotService;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshotRequestHandler;
import org.neo4j.causalclustering.identity.StoreId;
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

    private final Monitors monitors;
    private final LogProvider logProvider;
    private final Supplier<StoreId> storeIdSupplier;
    private final Supplier<TransactionIdStore> transactionIdStoreSupplier;
    private final Supplier<LogicalTransactionStore> logicalTransactionStoreSupplier;
    private final Supplier<NeoStoreDataSource> dataSourceSupplier;
    private final BooleanSupplier dataSourceAvailabilitySupplier;
    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final StoreCopyCheckPointMutex storeCopyCheckPointMutex;
    private final CoreSnapshotService snapshotService;
    private final Supplier<CheckPointer> checkPointerSupplier;

    public RegularCatchupServerHandler( Monitors monitors, LogProvider logProvider, Supplier<StoreId> storeIdSupplier,
            Supplier<TransactionIdStore> transactionIdStoreSupplier, Supplier<LogicalTransactionStore> logicalTransactionStoreSupplier,
            Supplier<NeoStoreDataSource> dataSourceSupplier, BooleanSupplier dataSourceAvailabilitySupplier, FileSystemAbstraction fs, PageCache pageCache,
            StoreCopyCheckPointMutex storeCopyCheckPointMutex, CoreSnapshotService snapshotService, Supplier<CheckPointer> checkPointerSupplier )
    {

        this.monitors = monitors;
        this.logProvider = logProvider;
        this.storeIdSupplier = storeIdSupplier;
        this.transactionIdStoreSupplier = transactionIdStoreSupplier;
        this.logicalTransactionStoreSupplier = logicalTransactionStoreSupplier;
        this.dataSourceSupplier = dataSourceSupplier;
        this.dataSourceAvailabilitySupplier = dataSourceAvailabilitySupplier;
        this.fs = fs;
        this.pageCache = pageCache;
        this.storeCopyCheckPointMutex = storeCopyCheckPointMutex;
        this.snapshotService = snapshotService;
        this.checkPointerSupplier = checkPointerSupplier;
    }

    @Override
    public ChannelHandler txPullRequestHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return new TxPullRequestHandler( catchupServerProtocol, storeIdSupplier, dataSourceAvailabilitySupplier, transactionIdStoreSupplier,
                logicalTransactionStoreSupplier, monitors, logProvider );
    }

    @Override
    public ChannelHandler getStoreIdRequestHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return new GetStoreIdRequestHandler( catchupServerProtocol, storeIdSupplier );
    }

    @Override
    public ChannelHandler storeListingRequestHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return new PrepareStoreCopyRequestHandler( catchupServerProtocol, checkPointerSupplier, storeCopyCheckPointMutex, dataSourceSupplier,
                new PrepareStoreCopyFilesProvider( pageCache, fs ) );
    }

    @Override
    public ChannelHandler getStoreFileRequestHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return new StoreCopyRequestHandler.GetStoreFileRequestHandler( catchupServerProtocol, dataSourceSupplier, checkPointerSupplier,
                new StoreFileStreamingProtocol(), pageCache, fs,
                logProvider );
    }

    @Override
    public ChannelHandler getIndexSnapshotRequestHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return new StoreCopyRequestHandler.GetIndexSnapshotRequestHandler( catchupServerProtocol, dataSourceSupplier, checkPointerSupplier,
                new StoreFileStreamingProtocol(), pageCache, fs, logProvider );
    }

    @Override
    public Optional<ChannelHandler> snapshotHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return Optional.ofNullable( (snapshotService != null) ? new CoreSnapshotRequestHandler( catchupServerProtocol, snapshotService ) : null );
    }
}
