/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
    private final CheckPointerService checkPointerService;

    public RegularCatchupServerHandler( Monitors monitors, LogProvider logProvider, Supplier<StoreId> storeIdSupplier,
            Supplier<TransactionIdStore> transactionIdStoreSupplier, Supplier<LogicalTransactionStore> logicalTransactionStoreSupplier,
            Supplier<NeoStoreDataSource> dataSourceSupplier, BooleanSupplier dataSourceAvailabilitySupplier, FileSystemAbstraction fs, PageCache pageCache,
            StoreCopyCheckPointMutex storeCopyCheckPointMutex, CoreSnapshotService snapshotService, CheckPointerService checkPointerService )
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
        this.checkPointerService = checkPointerService;
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
        return new PrepareStoreCopyRequestHandler( catchupServerProtocol, checkPointerService::getCheckPointer, storeCopyCheckPointMutex,
                dataSourceSupplier, new PrepareStoreCopyFilesProvider( pageCache, fs ) );
    }

    @Override
    public ChannelHandler getStoreFileRequestHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return new StoreCopyRequestHandler.GetStoreFileRequestHandler( catchupServerProtocol, dataSourceSupplier, checkPointerService,
                new StoreFileStreamingProtocol(), pageCache, fs, logProvider );
    }

    @Override
    public ChannelHandler getIndexSnapshotRequestHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return new StoreCopyRequestHandler.GetIndexSnapshotRequestHandler( catchupServerProtocol, dataSourceSupplier, checkPointerService,
                new StoreFileStreamingProtocol(), pageCache, fs, logProvider );
    }

    @Override
    public Optional<ChannelHandler> snapshotHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return Optional.ofNullable( (snapshotService != null) ? new CoreSnapshotRequestHandler( catchupServerProtocol, snapshotService ) : null );
    }
}
