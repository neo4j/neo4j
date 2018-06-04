/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import org.neo4j.causalclustering.messaging.LoggingEventHandlerProvider;
import org.neo4j.io.fs.FileSystemAbstraction;
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
    private final StoreCopyCheckPointMutex storeCopyCheckPointMutex;
    private final CoreSnapshotService snapshotService;
    private final Supplier<CheckPointer> checkPointerSupplier;

    public RegularCatchupServerHandler( Monitors monitors, LogProvider logProvider, Supplier<StoreId> storeIdSupplier,
            Supplier<TransactionIdStore> transactionIdStoreSupplier, Supplier<LogicalTransactionStore> logicalTransactionStoreSupplier,
            Supplier<NeoStoreDataSource> dataSourceSupplier, BooleanSupplier dataSourceAvailabilitySupplier, FileSystemAbstraction fs,
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
        this.storeCopyCheckPointMutex = storeCopyCheckPointMutex;
        this.snapshotService = snapshotService;
        this.checkPointerSupplier = checkPointerSupplier;
    }

    @Override
    public ChannelHandler txPullRequestHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return new TxPullRequestHandler( catchupServerProtocol, storeIdSupplier, dataSourceAvailabilitySupplier, transactionIdStoreSupplier,
                logicalTransactionStoreSupplier, monitors, new LoggingEventHandlerProvider( logProvider.getLog( TxPullRequestHandler.class ) ) );
    }

    @Override
    public ChannelHandler getStoreIdRequestHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return new GetStoreIdRequestHandler( catchupServerProtocol, storeIdSupplier,
                                             new LoggingEventHandlerProvider( logProvider.getLog( GetStoreIdRequestHandler.class ) ) );
    }

    @Override
    public ChannelHandler storeListingRequestHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return new PrepareStoreCopyRequestHandler( catchupServerProtocol, checkPointerSupplier, storeCopyCheckPointMutex, dataSourceSupplier,
                new PrepareStoreCopyFilesProvider( fs ),
                new LoggingEventHandlerProvider( logProvider.getLog( PrepareStoreCopyRequestHandler.class ) ) );
    }

    @Override
    public ChannelHandler getStoreFileRequestHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return new StoreCopyRequestHandler.GetStoreFileRequestHandler( catchupServerProtocol, dataSourceSupplier, checkPointerSupplier,
                new StoreFileStreamingProtocol(), fs, logProvider );
    }

    @Override
    public ChannelHandler getIndexSnapshotRequestHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return new StoreCopyRequestHandler.GetIndexSnapshotRequestHandler( catchupServerProtocol, dataSourceSupplier, checkPointerSupplier,
                new StoreFileStreamingProtocol(), fs, logProvider );
    }

    @Override
    public Optional<ChannelHandler> snapshotHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return Optional.ofNullable( (snapshotService != null) ? new CoreSnapshotRequestHandler( catchupServerProtocol, snapshotService,
                new LoggingEventHandlerProvider( logProvider.getLog( CoreSnapshotRequestHandler.class ) ) ) : null );
    }
}
