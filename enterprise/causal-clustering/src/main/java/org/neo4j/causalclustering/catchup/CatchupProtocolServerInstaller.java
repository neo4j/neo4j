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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInboundHandler;
import io.netty.handler.stream.ChunkedWriteHandler;

import java.util.Collection;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.catchup.storecopy.FileChunkEncoder;
import org.neo4j.causalclustering.catchup.storecopy.FileHeaderEncoder;
import org.neo4j.causalclustering.catchup.storecopy.GetIndexFilesRequest;
import org.neo4j.causalclustering.catchup.storecopy.GetIndexSnapshotRequestHandler;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreFileRequest;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreFileRequestHandler;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreIdRequest;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreIdRequestHandler;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreIdResponseEncoder;
import org.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyFilesProvider;
import org.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyRequestDecoder;
import org.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyRequestHandler;
import org.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponseEncoder;
import org.neo4j.causalclustering.catchup.storecopy.StoreFileStreamingProtocol;
import org.neo4j.causalclustering.catchup.tx.TxPullRequestDecoder;
import org.neo4j.causalclustering.catchup.tx.TxPullRequestHandler;
import org.neo4j.causalclustering.catchup.tx.TxPullResponseEncoder;
import org.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponseEncoder;
import org.neo4j.causalclustering.core.state.CoreSnapshotService;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshotEncoder;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshotRequest;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshotRequestHandler;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.causalclustering.protocol.ProtocolInstaller;
import org.neo4j.causalclustering.protocol.ProtocolInstaller.Orientation;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class CatchupProtocolServerInstaller implements ProtocolInstaller<Orientation.Server>
{
    private static final Protocol.ApplicationProtocols APPLICATION_PROTOCOL = Protocol.ApplicationProtocols.CATCHUP_1;

    public static class Factory extends ProtocolInstaller.Factory<Orientation.Server,CatchupProtocolServerInstaller>
    {
        public Factory( NettyPipelineBuilderFactory pipelineBuilderFactory, LogProvider logProvider, Supplier<StoreId> storeIdSupplier,
                Supplier<TransactionIdStore> transactionIdStoreSupplier, Supplier<LogicalTransactionStore> logicalTransactionStoreSupplier,
                Supplier<NeoStoreDataSource> dataSourceSupplier, BooleanSupplier dataSourceAvailabilitySupplier, CoreSnapshotService snapshotService,
                Monitors monitors, Supplier<CheckPointer> checkPointerSupplier, FileSystemAbstraction fs, PageCache pageCache,
                StoreCopyCheckPointMutex storeCopyCheckPointMutex )
        {
            super( APPLICATION_PROTOCOL,
                    modifiers -> new CatchupProtocolServerInstaller( pipelineBuilderFactory, modifiers, logProvider, monitors, storeIdSupplier,
                            transactionIdStoreSupplier, logicalTransactionStoreSupplier, dataSourceSupplier, dataSourceAvailabilitySupplier, fs, pageCache,
                            storeCopyCheckPointMutex, snapshotService, checkPointerSupplier ) );
        }
    }

    private final NettyPipelineBuilderFactory pipelineBuilderFactory;
    private final List<ModifierProtocolInstaller<Orientation.Server>> modifiers;
    private final Log log;

    private final LogProvider logProvider;
    private final Monitors monitors;

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

    public CatchupProtocolServerInstaller( NettyPipelineBuilderFactory pipelineBuilderFactory, List<ModifierProtocolInstaller<Orientation.Server>> modifiers,
            LogProvider logProvider, Monitors monitors, Supplier<StoreId> storeIdSupplier, Supplier<TransactionIdStore> transactionIdStoreSupplier,
            Supplier<LogicalTransactionStore> logicalTransactionStoreSupplier, Supplier<NeoStoreDataSource> dataSourceSupplier,
            BooleanSupplier dataSourceAvailabilitySupplier, FileSystemAbstraction fs, PageCache pageCache, StoreCopyCheckPointMutex storeCopyCheckPointMutex,
            CoreSnapshotService snapshotService, Supplier<CheckPointer> checkPointerSupplier )
    {
        this.pipelineBuilderFactory = pipelineBuilderFactory;
        this.modifiers = modifiers;
        this.log = logProvider.getLog( getClass() );
        this.logProvider = logProvider;
        this.monitors = monitors;
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
    public void install( Channel channel ) throws Exception
    {
        CatchupServerProtocol protocol = new CatchupServerProtocol();

        List<ChannelHandler> snapshotHandler = (snapshotService != null)
                                               ? singletonList( new CoreSnapshotRequestHandler( protocol, snapshotService ) )
                                               : emptyList();

        ServerMessageTypeHandler serverMessageHandler = new ServerMessageTypeHandler( protocol, logProvider );
        TxPullRequestHandler txPullRequestHandler = new TxPullRequestHandler( protocol, storeIdSupplier, dataSourceAvailabilitySupplier,
                transactionIdStoreSupplier, logicalTransactionStoreSupplier, monitors, logProvider );
        GetStoreIdRequestHandler getStoreIdRequestHandler = new GetStoreIdRequestHandler( protocol, storeIdSupplier );
        PrepareStoreCopyRequestHandler storeListingRequestHandler = storeListingRequestHandler( protocol, checkPointerSupplier, storeCopyCheckPointMutex,
                dataSourceSupplier, pageCache, fs );
        GetStoreFileRequestHandler getStoreFileRequestHandler = new GetStoreFileRequestHandler( protocol, dataSourceSupplier, checkPointerSupplier,
                new StoreFileStreamingProtocol(), pageCache, fs, logProvider );
        GetIndexSnapshotRequestHandler getIndexSnapshotRequestHandler = new GetIndexSnapshotRequestHandler( protocol, dataSourceSupplier, checkPointerSupplier,
                new StoreFileStreamingProtocol(), pageCache, fs );

        pipelineBuilderFactory.server( channel, log )
                .modify( modifiers )
                .addFraming()
                .add( "hnd_req_type", serverMessageHandler )
                .add( "hnd_chunked_write", new ChunkedWriteHandler() )
                .add( "hnd_req_tx", txPullRequestHandler )
                .add( "hnd_req_store_id", getStoreIdRequestHandler )
                .add( "hnd_req_store_listing", storeListingRequestHandler )
                .add( "hnd_req_store_file", getStoreFileRequestHandler )
                .add( "hnd_req_index_snapshot", getIndexSnapshotRequestHandler )
                .add( "hnd_req_snapshot", snapshotHandler )
                .add( "dec_req_dispatch", requestDecoders( protocol ) )
                .add( "enc_req_type", new RequestMessageTypeEncoder() )
                .add( "enc_res_type", new ResponseMessageTypeEncoder() )
                .add( "enc_res_pull", new TxPullResponseEncoder() )
                .add( "enc_res_storeid", new GetStoreIdResponseEncoder() )
                .add( "enc_res_copy_fin", new StoreCopyFinishedResponseEncoder() )
                .add( "enc_res_tx_fin", new TxStreamFinishedResponseEncoder() )
                .add( "enc_res_pre_copy", new PrepareStoreCopyResponse.Encoder() )
                .add( "enc_snapshot", new CoreSnapshotEncoder() )
                .add( "enc_file_chunk", new FileChunkEncoder() )
                .add( "enc_file_header", new FileHeaderEncoder() )
                .install();
    }

    private PrepareStoreCopyRequestHandler storeListingRequestHandler( CatchupServerProtocol protocol, Supplier<CheckPointer> checkPointerSupplier,
            StoreCopyCheckPointMutex storeCopyCheckPointMutex, Supplier<NeoStoreDataSource> dataSourceSupplier, PageCache pageCache, FileSystemAbstraction fs )
    {
        PrepareStoreCopyFilesProvider prepareStoreCopyFilesProvider = new PrepareStoreCopyFilesProvider( pageCache, fs );
        return new PrepareStoreCopyRequestHandler( protocol, checkPointerSupplier, storeCopyCheckPointMutex, dataSourceSupplier,
                prepareStoreCopyFilesProvider );
    }

    private ChannelInboundHandler requestDecoders( CatchupServerProtocol protocol )
    {
        RequestDecoderDispatcher<CatchupServerProtocol.State> decoderDispatcher = new RequestDecoderDispatcher<>( protocol, logProvider );
        decoderDispatcher.register( CatchupServerProtocol.State.TX_PULL, new TxPullRequestDecoder() );
        decoderDispatcher.register( CatchupServerProtocol.State.GET_STORE_ID, new SimpleRequestDecoder( GetStoreIdRequest::new ) );
        decoderDispatcher.register( CatchupServerProtocol.State.GET_CORE_SNAPSHOT, new SimpleRequestDecoder( CoreSnapshotRequest::new ) );
        decoderDispatcher.register( CatchupServerProtocol.State.PREPARE_STORE_COPY, new PrepareStoreCopyRequestDecoder() );
        decoderDispatcher.register( CatchupServerProtocol.State.GET_STORE_FILE, new GetStoreFileRequest.Decoder() );
        decoderDispatcher.register( CatchupServerProtocol.State.GET_INDEX_SNAPSHOT, new GetIndexFilesRequest.Decoder() );
        return decoderDispatcher;
    }

    @Override
    public Protocol.ApplicationProtocol applicationProtocol()
    {
        return APPLICATION_PROTOCOL;
    }

    @Override
    public Collection<Collection<Protocol.ModifierProtocol>> modifiers()
    {
        return modifiers.stream()
                .map( ModifierProtocolInstaller::protocols )
                .collect( Collectors.toList() );
    }
}
