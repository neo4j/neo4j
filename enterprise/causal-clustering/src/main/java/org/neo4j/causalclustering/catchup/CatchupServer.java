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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.stream.ChunkedWriteHandler;

import java.net.InetSocketAddress;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.neo4j.causalclustering.VersionDecoder;
import org.neo4j.causalclustering.VersionPrepender;
import org.neo4j.causalclustering.catchup.CatchupServerProtocol.State;
import org.neo4j.causalclustering.catchup.storecopy.FileChunkEncoder;
import org.neo4j.causalclustering.catchup.storecopy.FileHeaderEncoder;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreIdRequest;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreIdRequestHandler;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreIdResponseEncoder;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreRequestDecoder;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreRequestHandler;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponseEncoder;
import org.neo4j.causalclustering.catchup.storecopy.StoreResourceStreamFactory;
import org.neo4j.causalclustering.catchup.storecopy.StoreStreamingProcess;
import org.neo4j.causalclustering.catchup.storecopy.StoreStreamingProtocol;
import org.neo4j.causalclustering.catchup.tx.TxPullRequestDecoder;
import org.neo4j.causalclustering.catchup.tx.TxPullRequestHandler;
import org.neo4j.causalclustering.catchup.tx.TxPullResponseEncoder;
import org.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponseEncoder;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.server.AbstractNettyApplication;
import org.neo4j.causalclustering.core.state.CoreSnapshotService;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshotEncoder;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshotRequest;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshotRequestHandler;
import org.neo4j.causalclustering.handlers.ExceptionLoggingHandler;
import org.neo4j.causalclustering.handlers.ExceptionMonitoringHandler;
import org.neo4j.causalclustering.handlers.ExceptionSwallowingHandler;
import org.neo4j.causalclustering.handlers.PipelineHandlerAppender;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class CatchupServer extends AbstractNettyApplication<ServerBootstrap>
{
    private final LogProvider logProvider;
    private final Log log;
    private final Log userLog;
    private final Monitors monitors;

    private final Supplier<StoreId> storeIdSupplier;
    private final Supplier<TransactionIdStore> transactionIdStoreSupplier;
    private final Supplier<LogicalTransactionStore> logicalTransactionStoreSupplier;
    private final Supplier<NeoStoreDataSource> dataSourceSupplier;
    private final BooleanSupplier dataSourceAvailabilitySupplier;
    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final PipelineHandlerAppender pipelineAppender;
    private final StoreCopyCheckPointMutex storeCopyCheckPointMutex;

    private final NamedThreadFactory threadFactory = new NamedThreadFactory( "catchup-server" );
    private final CoreSnapshotService snapshotService;
    private final ListenSocketAddress listenAddress;

    private EventLoopGroup workerGroup;
    private final Supplier<CheckPointer> checkPointerSupplier;

    public CatchupServer( LogProvider logProvider, LogProvider userLogProvider, Supplier<StoreId> storeIdSupplier,
            Supplier<TransactionIdStore> transactionIdStoreSupplier,
            Supplier<LogicalTransactionStore> logicalTransactionStoreSupplier,
            Supplier<NeoStoreDataSource> dataSourceSupplier, BooleanSupplier dataSourceAvailabilitySupplier,
            CoreSnapshotService snapshotService, Config config, Monitors monitors,
            Supplier<CheckPointer> checkPointerSupplier,
            FileSystemAbstraction fs, PageCache pageCache,
            StoreCopyCheckPointMutex storeCopyCheckPointMutex, PipelineHandlerAppender pipelineAppender )
    {
        super( logProvider, userLogProvider );
        this.snapshotService = snapshotService;
        this.storeCopyCheckPointMutex = storeCopyCheckPointMutex;
        this.listenAddress = config.get( CausalClusteringSettings.transaction_listen_address );
        this.transactionIdStoreSupplier = transactionIdStoreSupplier;
        this.storeIdSupplier = storeIdSupplier;
        this.dataSourceAvailabilitySupplier = dataSourceAvailabilitySupplier;
        this.logicalTransactionStoreSupplier = logicalTransactionStoreSupplier;
        this.logProvider = logProvider;
        this.monitors = monitors;
        this.log = logProvider.getLog( getClass() );
        this.userLog = userLogProvider.getLog( getClass() );
        this.dataSourceSupplier = dataSourceSupplier;
        this.checkPointerSupplier = checkPointerSupplier;
        this.fs = fs;
        this.pageCache = pageCache;
        this.pipelineAppender = pipelineAppender;
    }

    @Override
    protected EventLoopGroup getEventLoopGroup()
    {
        if ( workerGroup == null )
        {
            workerGroup = new NioEventLoopGroup( 0, threadFactory );
        }
        return workerGroup;
    }

    @Override
    protected ServerBootstrap bootstrap()
    {
        return new ServerBootstrap()
                .group( getEventLoopGroup() )
                .channel( NioServerSocketChannel.class )
                .childHandler( new ChannelInitializer<SocketChannel>()
                {
                    @Override
                    protected void initChannel( SocketChannel ch ) throws Exception
                    {
                        CatchupServerProtocol protocol = new CatchupServerProtocol();

                        ChannelPipeline pipeline = ch.pipeline();

                        pipelineAppender.addPipelineHandlerForServer( pipeline, ch );

                        pipeline.addLast( new LengthFieldBasedFrameDecoder( Integer.MAX_VALUE, 0, 4, 0, 4 ) );
                        pipeline.addLast( new LengthFieldPrepender( 4 ) );

                        pipeline.addLast( new VersionDecoder( logProvider ) );
                        pipeline.addLast( new VersionPrepender() );

                        pipeline.addLast( new ResponseMessageTypeEncoder() );
                        pipeline.addLast( new RequestMessageTypeEncoder() );

                        pipeline.addLast( new TxPullResponseEncoder() );
                        pipeline.addLast( new CoreSnapshotEncoder() );
                        pipeline.addLast( new GetStoreIdResponseEncoder() );
                        pipeline.addLast( new StoreCopyFinishedResponseEncoder() );
                        pipeline.addLast( new TxStreamFinishedResponseEncoder() );
                        pipeline.addLast( new FileChunkEncoder() );
                        pipeline.addLast( new FileHeaderEncoder() );

                        pipeline.addLast( new ServerMessageTypeHandler( protocol, logProvider ) );

                        pipeline.addLast( decoders( protocol ) );

                        pipeline.addLast( new ChunkedWriteHandler() );

                        pipeline.addLast(
                                new TxPullRequestHandler( protocol, storeIdSupplier, dataSourceAvailabilitySupplier,
                                        transactionIdStoreSupplier, logicalTransactionStoreSupplier, monitors,
                                        logProvider ) );
                        pipeline.addLast( new GetStoreRequestHandler( protocol, dataSourceSupplier,
                                new StoreStreamingProcess( new StoreStreamingProtocol(), checkPointerSupplier,
                                        storeCopyCheckPointMutex,
                                        new StoreResourceStreamFactory( pageCache, fs, dataSourceSupplier ) ) ) );

                        pipeline.addLast( new GetStoreIdRequestHandler( protocol, storeIdSupplier ) );

                        if ( snapshotService != null )
                        {
                            pipeline.addLast( new CoreSnapshotRequestHandler( protocol, snapshotService ) );
                        }

                        pipeline.addLast( new ExceptionLoggingHandler( log ) );
                        pipeline.addLast( new ExceptionMonitoringHandler(
                                monitors.newMonitor( ExceptionMonitoringHandler.Monitor.class,
                                        CatchupServer.class ) ) );
                        pipeline.addLast( new ExceptionSwallowingHandler() );
                    }
                } );
    }

    @Override
    protected InetSocketAddress bindAddress()
    {
        return listenAddress.socketAddress();
    }

    private ChannelInboundHandler decoders( CatchupServerProtocol protocol )
    {
        RequestDecoderDispatcher<State> decoderDispatcher = new RequestDecoderDispatcher<>( protocol, logProvider );
        decoderDispatcher.register( State.TX_PULL, new TxPullRequestDecoder() );
        decoderDispatcher.register( State.GET_STORE, new GetStoreRequestDecoder() );
        decoderDispatcher.register( State.GET_STORE_ID, new SimpleRequestDecoder( GetStoreIdRequest::new ) );
        decoderDispatcher.register( State.GET_CORE_SNAPSHOT, new SimpleRequestDecoder( CoreSnapshotRequest::new ) );
        return decoderDispatcher;
    }
}
