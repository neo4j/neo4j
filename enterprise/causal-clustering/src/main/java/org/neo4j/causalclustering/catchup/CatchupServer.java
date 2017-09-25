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
package org.neo4j.causalclustering.catchup;

import java.net.BindException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.stream.ChunkedWriteHandler;

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
import org.neo4j.causalclustering.catchup.tx.TxPullRequestDecoder;
import org.neo4j.causalclustering.catchup.tx.TxPullRequestHandler;
import org.neo4j.causalclustering.catchup.tx.TxPullResponseEncoder;
import org.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponseEncoder;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
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
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class CatchupServer extends LifecycleAdapter
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
    private Channel channel;
    private final Supplier<CheckPointer> checkPointerSupplier;

    public CatchupServer( LogProvider logProvider, LogProvider userLogProvider, Supplier<StoreId> storeIdSupplier,
                          Supplier<TransactionIdStore> transactionIdStoreSupplier,
                          Supplier<LogicalTransactionStore> logicalTransactionStoreSupplier,
                          Supplier<NeoStoreDataSource> dataSourceSupplier, BooleanSupplier dataSourceAvailabilitySupplier,
                          CoreSnapshotService snapshotService, Config config, Monitors monitors, Supplier<CheckPointer> checkPointerSupplier,
                          FileSystemAbstraction fs, PageCache pageCache,
                          StoreCopyCheckPointMutex storeCopyCheckPointMutex, PipelineHandlerAppender pipelineAppender )
    {
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
    public synchronized void start() throws Throwable
    {
        if ( channel != null )
        {
            return;
        }

        workerGroup = new NioEventLoopGroup( 0, threadFactory );

        ServerBootstrap bootstrap = new ServerBootstrap().group( workerGroup ).channel( NioServerSocketChannel.class )
                .option( ChannelOption.SO_REUSEADDR, true )
                .localAddress( listenAddress.socketAddress() ).childHandler( new ChannelInitializer<SocketChannel>()
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

                        pipeline.addLast( new TxPullRequestHandler( protocol, storeIdSupplier, dataSourceAvailabilitySupplier,
                                transactionIdStoreSupplier, logicalTransactionStoreSupplier, monitors, logProvider ) );
                        pipeline.addLast( new GetStoreRequestHandler( protocol, dataSourceSupplier,
                                checkPointerSupplier, fs, pageCache, logProvider, storeCopyCheckPointMutex ) );

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

        try
        {
            channel = bootstrap.bind().syncUninterruptibly().channel();
        }
        catch ( Exception e )
        {
            // thanks to netty we need to catch everything and do an instanceof because it does not declare properly
            // checked exception but it still throws them with some black magic at runtime.
            //noinspection ConstantConditions
            if ( e instanceof BindException )
            {
                userLog.error(
                        "Address is already bound for setting: " + CausalClusteringSettings.transaction_listen_address +
                                " with value: " + listenAddress );
                log.error(
                        "Address is already bound for setting: " + CausalClusteringSettings.transaction_listen_address +
                                " with value: " + listenAddress, e );
                throw e;
            }
        }
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

    @Override
    public synchronized void stop() throws Throwable
    {
        if ( channel == null )
        {
            return;
        }

        log.info( "CatchupServer stopping and unbinding from " + listenAddress );
        try
        {
            channel.close().sync();
            channel = null;
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            log.warn( "Interrupted while closing channel." );
        }

        if ( workerGroup != null &&
                workerGroup.shutdownGracefully( 2, 5, TimeUnit.SECONDS ).awaitUninterruptibly( 10, TimeUnit.SECONDS ) )
        {
            log.warn( "Worker group not shutdown within 10 seconds." );
        }
        workerGroup = null;
    }
}
