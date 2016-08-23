/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.catchup;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
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

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.neo4j.coreedge.VersionDecoder;
import org.neo4j.coreedge.VersionPrepender;
import org.neo4j.coreedge.catchup.CatchupServerProtocol.State;
import org.neo4j.coreedge.catchup.storecopy.FileHeaderEncoder;
import org.neo4j.coreedge.catchup.storecopy.GetStoreIdRequest;
import org.neo4j.coreedge.catchup.storecopy.GetStoreIdRequestHandler;
import org.neo4j.coreedge.catchup.storecopy.GetStoreRequest;
import org.neo4j.coreedge.catchup.storecopy.GetStoreRequestHandler;
import org.neo4j.coreedge.catchup.storecopy.StoreCopyFinishedResponseEncoder;
import org.neo4j.coreedge.catchup.tx.TxPullRequestDecoder;
import org.neo4j.coreedge.catchup.tx.TxPullRequestHandler;
import org.neo4j.coreedge.catchup.tx.TxPullResponseEncoder;
import org.neo4j.coreedge.catchup.tx.TxStreamFinishedResponseEncoder;
import org.neo4j.coreedge.core.state.CoreState;
import org.neo4j.coreedge.core.state.snapshot.CoreSnapshotEncoder;
import org.neo4j.coreedge.core.state.snapshot.CoreSnapshotRequest;
import org.neo4j.coreedge.core.state.snapshot.CoreSnapshotRequestHandler;
import org.neo4j.coreedge.identity.StoreId;
import org.neo4j.coreedge.logging.ExceptionLoggingHandler;
import org.neo4j.coreedge.messaging.address.ListenSocketAddress;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class CatchupServer extends LifecycleAdapter
{
    private final LogProvider logProvider;
    private Monitors monitors;

    private final Supplier<StoreId> storeIdSupplier;
    private final Supplier<TransactionIdStore> transactionIdStoreSupplier;
    private final Supplier<LogicalTransactionStore> logicalTransactionStoreSupplier;
    private final Supplier<NeoStoreDataSource> dataSourceSupplier;

    private final NamedThreadFactory threadFactory = new NamedThreadFactory( "catchup-server" );
    private final CoreState coreState;
    private final ListenSocketAddress listenAddress;

    private EventLoopGroup workerGroup;
    private Channel channel;
    private Supplier<CheckPointer> checkPointerSupplier;
    private Log log;

    public CatchupServer( LogProvider logProvider,
                          Supplier<StoreId> storeIdSupplier,
                          Supplier<TransactionIdStore> transactionIdStoreSupplier,
                          Supplier<LogicalTransactionStore> logicalTransactionStoreSupplier,
                          Supplier<NeoStoreDataSource> dataSourceSupplier,
                          Supplier<CheckPointer> checkPointerSupplier,
                          CoreState coreState,
                          ListenSocketAddress listenAddress, Monitors monitors )
    {
        this.coreState = coreState;
        this.listenAddress = listenAddress;
        this.transactionIdStoreSupplier = transactionIdStoreSupplier;
        this.storeIdSupplier = storeIdSupplier;
        this.logicalTransactionStoreSupplier = logicalTransactionStoreSupplier;
        this.logProvider = logProvider;
        this.monitors = monitors;
        this.log = logProvider.getLog( getClass() );
        this.dataSourceSupplier = dataSourceSupplier;
        this.checkPointerSupplier = checkPointerSupplier;
    }

    @Override
    public synchronized void start() throws Throwable
    {
        workerGroup = new NioEventLoopGroup( 0, threadFactory );

        ServerBootstrap bootstrap = new ServerBootstrap()
                .group( workerGroup )
                .channel( NioServerSocketChannel.class )
                .localAddress( listenAddress.socketAddress() )
                .childHandler( new ChannelInitializer<SocketChannel>()
                {
                    @Override
                    protected void initChannel( SocketChannel ch ) throws Exception
                    {
                        CatchupServerProtocol protocol = new CatchupServerProtocol();

                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast( new LengthFieldBasedFrameDecoder( Integer.MAX_VALUE, 0, 4, 0, 4 ) );
                        pipeline.addLast( new LengthFieldPrepender( 4 ) );

                        pipeline.addLast( new VersionDecoder( logProvider ) );
                        pipeline.addLast( new VersionPrepender() );

                        pipeline.addLast( new ResponseMessageTypeEncoder() );
                        pipeline.addLast( new RequestMessageTypeEncoder() );

                        pipeline.addLast( new TxPullResponseEncoder() );
                        pipeline.addLast( new CoreSnapshotEncoder() );
                        pipeline.addLast( new StoreCopyFinishedResponseEncoder() );
                        pipeline.addLast( new TxStreamFinishedResponseEncoder() );
                        pipeline.addLast( new FileHeaderEncoder() );

                        pipeline.addLast( new ServerMessageTypeHandler( protocol, logProvider ) );

                        pipeline.addLast( decoders( protocol ) );

                        pipeline.addLast( new TxPullRequestHandler( protocol, storeIdSupplier,
                                transactionIdStoreSupplier, logicalTransactionStoreSupplier,
                                monitors, logProvider ) );
                        pipeline.addLast( new ChunkedWriteHandler() );
                        pipeline.addLast( new GetStoreRequestHandler( protocol, dataSourceSupplier,
                                checkPointerSupplier ) );
                        pipeline.addLast( new GetStoreIdRequestHandler( protocol, storeIdSupplier ) );
                        pipeline.addLast( new CoreSnapshotRequestHandler( protocol, coreState ) );
                        pipeline.addLast( new ExceptionLoggingHandler( log ) );
                    }
                } );

        channel = bootstrap.bind().syncUninterruptibly().channel();
    }

    private ChannelInboundHandler decoders( CatchupServerProtocol protocol )
    {
        RequestDecoderDispatcher<State> decoderDispatcher =
                new RequestDecoderDispatcher<>( protocol, logProvider );
        decoderDispatcher.register( State.TX_PULL, new TxPullRequestDecoder() );
        decoderDispatcher.register( State.GET_STORE, new SimpleRequestDecoder( GetStoreRequest::new ) );
        decoderDispatcher.register( State.GET_STORE_ID, new SimpleRequestDecoder( GetStoreIdRequest::new ) );
        decoderDispatcher.register( State.GET_CORE_SNAPSHOT, new SimpleRequestDecoder( CoreSnapshotRequest::new) );
        return decoderDispatcher;
    }

    @Override
    public synchronized void stop() throws Throwable
    {
        try
        {
            channel.close().sync();
        }
        catch( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            log.warn( "Interrupted while closing channel." );
        }

        if ( workerGroup.shutdownGracefully( 2, 5, TimeUnit.SECONDS ).awaitUninterruptibly( 10, TimeUnit.SECONDS ) )
        {
            log.warn( "Worker group not shutdown within 10 seconds." );
        }
    }
}
