/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.coreedge.catchup.storecopy.FileHeaderEncoder;
import org.neo4j.coreedge.catchup.storecopy.core.GetStoreRequestHandler;
import org.neo4j.coreedge.catchup.storecopy.core.StoreCopyFinishedResponseEncoder;
import org.neo4j.coreedge.catchup.storecopy.edge.GetStoreRequestDecoder;
import org.neo4j.coreedge.catchup.tx.core.TxPullRequestDecoder;
import org.neo4j.coreedge.catchup.tx.core.TxPullRequestHandler;
import org.neo4j.coreedge.catchup.tx.core.TxPullResponseEncoder;
import org.neo4j.coreedge.catchup.tx.edge.TxStreamFinishedResponseEncoder;
import org.neo4j.coreedge.server.ListenSocketAddress;
import org.neo4j.coreedge.server.logging.ExceptionLoggingHandler;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class CatchupServer extends LifecycleAdapter
{
    private final LogProvider logProvider;

    private final Supplier<StoreId> storeIdSupplier;
    private final Supplier<TransactionIdStore> transactionIdStoreSupplier;
    private final Supplier<LogicalTransactionStore> logicalTransactionStoreSupplier;
    private final Supplier<NeoStoreDataSource> dataSourceSupplier;

    private final NamedThreadFactory threadFactory = new NamedThreadFactory( "catchup-server" );
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
                          ListenSocketAddress listenAddress )
    {
        this.listenAddress = listenAddress;
        this.transactionIdStoreSupplier = transactionIdStoreSupplier;
        this.storeIdSupplier = storeIdSupplier;
        this.logicalTransactionStoreSupplier = logicalTransactionStoreSupplier;
        this.logProvider = logProvider;
        this.log = logProvider.getLog( getClass() );
        this.dataSourceSupplier = dataSourceSupplier;
        this.checkPointerSupplier = checkPointerSupplier;
    }

    @Override
    public synchronized void start() throws Throwable
    {
        startNettyServer();
    }

    private void startNettyServer()
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

                        pipeline.addLast( new ResponseMessageTypeEncoder() );
                        pipeline.addLast( new RequestMessageTypeEncoder() );
                        pipeline.addLast( new TxPullResponseEncoder() );
                        pipeline.addLast( new StoreCopyFinishedResponseEncoder() );
                        pipeline.addLast( new TxStreamFinishedResponseEncoder() );
                        pipeline.addLast( new FileHeaderEncoder() );

                        pipeline.addLast( new ServerMessageTypeHandler( protocol, logProvider ) );

                        pipeline.addLast( new TxPullRequestDecoder( protocol ) );
                        pipeline.addLast( new TxPullRequestHandler( protocol, storeIdSupplier,
                                transactionIdStoreSupplier, logicalTransactionStoreSupplier ) );

                        pipeline.addLast( new ChunkedWriteHandler() );
                        pipeline.addLast( new GetStoreRequestDecoder( protocol ) );
                        pipeline.addLast( new GetStoreRequestHandler( protocol, dataSourceSupplier,
                                checkPointerSupplier ) );

                        pipeline.addLast( new ExceptionLoggingHandler( log ) );
                    }
                } );

        channel = bootstrap.bind().syncUninterruptibly().channel();
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
