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
package org.neo4j.coreedge.raft;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.raft.membership.MembershipWaiter;
import org.neo4j.coreedge.raft.net.Inbound;
import org.neo4j.coreedge.raft.net.codecs.RaftMessageDecoder;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.coreedge.server.ListenSocketAddress;
import org.neo4j.coreedge.server.StoreId;
import org.neo4j.coreedge.server.logging.ExceptionLoggingHandler;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.kernel.impl.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.store.StoreFailureException;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

public class RaftServer extends LifecycleAdapter implements Inbound<RaftMessages.RaftMessage>
{
    private final ListenSocketAddress listenAddress;
    private final LocalDatabase localDatabase;
    private final RaftStateMachine raftStateMachine;
    private final Log log;
    private final ChannelMarshal<ReplicatedContent> marshal;
    private MessageHandler<RaftMessages.RaftMessage> messageHandler;
    private EventLoopGroup workerGroup;
    private Channel channel;
    private final List<MismatchedStoreListener> listeners = new ArrayList<>();

    private final NamedThreadFactory threadFactory = new NamedThreadFactory( "raft-server" );

    public RaftServer( ChannelMarshal<ReplicatedContent> marshal, ListenSocketAddress listenAddress,
                       LocalDatabase localDatabase, LogProvider logProvider,
                       RaftStateMachine raftStateMachine)
    {
        this.marshal = marshal;
        this.listenAddress = listenAddress;
        this.localDatabase = localDatabase;
        this.raftStateMachine = raftStateMachine;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public synchronized void start() throws Throwable
    {
        startNettyServer();
    }

    @Override
    public synchronized void stop() throws Throwable
    {
        try
        {
            channel.close().sync();
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            log.warn( "Interrupted while closing channel." );
        }

        if ( workerGroup.shutdownGracefully( 2, 5, TimeUnit.SECONDS ).awaitUninterruptibly( 10, TimeUnit.SECONDS ) )
        {
            log.warn( "Worker group not shutdown within 10 seconds." );
        }
    }

    private void startNettyServer()
    {
        workerGroup = new NioEventLoopGroup( 0, threadFactory );

        log.info( "Starting server at: " + listenAddress );

        ServerBootstrap bootstrap = new ServerBootstrap()
                .group( workerGroup )
                .channel( NioServerSocketChannel.class )
                .option( ChannelOption.SO_REUSEADDR, true )
                .localAddress( listenAddress.socketAddress() )
                .childHandler( new ChannelInitializer<SocketChannel>()
                {
                    @Override
                    protected void initChannel( SocketChannel ch ) throws Exception
                    {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast( new LengthFieldBasedFrameDecoder( Integer.MAX_VALUE, 0, 4, 0, 4 ) );
                        pipeline.addLast( new LengthFieldPrepender( 4 ) );
                        pipeline.addLast( new RaftMessageDecoder( marshal ) );
                        pipeline.addLast( new RaftMessageHandler() );
                        pipeline.addLast( new ExceptionLoggingHandler( log ) );
                    }
                } );

        channel = bootstrap.bind().syncUninterruptibly().channel();
    }

    @Override
    public void registerHandler( Inbound.MessageHandler<RaftMessages.RaftMessage> handler )
    {
        this.messageHandler = handler;
    }

    public void addMismatchedStoreListener( MismatchedStoreListener listener )
    {
        listeners.add( listener );
    }

    private class RaftMessageHandler extends SimpleChannelInboundHandler<RaftMessages.StoreIdAwareMessage>
    {
        @Override
        protected void channelRead0( ChannelHandlerContext channelHandlerContext,
                                     RaftMessages.StoreIdAwareMessage storeIdAwareMessage ) throws Exception
        {
            try
            {
                RaftMessages.RaftMessage message = storeIdAwareMessage.message();
                StoreId storeId = storeIdAwareMessage.storeId();

                if ( storeId.equals( localDatabase.storeId() ) )
                {
                    messageHandler.handle( message );
                }
                else
                {
                    if ( localDatabase.isEmpty() )
                    {
                        raftStateMachine.downloadSnapshot( message.from() );
                    }
                    else
                    {
                        log.info( "Discarding message owing to mismatched storeId and non-empty store. Expected: %s, " +
                                "Encountered: %s", storeId, localDatabase.storeId() );
                        listeners.forEach( l -> {
                            MismatchedStoreIdException ex = new MismatchedStoreIdException( storeId, localDatabase.storeId() );
                            l.onMismatchedStore( ex );
                        } );
                    }
                }
            }
            catch ( Exception e )
            {
                log.error( format( "Failed to process message %s", storeIdAwareMessage ), e );
            }
        }
    }

    public interface MismatchedStoreListener
    {
        void onMismatchedStore(MismatchedStoreIdException ex);
    }

    public class MismatchedStoreIdException extends StoreFailureException
    {
        public MismatchedStoreIdException( StoreId expected, StoreId encountered )
        {
            super( "Expected:" + expected + ", encountered:" + encountered );
        }
    }
}
