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
package org.neo4j.com;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.queue.BlockingReadHandler;

import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.com.storecopy.ResponseUnpacker;
import org.neo4j.com.storecopy.ResponseUnpacker.TxHandler;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.impl.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;

import static java.util.concurrent.Executors.newCachedThreadPool;

import static org.neo4j.com.Protocol.addLengthFieldPipes;
import static org.neo4j.com.Protocol.assertChunkSizeIsWithinFrameSize;
import static org.neo4j.com.ResourcePool.DEFAULT_CHECK_INTERVAL;
import static org.neo4j.com.storecopy.ResponseUnpacker.NO_OP_TX_HANDLER;
import static org.neo4j.helpers.Clock.SYSTEM_CLOCK;
import static org.neo4j.helpers.NamedThreadFactory.daemon;

/**
 * A means for a client to communicate with a {@link Server}. It
 * serializes requests and sends them to the server and waits for
 * a response back.
 *
 * @see Server
 */
public abstract class Client<T> extends LifecycleAdapter implements ChannelPipelineFactory
{
    // Max number of concurrent channels that may exist. Needs to be high because we
    // don't want to run into that limit, it will make some #acquire calls block and
    // gets disastrous if that thread is holding monitors that is needed to communicate
    // with the server in some way.
    public static final int DEFAULT_MAX_NUMBER_OF_CONCURRENT_CHANNELS_PER_CLIENT = 20;
    public static final int DEFAULT_READ_RESPONSE_TIMEOUT_SECONDS = 20;

    private static final String BLOCKING_CHANNEL_HANDLER_NAME = "blockingHandler";
    private static final String MONITORING_CHANNEL_HANDLER_NAME = "monitor";

    private ClientBootstrap bootstrap;
    private final SocketAddress address;
    private final StringLogger msgLog;
    private ResourcePool<ChannelContext> channelPool;
    private final Protocol protocol;
    private final int frameLength;
    private final long readTimeout;
    private final int maxUnusedChannels;
    private final StoreId storeId;
    private ResourceReleaser resourcePoolReleaser;
    private ComExceptionHandler comExceptionHandler;
    private final ResponseUnpacker responseUnpacker;
    private final ByteCounterMonitor byteCounterMonitor;
    private final RequestMonitor requestMonitor;

    public Client( String hostNameOrIp, int port, Logging logging, StoreId storeId, int frameLength,
            ProtocolVersion protocolVersion, long readTimeout, int maxConcurrentChannels, int chunkSize,
            ResponseUnpacker responseUnpacker,
            ByteCounterMonitor byteCounterMonitor, RequestMonitor requestMonitor )
    {
        assert byteCounterMonitor != null;
        assert requestMonitor != null;

        this.byteCounterMonitor = byteCounterMonitor;
        this.requestMonitor = requestMonitor;
        assertChunkSizeIsWithinFrameSize( chunkSize, frameLength );

        this.msgLog = logging.getMessagesLog( getClass() );
        this.storeId = storeId;
        this.frameLength = frameLength;
        this.readTimeout = readTimeout;
        // ResourcePool no longer controls max concurrent channels. Use this value for the pool size
        this.maxUnusedChannels = maxConcurrentChannels;
        this.comExceptionHandler = ComExceptionHandler.NO_OP;
        this.address = new InetSocketAddress( hostNameOrIp, port );
        this.protocol = createProtocol( chunkSize, protocolVersion.getApplicationProtocol() );
        this.responseUnpacker = responseUnpacker;

        msgLog.info( getClass().getSimpleName() + " communication channel created towards " + address );
    }

    protected Protocol createProtocol( int chunkSize, byte applicationProtocolVersion )
    {
        return new Protocol214( chunkSize, applicationProtocolVersion, getInternalProtocolVersion() );
    }

    @Override
    public void start()
    {
        bootstrap = new ClientBootstrap( new NioClientSocketChannelFactory(
                newCachedThreadPool( daemon( getClass().getSimpleName() + "-boss@" + address ) ),
                newCachedThreadPool( daemon( getClass().getSimpleName() + "-worker@" + address ) ) ) );
        bootstrap.setPipelineFactory( this );

        channelPool = new ResourcePool<ChannelContext>( maxUnusedChannels,
                new ResourcePool.CheckStrategy.TimeoutCheckStrategy( DEFAULT_CHECK_INTERVAL, SYSTEM_CLOCK ),
                new LoggingResourcePoolMonitor( msgLog ) )
        {
            @Override
            protected ChannelContext create()
            {
                ChannelFuture channelFuture = bootstrap.connect( address );
                channelFuture.awaitUninterruptibly( 5, TimeUnit.SECONDS );
                if ( channelFuture.isSuccess() )
                {
                    msgLog.logMessage( threadInfo() + "Opened a new channel to " + address, true );

                    return new ChannelContext( channelFuture.getChannel(), ChannelBuffers.dynamicBuffer(),
                            ByteBuffer.allocate( 1024 * 1024 ) );
                }

                String msg = Client.this.getClass().getSimpleName() + " could not connect to " + address;
                msgLog.logMessage( msg, true );
                throw new ComException( msg, channelFuture.getCause() );
            }

            @Override
            protected boolean isAlive( ChannelContext context )
            {
                return context.channel().isConnected();
            }

            @Override
            protected void dispose( ChannelContext context )
            {
                Channel channel = context.channel();
                if ( channel.isConnected() )
                {
                    msgLog.logMessage( threadInfo() + "Closing: " + context + ". " +
                                       "Channel pool size is now " + currentSize(), true );
                    channel.close();
                }
            }

            private String threadInfo()
            {
                return "Thread[" + Thread.currentThread().getId() + ", " + Thread.currentThread().getName() + "] ";
            }
        };
        /*
         * This is here to couple the channel releasing to Response.close() itself and not
         * to TransactionStream.close() as it is implemented here. The reason is that a Response
         * that is returned without a TransactionStream will still hold the channel and should
         * release it eventually. Also, logically, closing the channel is not dependent on the
         * TransactionStream.
         */
        resourcePoolReleaser = new ResourceReleaser()
        {
            @Override
            public void release()
            {
                channelPool.release();
            }
        };
    }


    @Override
    public void stop()
    {
        channelPool.close( true );
        bootstrap.releaseExternalResources();
        comExceptionHandler = ComExceptionHandler.NO_OP;
        msgLog.logMessage( toString() + " shutdown", true );
    }

    protected <R> Response<R> sendRequest( RequestType<T> type, RequestContext context,
            Serializer serializer, Deserializer<R> deserializer )
    {
        return sendRequest( type, context, serializer, deserializer, null, NO_OP_TX_HANDLER );
    }

    protected <R> Response<R> sendRequest( RequestType<T> type, RequestContext context,
            Serializer serializer, Deserializer<R> deserializer,
            StoreId specificStoreId, TxHandler txHandler )
    {
        ChannelContext channelContext = acquireChannelContext( type );

        Throwable failure = null;
        try
        {
            requestMonitor.beginRequest( channelContext.channel().getRemoteAddress(), type, context );

            // Request
            protocol.serializeRequest( channelContext.channel(), channelContext.output(), type, context, serializer );

            // Response
            Response<R> response = protocol.deserializeResponse( extractBlockingReadHandler( channelContext ),
                    channelContext.input(), getReadTimeout( type, readTimeout ), deserializer, resourcePoolReleaser );

            if ( type.responseShouldBeUnpacked() )
            {
                responseUnpacker.unpackResponse( response, txHandler );
            }

            if ( shouldCheckStoreId( type ) )
            {
                // specificStoreId is there as a workaround for then the graphDb isn't initialized yet
                if ( specificStoreId != null )
                {
                    assertCorrectStoreId( response.getStoreId(), specificStoreId );
                }
                else
                {
                    assertCorrectStoreId( response.getStoreId(), storeId );
                }
            }

            return response;
        }
        catch ( ComException e )
        {
            failure = e;
            comExceptionHandler.handle( e );
            throw e;
        }
        catch ( Throwable e )
        {
            failure = e;
            throw Exceptions.launderedException( ComException.class, e );
        }
        finally
        {
            /*
             * Otherwise the user must call response.close() to prevent resource leaks.
             */
            if ( failure != null )
            {
                dispose( channelContext );
            }
            requestMonitor.endRequest( failure );
        }
    }

    protected long getReadTimeout( RequestType<T> type, long readTimeout )
    {
        return readTimeout;
    }

    protected boolean shouldCheckStoreId( RequestType<T> type )
    {
        return true;
    }

    protected StoreId getStoreId()
    {
        return storeId;
    }

    private void assertCorrectStoreId( StoreId storeId, StoreId myStoreId )
    {
        if ( !myStoreId.equals( storeId ) )
        {
            throw new MismatchingStoreIdException( myStoreId, storeId );
        }
    }

    private ChannelContext acquireChannelContext( RequestType<T> type )
    {
        try
        {
            // Calling acquire is dangerous since it may be a blocking call... and if this
            // thread holds a lock which others may want to be able to communicate with
            // the server things go stiff.
            ChannelContext result = channelPool.acquire();
            if ( result == null )
            {
                msgLog.error( "Unable to acquire new channel for " + type );
                throw new ComException( "Unable to acquire new channel for " + type );
            }
            return result;
        }
        catch ( Throwable e )
        {
            throw Exceptions.launderedException( ComException.class, e );
        }
    }

    private void dispose( ChannelContext channelContext )
    {
        channelContext.channel().close().awaitUninterruptibly();
        channelPool.release();
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception
    {
        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast( MONITORING_CHANNEL_HANDLER_NAME, new MonitorChannelHandler( byteCounterMonitor ) );
        addLengthFieldPipes( pipeline, frameLength );
        BlockingReadHandler<ChannelBuffer> reader =
                new BlockingReadHandler<>( new ArrayBlockingQueue<ChannelEvent>( 100, false ) );
        pipeline.addLast( BLOCKING_CHANNEL_HANDLER_NAME, reader );
        return pipeline;
    }

    public void setComExceptionHandler( ComExceptionHandler handler )
    {
        comExceptionHandler = (handler == null) ? ComExceptionHandler.NO_OP : handler;
    }

    protected byte getInternalProtocolVersion()
    {
        return Server.INTERNAL_PROTOCOL_VERSION;
    }

    @SuppressWarnings( "unchecked" )
    private static BlockingReadHandler<ChannelBuffer> extractBlockingReadHandler( ChannelContext channelContext )
    {
        ChannelPipeline pipeline = channelContext.channel().getPipeline();
        return (BlockingReadHandler<ChannelBuffer>) pipeline.get( BLOCKING_CHANNEL_HANDLER_NAME );
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + address + "]";
    }
}