/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static org.neo4j.com.Protocol.addLengthFieldPipes;
import static org.neo4j.com.Protocol.assertChunkSizeIsWithinFrameSize;
import static org.neo4j.com.Protocol.readString;
import static org.neo4j.com.Protocol.writeString;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import org.neo4j.com.RequestContext.Tx;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.helpers.Triplet;
import org.neo4j.kernel.impl.nioneo.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.tooling.RealClock;

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

    private ClientBootstrap bootstrap;
    private final SocketAddress address;
    private final StringLogger msgLog;
    private ExecutorService executor;
    private ResourcePool<Triplet<Channel, ChannelBuffer, ByteBuffer>> channelPool;
    private final int frameLength;
    private final long readTimeout;
    private final int maxUnusedChannels;
    private final byte applicationProtocolVersion;
    private final StoreId storeId;
    private ResourceReleaser resourcePoolReleaser;
    private final List<MismatchingVersionHandler> mismatchingVersionHandlers;

    private int chunkSize;

    public Client( String hostNameOrIp, int port, Logging logging,
            StoreId storeId, int frameLength,
            byte applicationProtocolVersion, long readTimeout,
            int maxConcurrentChannels, int maxUnusedPoolSize, int chunkSize )
    {
        assertChunkSizeIsWithinFrameSize( chunkSize, frameLength );
        
        this.msgLog = logging.getMessagesLog( getClass() );
        this.storeId = storeId;
        this.frameLength = frameLength;
        this.applicationProtocolVersion = applicationProtocolVersion;
        this.readTimeout = readTimeout;
        // ResourcePool no longer controls max concurrent channels. Use this value for the pool size
        this.maxUnusedChannels = maxConcurrentChannels;
        this.chunkSize = chunkSize;
        this.mismatchingVersionHandlers = new ArrayList<MismatchingVersionHandler>( 2 );
        address = new InetSocketAddress( hostNameOrIp, port );
        msgLog.logMessage( getClass().getSimpleName() + " communication channel created towards " + hostNameOrIp + ":" +
                port, true );
    }

    @Override
    public void start()
    {
        executor = Executors.newCachedThreadPool( new NamedThreadFactory( getClass().getSimpleName() + "@" + address ) );
        bootstrap = new ClientBootstrap( new NioClientSocketChannelFactory( executor, executor ) );
        bootstrap.setPipelineFactory( this );
        channelPool = new ResourcePool<Triplet<Channel, ChannelBuffer, ByteBuffer>>( maxUnusedChannels,
                new ResourcePool.CheckStrategy.TimeoutCheckStrategy( ResourcePool.DEFAULT_CHECK_INTERVAL, new RealClock() ),
                new LoggingResourcePoolMonitor())
        {
            @Override
            protected Triplet<Channel, ChannelBuffer, ByteBuffer> create()
            {
                ChannelFuture channelFuture = bootstrap.connect( address );
                channelFuture.awaitUninterruptibly( 5, TimeUnit.SECONDS );
                Triplet<Channel, ChannelBuffer, ByteBuffer> channel = null;
                if ( channelFuture.isSuccess() )
                {
                    channel = Triplet.of( channelFuture.getChannel(),
                            ChannelBuffers.dynamicBuffer(),
                            ByteBuffer.allocate( 1024 * 1024 ) );
                    msgLog.logMessage( "Opened a new channel to " + address, true );
                    return channel;
                }

                String msg = Client.this.getClass().getSimpleName()+" could not connect to " + address;
                msgLog.logMessage( msg, true );
                ComException exception = new ComException( msg );
                // connectionLostHandler.handle( exception );
                throw exception;
            }

            @Override
            protected boolean isAlive(
                    Triplet<Channel, ChannelBuffer, ByteBuffer> resource )
            {
                return resource.first().isConnected();
            }

            @Override
            protected void dispose(
                    Triplet<Channel, ChannelBuffer, ByteBuffer> resource )
            {
                Channel channel = resource.first();
                if ( channel.isConnected() )
                {
                    channel.close();
                }
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
        executor.shutdownNow();
        mismatchingVersionHandlers.clear();
        msgLog.logMessage( toString() + " shutdown", true );
    }

    /**
     * Only exposed so that tests can control it. It's not configurable really.
     */
    protected byte getInternalProtocolVersion()
    {
        return Server.INTERNAL_PROTOCOL_VERSION;
    }

    protected <R> Response<R> sendRequest( RequestType<T> type, RequestContext context,
                                           Serializer serializer, Deserializer<R> deserializer )
    {
        return sendRequest( type, context, serializer, deserializer, null );
    }

    protected <R> Response<R> sendRequest( RequestType<T> type, RequestContext context,
                                           Serializer serializer, Deserializer<R> deserializer,
                                           StoreId specificStoreId )
    {
        boolean success = true;
        Triplet<Channel, ChannelBuffer, ByteBuffer> channelContext = null;
        try
        {
            // Send 'em over the wire
            channelContext = getChannel( type );
            Channel channel = channelContext.first();
            channelContext.second().clear();
            ChunkingChannelBuffer chunkingBuffer = new ChunkingChannelBuffer( channelContext.second(),
                    channel, chunkSize, getInternalProtocolVersion(), applicationProtocolVersion );
            chunkingBuffer.writeByte( type.id() );
            writeContext( type, context, chunkingBuffer );
            serializer.write( chunkingBuffer, channelContext.third() );
            chunkingBuffer.done();

            // Read the response
            @SuppressWarnings("unchecked")
            BlockingReadHandler<ChannelBuffer> reader = (BlockingReadHandler<ChannelBuffer>)
                    channel.getPipeline().get( "blockingHandler" );
            DechunkingChannelBuffer dechunkingBuffer = new DechunkingChannelBuffer( reader, getReadTimeout( type,
                    readTimeout ),
                    getInternalProtocolVersion(), applicationProtocolVersion );

            R response = deserializer.read( dechunkingBuffer, channelContext.third() );
            StoreId storeId = readStoreId( dechunkingBuffer, channelContext.third() );
            if ( shouldCheckStoreId( type ) )
            {
                // specificStoreId is there as a workaround for then the graphDb isn't initialized yet
                if ( specificStoreId != null )
                {
                    assertCorrectStoreId( storeId, specificStoreId );
                }
                else
                {
                    assertCorrectStoreId( storeId, this.storeId );
                }
            }
            TransactionStream txStreams = readTransactionStreams(
                    dechunkingBuffer, channelPool );
            return new Response<R>( response, storeId, txStreams,
                    resourcePoolReleaser );
        }
        catch ( IllegalProtocolVersionException e )
        {
            success = false;
            for ( MismatchingVersionHandler handler : mismatchingVersionHandlers )
            {
                handler.versionMismatched( e.getExpected(), e.getReceived() );
            }
            throw e;
        }
        catch ( Throwable e )
        {
            success = false;
            if ( channelContext != null )
            {
                closeChannel( channelContext );
            }
            throw Exceptions.launderedException( ComException.class, e );
        }
        finally
        {
            /*
             * Otherwise the user must call response.close() to prevent resource leaks.
             */
            if ( !success )
            {
                releaseChannel( type, channelContext );
            }
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

    private StoreId readStoreId( ChannelBuffer source, ByteBuffer byteBuffer )
    {
        byteBuffer.clear();
        byteBuffer.limit( 8 + 8 + 8 );
        source.readBytes( byteBuffer );
        byteBuffer.flip();
        return StoreId.deserialize( byteBuffer );
    }

    protected void writeContext( RequestType<T> type, RequestContext context, ChannelBuffer targetBuffer )
    {
        targetBuffer.writeLong( context.getSessionId() );
        targetBuffer.writeInt( context.machineId() );
        targetBuffer.writeInt( context.getEventIdentifier() );
        Tx[] txs = context.lastAppliedTransactions();
        targetBuffer.writeByte( txs.length );
        for ( Tx tx : txs )
        {
            writeString( targetBuffer, tx.getDataSourceName() );
            targetBuffer.writeLong( tx.getTxId() );
        }
        targetBuffer.writeInt( context.getMasterId() );
        targetBuffer.writeLong( context.getChecksum() );
    }

    private Triplet<Channel, ChannelBuffer, ByteBuffer> getChannel( RequestType<T> type ) throws Exception
    {
        // Calling acquire is dangerous since it may be a blocking call... and if this
        // thread holds a lock which others may want to be able to communicate with
        // the server things go stiff.
        Triplet<Channel, ChannelBuffer, ByteBuffer> result = channelPool.acquire();
        if ( result == null )
        {
            msgLog.logMessage( "Unable to acquire new channel for " + type );
            throw new ComException( "Unable to acquire new channel for " + type );
        }
        return result;
    }

    protected void releaseChannel( RequestType<T> type, Triplet<Channel, ChannelBuffer, ByteBuffer> channel )
    {
        channelPool.release();
    }

    protected void closeChannel( Triplet<Channel, ChannelBuffer, ByteBuffer> channel )
    {
        channel.first().close().awaitUninterruptibly();
    }

    public ChannelPipeline getPipeline() throws Exception
    {
        ChannelPipeline pipeline = Channels.pipeline();
        addLengthFieldPipes( pipeline, frameLength );
        BlockingReadHandler<ChannelBuffer> reader = new BlockingReadHandler<ChannelBuffer>(
                new ArrayBlockingQueue<ChannelEvent>( 3, false ) );
        pipeline.addLast( "blockingHandler", reader );
        return pipeline;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + address + "]";
    }

    protected static TransactionStream readTransactionStreams(
            final ChannelBuffer buffer,
            final ResourcePool<Triplet<Channel, ChannelBuffer, ByteBuffer>> resourcePool )
    {
        final String[] datasources = readTransactionStreamHeader( buffer );

        if ( datasources.length == 1 )
        {
            return TransactionStream.EMPTY;
        }

        return new TransactionStream()
        {
            @Override
            protected Triplet<String, Long, TxExtractor> fetchNextOrNull()
            {
                makeSureNextTransactionIsFullyFetched( buffer );
                String datasource = datasources[buffer.readUnsignedByte()];
                if ( datasource == null )
                {
                    return null;
                }
                long txId = buffer.readLong();
                TxExtractor extractor = TxExtractor.create( new BlockLogReader( buffer ) );
                return Triplet.of( datasource, txId, extractor );
            }

            @Override
            public String[] dataSourceNames()
            {
                return Arrays.copyOfRange( datasources, 1, datasources.length );
            }
        };
    }

    protected static String[] readTransactionStreamHeader( ChannelBuffer buffer )
    {
        short numberOfDataSources = buffer.readUnsignedByte();
        final String[] datasources = new String[numberOfDataSources + 1];
        datasources[0] = null; // identifier for "no more transactions"
        for ( int i = 1; i < datasources.length; i++ )
        {
            datasources[i] = readString( buffer );
        }
        return datasources;
    }

    private static void makeSureNextTransactionIsFullyFetched( ChannelBuffer buffer )
    {
        buffer.markReaderIndex();
        try
        {
            if ( buffer.readUnsignedByte() > 0 /* datasource id */ )
            {
                buffer.skipBytes( 8 ); // tx id
                int blockSize = 0;
                while ( (blockSize = buffer.readUnsignedByte()) == 0 )
                {
                    buffer.skipBytes( BlockLogBuffer.DATA_SIZE );
                }
                buffer.skipBytes( blockSize );
            }
        }
        finally
        {
            buffer.resetReaderIndex();
        }
    }

    public void addMismatchingVersionHandler( MismatchingVersionHandler toAdd )
    {
        mismatchingVersionHandlers.add( toAdd );
    }

    private class LoggingResourcePoolMonitor extends ResourcePool.Monitor.Adapter<Triplet<Channel, ChannelBuffer, ByteBuffer>>
    {
        @Override
        public void updatedCurrentPeakSize( int currentPeakSize )
        {
            msgLog.debug( "ResourcePool updated currentPeakSize to " + currentPeakSize );
        }

        @Override
        public void created( Triplet<Channel, ChannelBuffer, ByteBuffer> resource  )
        {
            msgLog.debug( "ResourcePool create resource " + resource );
        }

        @Override
        public void updatedTargetSize( int targetSize )
        {
            msgLog.debug( "ResourcePool updated targetSize to " + targetSize );
        }
    }
}
