/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
import static org.neo4j.com.Protocol.readString;
import static org.neo4j.com.Protocol.writeString;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
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
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Triplet;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * A means for a client to communicate with a {@link Server}. It
 * serializes requests and sends them to the server and waits for
 * a response back.
 */
public abstract class Client<M> implements ChannelPipelineFactory
{
    public static final int DEFAULT_MAX_NUMBER_OF_CONCURRENT_REQUESTS_PER_CLIENT = 20;
    public static final int DEFAULT_READ_RESPONSE_TIMEOUT_SECONDS = 20;
    private static final int DEFAULT_MAX_NUMBER_OF_UNUSED_CHANNELS = 5;

    private final ClientBootstrap bootstrap;
    private final SocketAddress address;
    private final StringLogger msgLog;
    private final ExecutorService executor;
    private final ResourcePool<Triplet<Channel, ChannelBuffer, ByteBuffer>> channelPool;
    private final GraphDatabaseService graphDb;
    private StoreId myStoreId;

    public Client( String hostNameOrIp, int port, GraphDatabaseService graphDb )
    {
        this( hostNameOrIp, port, graphDb, DEFAULT_MAX_NUMBER_OF_CONCURRENT_REQUESTS_PER_CLIENT,
                DEFAULT_READ_RESPONSE_TIMEOUT_SECONDS, DEFAULT_MAX_NUMBER_OF_UNUSED_CHANNELS );
    }
    
    public Client( String hostNameOrIp, int port, GraphDatabaseService graphDb, int maxConcurrentTransactions,
            int readResponseTimeoutSeconds, int maxUnusedPoolSize )
    {
        this.graphDb = graphDb;
        channelPool = new ResourcePool<Triplet<Channel, ChannelBuffer, ByteBuffer>>(
                maxConcurrentTransactions, maxUnusedPoolSize )
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
                                          ByteBuffer.allocateDirect( 1024 * 1024 ) );
                    msgLog.logMessage( "Opened a new channel to " + address, true );
                    return channel;
                }

                // TODO Here it would be neat if we could ask the db to find us a new master
                // and if this still will be a slave then retry to connect.

                String msg = "Client could not connect to " + address;
                msgLog.logMessage( msg, true );
                throw new ComException( msg );
            }

            @Override
            protected boolean isAlive( Triplet<Channel, ChannelBuffer, ByteBuffer> resource )
            {
                return resource.first().isConnected();
            }

            @Override
            protected void dispose( Triplet<Channel, ChannelBuffer, ByteBuffer> resource )
            {
                Channel channel = resource.first();
                if ( channel.isConnected() ) channel.close();
            }
        };

        address = new InetSocketAddress( hostNameOrIp, port );
        executor = Executors.newCachedThreadPool();
        bootstrap = new ClientBootstrap( new NioClientSocketChannelFactory( executor, executor ) );
        bootstrap.setPipelineFactory( this );
        String storeDir = ((AbstractGraphDatabase) graphDb).getStoreDir();
        msgLog = StringLogger.getLogger( storeDir );
        msgLog.logMessage( "Client connected to " + hostNameOrIp + ":" + port, true );
    }

    protected <R> Response<R> sendRequest( RequestType<M> type, SlaveContext context,
            Serializer serializer, Deserializer<R> deserializer )
    {
        Triplet<Channel, ChannelBuffer, ByteBuffer> channelContext = null;
        try
        {
            // Send 'em over the wire
            channelContext = getChannel();
            Channel channel = channelContext.first();
            channelContext.second().clear();
            
            ChunkingChannelBuffer chunkingBuffer = new ChunkingChannelBuffer( channelContext.second(),
                    channel, Protocol.MAX_FRAME_LENGTH );
            chunkingBuffer.writeByte( type.id() );
            writeContext( type, context, chunkingBuffer );
            serializer.write( chunkingBuffer, channelContext.third() );
            chunkingBuffer.done();

            // Read the response
            @SuppressWarnings( "unchecked" )
            BlockingReadHandler<ChannelBuffer> reader = (BlockingReadHandler<ChannelBuffer>)
                    channel.getPipeline().get( "blockingHandler" );
            final Triplet<Channel, ChannelBuffer, ByteBuffer> finalChannelContext = channelContext;
            DechunkingChannelBuffer dechunkingBuffer = new DechunkingChannelBuffer( reader, DEFAULT_READ_RESPONSE_TIMEOUT_SECONDS )
            {
                @Override
                protected ChannelBuffer readNext()
                {
                    ChannelBuffer result = super.readNext();
                    if ( result == null )
                    {
                        channelPool.dispose( finalChannelContext );
                        throw new ComException( "Channel has been closed" );
                    }
                    return result;
                }
            };
            R response = deserializer.read( dechunkingBuffer, channelContext.third() );
            StoreId storeId = readStoreId( dechunkingBuffer, channelContext.third() );
            if ( shouldCheckStoreId( type ) )
            {
                assertCorrectStoreId( storeId );
            }
            TransactionStream txStreams = readTransactionStreams( dechunkingBuffer );
            return new Response<R>( response, storeId, txStreams );
        }
        catch ( ClosedChannelException e )
        {
            channelPool.dispose( channelContext );
            throw new ComException( e );
        }
        catch ( IOException e )
        {
            throw new ComException( e );
        }
        catch ( InterruptedException e )
        {
            throw new ComException( e );
        }
        catch ( Exception e )
        {
            throw new ComException( e );
        }
        finally
        {
            releaseChannel();
        }
    }

    protected boolean shouldCheckStoreId( RequestType<M> type )
    {
        return true;
    }

    private void assertCorrectStoreId( StoreId storeId )
    {
        StoreId myStoreId = getMyStoreId();
        if ( !myStoreId.equals( storeId ) )
        {
            throw new ComException( storeId + " from response doesn't match my " + myStoreId );
        }
    }

    protected StoreId getMyStoreId()
    {
        if ( myStoreId == null )
        {
            XaDataSource ds = ((AbstractGraphDatabase) graphDb).getConfig().getTxModule()
                    .getXaDataSourceManager().getXaDataSource( Config.DEFAULT_DATA_SOURCE_NAME );
            myStoreId = ((NeoStoreXaDataSource) ds).getStoreId();
        }
        return myStoreId;
    }

    private StoreId readStoreId( ChannelBuffer source, ByteBuffer byteBuffer )
    {
        byteBuffer.clear();
        byteBuffer.limit( 16 );
        source.readBytes( byteBuffer );
        byteBuffer.flip();
        return StoreId.deserialize( byteBuffer );
    }

    protected void writeContext( RequestType<M> type, SlaveContext context, ChannelBuffer targetBuffer )
    {
        targetBuffer.writeInt( context.machineId() );
        targetBuffer.writeInt( context.getEventIdentifier() );
        Pair<String, Long>[] txs = context.lastAppliedTransactions();
        targetBuffer.writeByte( txs.length );
        for ( Pair<String, Long> tx : txs )
        {
            writeString( targetBuffer, tx.first() );
            targetBuffer.writeLong( tx.other() );
        }
    }

    private Triplet<Channel, ChannelBuffer, ByteBuffer> getChannel() throws Exception
    {
        return channelPool.acquire();
    }

    private void releaseChannel()
    {
        channelPool.release();
    }

    public ChannelPipeline getPipeline() throws Exception
    {
        ChannelPipeline pipeline = Channels.pipeline();
        addLengthFieldPipes( pipeline );
        BlockingReadHandler<ChannelBuffer> reader = new BlockingReadHandler<ChannelBuffer>(
                new ArrayBlockingQueue<ChannelEvent>( 3, false ) );
        pipeline.addLast( "blockingHandler", reader );
        return pipeline;
    }

    public void shutdown()
    {
        msgLog.logMessage( getClass().getSimpleName() + " shutdown", true );
        channelPool.close( true );
        executor.shutdownNow();
    }

    protected static TransactionStream readTransactionStreams( final ChannelBuffer buffer )
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
                if ( datasource == null ) return null;
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
}
