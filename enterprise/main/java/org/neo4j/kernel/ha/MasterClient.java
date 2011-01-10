/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.kernel.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
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
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Triplet;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.ha.zookeeper.Machine;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * The {@link Master} a slave should use to communicate with its master. It
 * serializes requests and sends them to the master, more specifically
 * {@link MasterServer} (which delegates to {@link MasterImpl}
 * on the master side.
 */
public class MasterClient extends CommunicationProtocol implements Master, ChannelPipelineFactory
{
    public static final int MAX_NUMBER_OF_CONCURRENT_REQUESTS_PER_CLIENT = 20;
    public static final int READ_RESPONSE_TIMEOUT_SECONDS = 20;
    private static final int MAX_NUMBER_OF_UNUSED_CHANNELS = 5;

    private final ClientBootstrap bootstrap;
    private final SocketAddress address;
    private final StringLogger msgLog;
    private final ExecutorService executor;
    private final ResourcePool<Triplet<Channel, ChannelBuffer, ByteBuffer>> channelPool =
        new ResourcePool<Triplet<Channel, ChannelBuffer, ByteBuffer>>(
            MAX_NUMBER_OF_CONCURRENT_REQUESTS_PER_CLIENT, MAX_NUMBER_OF_UNUSED_CHANNELS )
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

            String msg = "MasterClient could not connect to " + address;
            msgLog.logMessage( msg, true );
            throw new HaCommunicationException( msg );
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

    public MasterClient( String hostNameOrIp, int port, String storeDir )
    {
        this.address = new InetSocketAddress( hostNameOrIp, port );
        executor = Executors.newCachedThreadPool();
        bootstrap = new ClientBootstrap( new NioClientSocketChannelFactory(
                executor, executor ) );
        bootstrap.setPipelineFactory( this );
        msgLog = StringLogger.getLogger( storeDir + "/messages.log" );
        msgLog.logMessage( "Client connected to " + hostNameOrIp + ":" + port, true );
    }

    public MasterClient( Machine machine, String storeDir )
    {
        this( machine.getServer().first(), machine.getServer().other(), storeDir );
    }

    private <T> Response<T> sendRequest( RequestType type,
            SlaveContext slaveContext, Serializer serializer, Deserializer<T> deserializer )
    {
        // TODO Refactor, break into smaller methods
        Triplet<Channel, ChannelBuffer, ByteBuffer> channelContext = null;
        try
        {
            // Send 'em over the wire
            channelContext = getChannel();
            Channel channel = channelContext.first();
            ChannelBuffer buffer = channelContext.second();
            buffer.clear();
            buffer = new ChunkingChannelBuffer( buffer, channel, MAX_FRAME_LENGTH );
            buffer.writeByte( type.ordinal() );
            if ( type.includesSlaveContext() )
            {
                writeSlaveContext( buffer, slaveContext );
            }
            serializer.write( buffer, channelContext.third() );
            if ( buffer.writerIndex() > 0 )
            {
                channel.write( buffer );
            }

            // Read the response
            @SuppressWarnings( "unchecked" )
            BlockingReadHandler<ChannelBuffer> reader = (BlockingReadHandler<ChannelBuffer>)
                    channel.getPipeline().get( "blockingHandler" );
            final Triplet<Channel, ChannelBuffer, ByteBuffer> finalChannelContext = channelContext;
            DechunkingChannelBuffer dechunkingBuffer = new DechunkingChannelBuffer( reader )
            {
                @Override
                protected ChannelBuffer readNext()
                {
                    ChannelBuffer result = super.readNext();
                    if ( result == null )
                    {
                        channelPool.dispose( finalChannelContext );
                        throw new HaCommunicationException( "Channel has been closed" );
                    }
                    return result;
                }
            };
            T response = deserializer.read( dechunkingBuffer );
            TransactionStream txStreams = type.includesSlaveContext() ?
                    readTransactionStreams( dechunkingBuffer ) : TransactionStream.EMPTY;
            return new Response<T>( response, txStreams );
        }
        catch ( ClosedChannelException e )
        {
            channelPool.dispose( channelContext );
            throw new HaCommunicationException( e );
        }
        catch ( IOException e )
        {
            throw new HaCommunicationException( e );
        }
        catch ( InterruptedException e )
        {
            throw new HaCommunicationException( e );
        }
        catch ( Exception e )
        {
            throw new HaCommunicationException( e );
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

    public IdAllocation allocateIds( final IdType idType )
    {
        return sendRequest( RequestType.ALLOCATE_IDS, null, new Serializer()
        {
            public void write( ChannelBuffer buffer, ByteBuffer readBuffer ) throws IOException
            {
                buffer.writeByte( idType.ordinal() );
            }
        }, new Deserializer<IdAllocation>()
        {
            public IdAllocation read( ChannelBuffer buffer ) throws IOException
            {
                return readIdAllocation( buffer );
            }
        } ).response();
    }

    public Response<Integer> createRelationshipType( SlaveContext context, final String name )
    {
        return sendRequest( RequestType.CREATE_RELATIONSHIP_TYPE, context, new Serializer()
        {
            public void write( ChannelBuffer buffer, ByteBuffer readBuffer ) throws IOException
            {
                writeString( buffer, name );
            }
        }, new Deserializer<Integer>()
        {
            @SuppressWarnings( "boxing" )
            public Integer read( ChannelBuffer buffer ) throws IOException
            {
                return buffer.readInt();
            }
        } );
    }

    public Response<LockResult> acquireNodeWriteLock( SlaveContext context, long... nodes )
    {
        return sendRequest( RequestType.ACQUIRE_NODE_WRITE_LOCK, context,
                new AcquireLockSerializer( nodes ), LOCK_RESULT_DESERIALIZER );
    }

    public Response<LockResult> acquireNodeReadLock( SlaveContext context, long... nodes )
    {
        return sendRequest( RequestType.ACQUIRE_NODE_READ_LOCK, context,
                new AcquireLockSerializer( nodes ), LOCK_RESULT_DESERIALIZER );
    }

    public Response<LockResult> acquireRelationshipWriteLock( SlaveContext context,
            long... relationships )
    {
        return sendRequest( RequestType.ACQUIRE_RELATIONSHIP_WRITE_LOCK, context,
                new AcquireLockSerializer( relationships ), LOCK_RESULT_DESERIALIZER );
    }

    public Response<LockResult> acquireRelationshipReadLock( SlaveContext context,
            long... relationships )
    {
        return sendRequest( RequestType.ACQUIRE_RELATIONSHIP_READ_LOCK, context,
                new AcquireLockSerializer( relationships ), LOCK_RESULT_DESERIALIZER );
    }

    public Response<Long> commitSingleResourceTransaction( SlaveContext context,
            final String resource, final TxExtractor txGetter )
    {
        return sendRequest( RequestType.COMMIT, context, new Serializer()
        {
            public void write( ChannelBuffer buffer, ByteBuffer readBuffer ) throws IOException
            {
                writeString( buffer, resource );
                BlockLogBuffer blockLogBuffer = new BlockLogBuffer( buffer );
                txGetter.extract( blockLogBuffer );
                blockLogBuffer.done();
            }
        }, new Deserializer<Long>()
        {
            @SuppressWarnings( "boxing" )
            public Long read( ChannelBuffer buffer ) throws IOException
            {
                return buffer.readLong();
            }
        });
    }

    public Response<Void> finishTransaction( SlaveContext context )
    {
        try
        {
            return sendRequest( RequestType.FINISH, context, new Serializer()
            {
                public void write( ChannelBuffer buffer, ByteBuffer readBuffer ) throws IOException
                {
                }
            }, VOID_DESERIALIZER );
        }
        finally
        {
            releaseChannel();
        }
    }

    public void rollbackOngoingTransactions( SlaveContext context )
    {
        throw new UnsupportedOperationException( "Should never be called from the client side" );
    }

    public Response<Void> pullUpdates( SlaveContext context )
    {
        return sendRequest( RequestType.PULL_UPDATES, context, EMPTY_SERIALIZER, VOID_DESERIALIZER );
    }

    public int getMasterIdForCommittedTx( final long txId )
    {
        return sendRequest( RequestType.GET_MASTER_ID_FOR_TX, null, new Serializer()
        {
            public void write( ChannelBuffer buffer, ByteBuffer readBuffer ) throws IOException
            {
                buffer.writeLong( txId );
            }
        }, INTEGER_DESERIALIZER ).response();
    }

    public Response<Void> copyStore( SlaveContext context, final StoreWriter writer )
    {
        context = new SlaveContext( context.machineId(), context.getEventIdentifier(), new Pair[0] );

        return sendRequest( RequestType.COPY_STORE, context, EMPTY_SERIALIZER, new Deserializer<Void>()
        {
            // NOTICE: this assumes a "smart" ChannelBuffer that continues to next chunk
            public Void read( ChannelBuffer buffer ) throws IOException
            {
                int pathLength;
                while ( 0 != ( pathLength = buffer.readUnsignedShort() ) )
                {
                    String path = readString( buffer, pathLength );
                    writer.write( path, new BlockLogReader( buffer ) );
                }
                writer.done();
                return null;
            }
        } );
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
        msgLog.logMessage( "MasterClient shutdown", true );
        channelPool.close( true );
        executor.shutdownNow();
    }
}
