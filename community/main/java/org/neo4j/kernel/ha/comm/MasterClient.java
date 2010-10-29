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

package org.neo4j.kernel.ha.comm;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.queue.BlockingReadHandler;
import org.jboss.netty.handler.stream.ChunkedWriteHandler;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.ha.HaCommunicationException;
import org.neo4j.kernel.ha.IdAllocation;
import org.neo4j.kernel.ha.LockResult;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.ha.MasterImpl;
import org.neo4j.kernel.ha.Response;
import org.neo4j.kernel.ha.SlaveContext;
import org.neo4j.kernel.ha.TransactionStream;
import org.neo4j.kernel.ha.zookeeper.Machine;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * The {@link Master} a slave should use to communicate with its master. It
 * serializes requests and sends them to the master, more specifically
 * {@link MasterServer} (which delegates to {@link MasterImpl}
 * on the master side.
 */
public class MasterClient implements Master, ChannelPipelineFactory
{
    private final Deque<Channel> unusedChannels = new LinkedList<Channel>();
    private final Map<Thread, Channel> channels = new HashMap<Thread, Channel>();
    private final ClientBootstrap bootstrap;
    private final String hostNameOrIp;
    private final int port;
    private final TransactionApplier txApplier = new TransactionApplier();

    private final StringLogger msgLog;

    public MasterClient( String hostNameOrIp, int port, String storeDir )
    {
        this.hostNameOrIp = hostNameOrIp;
        this.port = port;
        ExecutorService executor = Executors.newCachedThreadPool();
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

    private <T> Response<T> sendRequest( RequestType type, SlaveContext slaveContext,
            DataWriter serializer )
    {
        try
        {
            // FIXME: this should be slightly restructured
            // Send 'em over the wire
            ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
            buffer.writeByte( type.ordinal() );
            if ( type.includesSlaveContext )
            {
                CommunicationUtils.writeSlaveContext( slaveContext, buffer );
            }
            serializer.write( buffer );
            Channel channel = getChannel();
            channel.write( buffer );
            @SuppressWarnings( "unchecked" )
            BlockingReadHandler<ChannelBuffer> reader = (BlockingReadHandler<ChannelBuffer>)
                    channel.getPipeline().get( "blockingHandler" );

            ChannelBuffer message = reader.read( 20, TimeUnit.SECONDS );
            if ( message == null )
            {
                throw new HaCommunicationException( "Channel has been closed" );
            }
            @SuppressWarnings( "unchecked" ) T response = (T) type.readResponse( message );
            return Response.wrapResponseObjectOnly( response );
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

    private Channel getChannel() throws Exception
    {
        Thread thread = Thread.currentThread();
        synchronized ( channels )
        {
            Channel channel = channels.get( thread );
            if ( channel == null )
            {
                // Get unused channel from the channel pool
                while ( channel == null )
                {
                    Channel unusedChannel = unusedChannels.poll();
                    if ( unusedChannel == null )
                    {
                        break;
                    }
                    else if ( unusedChannel.isConnected() )
                    {
                        msgLog.logMessage( "Found unused (and still connected) channel" );
                        channel = unusedChannel;
                    }
                    else
                    {
                        msgLog.logMessage( "Found unused stale channel, discarding it" );
                    }
                }

                // No unused channel found, create a new one
                if ( channel == null )
                {
                    for ( int i = 0; i < 5; i++ )
                    {
                        ChannelFuture channelFuture = bootstrap.connect(
                                new InetSocketAddress( hostNameOrIp, port ) );
                        channelFuture.awaitUninterruptibly();
                        if ( channelFuture.isSuccess() )
                        {
                            channel = channelFuture.getChannel();
                            msgLog.logMessage( "Opened a new channel to " + hostNameOrIp + ":" + port, true );
                            break;
                        }
                        else
                        {
                            msgLog.logMessage( "Retrying connect to " + hostNameOrIp + ":" + port, true );
                            try
                            {
                                Thread.sleep( 500 );
                            }
                            catch ( InterruptedException e )
                            {
                                Thread.interrupted();
                            }
                        }
                    }
                }

                if ( channel == null )
                {
                    throw new IOException( "Not able to connect to master" );
                }

                channels.put( thread, channel );
            }
            return channel;
        }
    }

   private void releaseChannel()
    {
        // Release channel for this thread
        synchronized ( channels )
        {
            Channel channel = channels.remove( Thread.currentThread() );
            if ( channel != null )
            {
                if ( unusedChannels.size() < 5 )
                {
                    unusedChannels.push( channel );
                }
                else
                {
                    channel.close();
                }
            }
        }
    }

    public IdAllocation allocateIds( final IdType idType )
    {
        return this.<IdAllocation>sendRequest( RequestType.allocateIds( idType ), null, null ).response();
    }

    public Response<Integer> createRelationshipType( SlaveContext context, final String name )
    {
        return this.sendRequest( RequestType.CREATE_RELATIONSHIP_TYPE, context,
                new DataWriter.WriteString( name ) );
    }

    public Response<LockResult> acquireNodeWriteLock( SlaveContext context, long... nodes )
    {
        return this.sendRequest( RequestType.ACQUIRE_NODE_WRITE_LOCK, context,
                new DataWriter.WriteIdArray( nodes ) );
    }

    public Response<LockResult> acquireNodeReadLock( SlaveContext context, long... nodes )
    {
        return this.sendRequest( RequestType.ACQUIRE_NODE_READ_LOCK, context,
                new DataWriter.WriteIdArray( nodes ) );
    }

    public Response<LockResult> acquireRelationshipWriteLock( SlaveContext context,
            long... relationships )
    {
        return this.sendRequest( RequestType.ACQUIRE_RELATIONSHIP_WRITE_LOCK, context,
                new DataWriter.WriteIdArray( relationships ) );
    }

    public Response<LockResult> acquireRelationshipReadLock( SlaveContext context,
            long... relationships )
    {
        return this.sendRequest( RequestType.ACQUIRE_RELATIONSHIP_READ_LOCK, context,
                new DataWriter.WriteIdArray( relationships ) );
    }

    public Response<Long> commitSingleResourceTransaction( SlaveContext context,
            final String resource, final TransactionStream transactionStream )
    {
        if ( true != false )
        {
            throw new UnsupportedOperationException(
                    "Not implemented: commitSingleResourceTransaction()" );
        }
        return sendRequest( RequestType.COMMIT, context, null );
    }

    public Response<Void> finishTransaction( SlaveContext context )
    {
        return this.sendRequest( RequestType.FINISH, context, null );
    }

    public void rollbackOngoingTransactions( SlaveContext context )
    {
        throw new UnsupportedOperationException( "Should never be called from the client side" );
    }

    public Response<Void> pullUpdates( SlaveContext context )
    {
        return sendRequest( RequestType.PULL_UPDATES, context, null );
    }

    @SuppressWarnings( "boxing" )
    public int getMasterIdForCommittedTx( long txId )
    {
        return this.<Integer>sendRequest( RequestType.GET_MASTER_ID_FOR_TX, null,
                new DataWriter.WriteLong( txId ) ).response();
    }

    public ChannelPipeline getPipeline() throws Exception
    {
        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast( "chunkedWriter", new ChunkedWriteHandler() );
        pipeline.addLast( "responseDecoder", new ResponseDecoder() );
        pipeline.addLast( "applyTransactions", txApplier );
        BlockingReadHandler<ChannelBuffer> reader = new BlockingReadHandler<ChannelBuffer>();
        pipeline.addLast( "blockingHandler", reader );
        return pipeline;
    }

    public void shutdown()
    {
        msgLog.logMessage( "MasterClient shutdown", true );
        synchronized ( channels )
        {
            for ( Channel channel : unusedChannels )
            {
                channel.close();
            }

            for ( Channel channel : channels.values() )
            {
                channel.close();
            }
        }
    }
}
