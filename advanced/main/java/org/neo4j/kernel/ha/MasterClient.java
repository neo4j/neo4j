package org.neo4j.kernel.ha;

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
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.jboss.netty.handler.queue.BlockingReadHandler;
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
    private final Deque<Channel> unusedChannels = new LinkedList<Channel>();
    private final Map<Thread, Channel> channels = new HashMap<Thread, Channel>();
    private final ClientBootstrap bootstrap;
    private final String hostNameOrIp;
    private final int port;

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
        msgLog.logMessage( "Client connected to " + hostNameOrIp + ":" + port );
    }
    
    public MasterClient( Machine machine, String storeDir )
    {
        this( machine.getServer().first(), machine.getServer().other(), storeDir );
    }

    private <T> Response<T> sendRequest( RequestType type,
            SlaveContext slaveContext, Serializer serializer, Deserializer<T> deserializer )
    {
        try
        {
            // Send 'em over the wire
            ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
            buffer.writeByte( type.ordinal() );
            if ( type.includesSlaveContext() )
            {
                writeSlaveContext( buffer, slaveContext );
            }
            serializer.write( buffer );
            Channel channel = getChannel();
            channel.write( buffer );
            BlockingReadHandler<ChannelBuffer> reader = (BlockingReadHandler<ChannelBuffer>)
                    channel.getPipeline().get( "blockingHandler" );

            ChannelBuffer message = reader.read( 20, TimeUnit.SECONDS );
            if ( message == null )
            {
                throw new HaCommunicationException( "Channel has been closed" );
            }
            T response = deserializer.read( message );
            TransactionStreams txStreams = type.includesSlaveContext() ?
                    readTransactionStreams( message ) : TransactionStreams.EMPTY;
            return new Response<T>( response, txStreams );
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
                            msgLog.logMessage( "Opened a new channel to " + hostNameOrIp + ":" + port );
                            break;
                        }
                        else
                        {
                            msgLog.logMessage( "Retrying connect to " + hostNameOrIp + ":" + port );
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
        return sendRequest( RequestType.ALLOCATE_IDS, null, new Serializer()
        {
            public void write( ChannelBuffer buffer ) throws IOException
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
            public void write( ChannelBuffer buffer ) throws IOException
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
            final String resource, final TransactionStream transactionStream )
    {
        return sendRequest( RequestType.COMMIT, context, new Serializer()
        {
            public void write( ChannelBuffer buffer ) throws IOException
            {
                writeString( buffer, resource );
                writeTransactionStream(buffer, transactionStream);
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
                public void write( ChannelBuffer buffer ) throws IOException
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
            public void write( ChannelBuffer buffer ) throws IOException
            {
                buffer.writeLong( txId );
            }
        }, INTEGER_DESERIALIZER ).response();
    }

    public ChannelPipeline getPipeline() throws Exception
    {
        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast( "frameDecoder", new LengthFieldBasedFrameDecoder( MAX_FRAME_LENGTH,
                0, 4, 0, 4 ) );
        pipeline.addLast( "frameEncoder", new LengthFieldPrepender( 4 ) );
        BlockingReadHandler<ChannelBuffer> reader = new BlockingReadHandler<ChannelBuffer>();
        pipeline.addLast( "blockingHandler", reader );
        return pipeline;
    }
    
    public void shutdown()
    {
        msgLog.logMessage( "MasterClient shutdown" );
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
