package org.neo4j.kernel.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.ha.IdAllocation;
import org.neo4j.kernel.impl.ha.LockResult;
import org.neo4j.kernel.impl.ha.LockStatus;
import org.neo4j.kernel.impl.ha.Master;
import org.neo4j.kernel.impl.ha.Response;
import org.neo4j.kernel.impl.ha.SlaveContext;
import org.neo4j.kernel.impl.ha.TransactionStream;
import org.neo4j.kernel.impl.ha.TransactionStreams;

/**
 * The {@link Master} a slave should use to communicate with its master. It
 * serializes requests and sends them to the master, more specifically
 * {@link MasterServer} (which delegates to {@link MasterImpl}
 * on the master side.
 */
public class MasterClient implements Master
{
    public static final int PORT = 8901;
    private static final int MEGA = 1024*1024;
    
    public static enum RequestType
    {
        ALLOCATE_IDS,
        CREATE_RELATIONSHIP_TYPE,
        ACQUIRE_NODE_WRITE_LOCK,
        ACQUIRE_NODE_READ_LOCK,
        ACQUIRE_RELATIONSHIP_WRITE_LOCK,
        ACQUIRE_RELATIONSHIP_READ_LOCK,
        COMMIT,
        ROLLBACK,
        PULL_UPDATES
    }

    private static Client initClient()
    {
        ExecutorService executor = Executors.newCachedThreadPool();
        ClientBootstrap bootstrap = new ClientBootstrap( new NioClientSocketChannelFactory(
                executor, executor ) );
        BlockingReadHandler<ChannelBuffer> blockingReadHandler = new BlockingReadHandler<ChannelBuffer>();
        bootstrap.setPipelineFactory( new ClientPipelineFactory( blockingReadHandler ) );
        ChannelFuture channelFuture = bootstrap.connect( new InetSocketAddress( PORT ) );
        return new Client( blockingReadHandler, channelFuture );
    }

    private final Client client;
    
    MasterClient()
    {
        client = initClient();
    }
    
    private <T> Response<T> sendRequest( RequestType type,
            SlaveContext slaveContext, Serializer serializer, Deserializer<T> deserializer )
    {
        try
        {
            // Send 'em over the wire
            ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
            buffer.writeByte( type.ordinal() );
            writeSlaveContext( buffer, slaveContext );
            serializer.write( buffer );
            client.channel.write(buffer);
            
            // Read response
            ChannelBuffer message = client.blockingReadHandler.read();
            T response = deserializer.read( message );
            TransactionStreams txStreams = readTransactionStreams( message );
            return new Response<T>( response, txStreams );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }

    public Response<IdAllocation> allocateIds( SlaveContext context, final IdType idType )
    {
        return sendRequest( RequestType.ALLOCATE_IDS, context, new Serializer()
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
        } );                
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
    
    private static abstract class AcquireLockSerializer<E extends PropertyContainer> implements Serializer
    {
        private final E[] entities;
        private final int eventIdentifier;

        AcquireLockSerializer( int eventIdentifier, E... entities )
        {
            this.eventIdentifier = eventIdentifier;
            this.entities = entities;
        }
        
        public void write( ChannelBuffer buffer ) throws IOException
        {
            buffer.writeInt( eventIdentifier );
            buffer.writeInt( entities.length );
            for ( E entity : entities )
            {
                buffer.writeLong( getId( entity ) );
            }
        }
        
        protected abstract long getId( E entity );
    }
    
    private static final Deserializer<LockResult> LOCK_RESULT_DESERIALIZER = new Deserializer<LockResult>()
    {
        public LockResult read( ChannelBuffer buffer ) throws IOException
        {
            LockStatus status = LockStatus.values()[buffer.readByte()];
            return status.hasMessage() ? new LockResult( readString( buffer ) ) :
                    new LockResult( status );
        }
    };
    private static final Deserializer<Void> VOID_DESERIALIZER = new Deserializer<Void>()
    {
        public Void read( ChannelBuffer buffer ) throws IOException
        {
            return null;
        }
    };
    private static final Serializer EMPTY_SERIALIZER = new Serializer()
    {
        public void write( ChannelBuffer buffer ) throws IOException
        {
        }
    };
    
    private static class AcquireNodeLockSerializer extends AcquireLockSerializer<Node>
    {
        AcquireNodeLockSerializer( int eventIdentifier, Node... entities )
        {
            super( eventIdentifier, entities );
        }

        @Override
        protected long getId( Node entity )
        {
            return entity.getId();
        }
    }
    
    private static class AcquireRelationshipLockSerializer extends AcquireLockSerializer<Relationship>
    {
        AcquireRelationshipLockSerializer( int eventIdentifier, Relationship... entities )
        {
            super( eventIdentifier, entities );
        }

        @Override
        protected long getId( Relationship entity )
        {
            return entity.getId();
        }
    }

    protected static String readString( ChannelBuffer buffer )
    {
        int length = buffer.readInt();
        char[] chars = new char[length];
        for ( int i = 0; i < length; i++ )
        {
            chars[i] = buffer.readChar();
        }
        return new String( chars );
    }

    public Response<LockResult> acquireWriteLock( SlaveContext context, final int eventIdentifier,
            final Node... nodes )
    {
        return sendRequest( RequestType.ACQUIRE_NODE_WRITE_LOCK, context,
                new AcquireNodeLockSerializer( eventIdentifier, nodes ), LOCK_RESULT_DESERIALIZER );
    }

    public Response<LockResult> acquireReadLock( SlaveContext context, int eventIdentifier,
            Node... nodes )
    {
        return sendRequest( RequestType.ACQUIRE_NODE_READ_LOCK, context,
                new AcquireNodeLockSerializer( eventIdentifier, nodes ), LOCK_RESULT_DESERIALIZER );
    }

    public Response<LockResult> acquireWriteLock( SlaveContext context, int eventIdentifier,
            Relationship... relationships )
    {
        return sendRequest( RequestType.ACQUIRE_RELATIONSHIP_WRITE_LOCK, context,
                new AcquireRelationshipLockSerializer( eventIdentifier, relationships ),
                LOCK_RESULT_DESERIALIZER );
    }

    public Response<LockResult> acquireReadLock( SlaveContext context, int eventIdentifier,
            Relationship... relationships )
    {
        return sendRequest( RequestType.ACQUIRE_RELATIONSHIP_READ_LOCK, context,
                new AcquireRelationshipLockSerializer( eventIdentifier, relationships ),
                LOCK_RESULT_DESERIALIZER );
    }

    public Response<Long> commitSingleResourceTransaction( SlaveContext context,
            int eventIdentifier, final String resource, final TransactionStream transactionStream )
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

    protected void writeTransactionStream( ChannelBuffer dest, TransactionStream transactionStream )
            throws IOException
    {
        final Collection<ReadableByteChannel> channels = transactionStream.getChannels();
        dest.writeInt( channels.size() );
        for ( ReadableByteChannel channel : channels )
        {
            ByteData data = new ByteData( channel );
            dest.writeInt( data.size() );
            for ( byte[] bytes : data )
            {
                dest.writeBytes( bytes );
            }
        }
    }

    private static class ByteData implements Iterable<byte[]>
    {
        private final Collection<byte[]> data;
        private final int size;
        
        ByteData( ReadableByteChannel channel ) throws IOException
        {
            int size = 0, chunk = 0;
            List<byte[]> data = new LinkedList<byte[]>();
            ByteBuffer buffer = ByteBuffer.allocateDirect( 1 * MEGA );
            while ( (chunk = channel.read( buffer )) >= 0 )
            {
                size += chunk;
                byte[] bytes = new byte[chunk];
                buffer.flip();
                buffer.get( bytes );
                buffer.clear();
                data.add( bytes );
            }
            this.data = data;
            this.size = size;
        }

        int size()
        {
            return size;
        }

        public Iterator<byte[]> iterator()
        {
            return data.iterator();
        }
    }
    
    public Response<Void> rollbackTransaction( SlaveContext context, int eventIdentifier )
    {
        return sendRequest( RequestType.ROLLBACK, context, EMPTY_SERIALIZER, VOID_DESERIALIZER );
    }

    public Response<Void> pullUpdates( SlaveContext context )
    {
        return sendRequest( RequestType.PULL_UPDATES, context, EMPTY_SERIALIZER, VOID_DESERIALIZER );
    }

    private static class Client
    {
        private BlockingReadHandler<ChannelBuffer> blockingReadHandler;
        private ChannelFuture channelFuture;
        private Channel channel;

        public Client( BlockingReadHandler<ChannelBuffer> blockingReadHandler,
                ChannelFuture channelFuture )
        {
            this.blockingReadHandler = blockingReadHandler;
            this.channelFuture = channelFuture;
            this.channel = channelFuture.getChannel();
        }

        public void close()
        {
            channel.close();
        }

        public void awaitConnected()
        {
            channelFuture.awaitUninterruptibly();
        }
    }

    private static class ClientPipelineFactory implements ChannelPipelineFactory
    {
        private BlockingReadHandler<ChannelBuffer> blockingReadHandler;

        public ClientPipelineFactory( BlockingReadHandler<ChannelBuffer> blockingReadHandler )
        {
            this.blockingReadHandler = blockingReadHandler;
        }

        public ChannelPipeline getPipeline() throws Exception
        {
            ChannelPipeline pipeline = Channels.pipeline();
            pipeline.addLast( "frameDecoder", new LengthFieldBasedFrameDecoder( 100000, 0, 4, 0, 4 ) );
            pipeline.addLast( "frameEncoder", new LengthFieldPrepender( 4 ) );
            pipeline.addLast( "blockingHandler", blockingReadHandler );
            return pipeline;
        }
    }
    
    static interface Serializer
    {
        void write( ChannelBuffer buffer ) throws IOException;
    }
    
    static interface Deserializer<T>
    {
        T read( ChannelBuffer buffer ) throws IOException;
    }

    protected static IdAllocation readIdAllocation( ChannelBuffer buffer )
    {
        int numberOfIds = buffer.readInt();
        long[] ids = new long[numberOfIds];
        for ( int i = 0; i < numberOfIds; i++ )
        {
            ids[i] = buffer.readLong();
        }
        long highId = buffer.readLong();
        long defragCount = buffer.readLong();
        return new IdAllocation( ids, highId, defragCount );
    }
    
    protected static void writeString( ChannelBuffer buffer, String name )
    {
        char[] chars = name.toCharArray();
        buffer.writeInt( chars.length );
        
        // TODO optimize?
        for ( char ch : chars )
        {
            buffer.writeChar( ch );
        }
    }

    protected static void writeSlaveContext( ChannelBuffer buffer, SlaveContext context )
    {
        buffer.writeInt( context.slaveId() );
        Map<String, Long> txs = context.lastAppliedTransactions();
        buffer.writeByte( txs.size() );
        for ( Map.Entry<String, Long> tx : txs.entrySet() )
        {
            writeString( buffer, tx.getKey() );
            buffer.writeLong( tx.getValue() );
        }
    }

    protected static TransactionStreams readTransactionStreams( ChannelBuffer message )
    {
        // TODO implement
        return new TransactionStreams();
    }
}
