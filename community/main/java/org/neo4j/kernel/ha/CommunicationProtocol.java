package org.neo4j.kernel.ha;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.ha.IdAllocation;
import org.neo4j.kernel.impl.ha.LockResult;
import org.neo4j.kernel.impl.ha.LockStatus;
import org.neo4j.kernel.impl.ha.Master;
import org.neo4j.kernel.impl.ha.Response;
import org.neo4j.kernel.impl.ha.SlaveContext;
import org.neo4j.kernel.impl.ha.TransactionStream;
import org.neo4j.kernel.impl.ha.TransactionStreams;

abstract class CommunicationProtocol
{
    public static final int PORT = 8901;
    private static final int MEGA = 1024 * 1024;
    static final ObjectSerializer<Integer> INTEGER_SERIALIZER = new ObjectSerializer<Integer>()
    {
        @SuppressWarnings( "boxing" )
        public void write( Integer responseObject, ChannelBuffer result ) throws IOException
        {
            result.writeInt( responseObject );
        }
    };
    static final ObjectSerializer<Long> LONG_SERIALIZER = new ObjectSerializer<Long>()
    {
        @SuppressWarnings( "boxing" )
        public void write( Long responseObject, ChannelBuffer result ) throws IOException
        {
            result.writeLong( responseObject );
        }
    };
    static final ObjectSerializer<Void> VOID_SERIALIZER = new ObjectSerializer<Void>()
    {
        public void write( Void responseObject, ChannelBuffer result ) throws IOException
        {
        }
    };
    static final ObjectSerializer<LockResult> LOCK_SERIALIZER = new ObjectSerializer<LockResult>()
    {
        public void write( LockResult responseObject, ChannelBuffer result ) throws IOException
        {
            result.writeByte( responseObject.getStatus().ordinal() );
            if ( responseObject.getStatus().hasMessage() )
            {
                writeString( result, responseObject.getDeadlockMessage() );
            }
        }
    };
    protected static final Deserializer<LockResult> LOCK_RESULT_DESERIALIZER = new Deserializer<LockResult>()
    {
        public LockResult read( ChannelBuffer buffer ) throws IOException
        {
            LockStatus status = LockStatus.values()[buffer.readByte()];
            return status.hasMessage() ? new LockResult( readString( buffer ) ) : new LockResult(
                    status );
        }
    };
    protected static final Deserializer<Void> VOID_DESERIALIZER = new Deserializer<Void>()
    {
        public Void read( ChannelBuffer buffer ) throws IOException
        {
            return null;
        }
    };
    protected static final Serializer EMPTY_SERIALIZER = new Serializer()
    {
        public void write( ChannelBuffer buffer ) throws IOException
        {
        }
    };

    public static enum RequestType
    {
        ALLOCATE_IDS( new MasterCaller<IdAllocation>()
        {
            public Response<IdAllocation> callMaster( Master master, SlaveContext context,
                    ChannelBuffer input )
            {
                IdType idType = IdType.values()[input.readByte()];
                return master.allocateIds( context, idType );
            }
        }, new ObjectSerializer<IdAllocation>()
        {
            public void write( IdAllocation idAllocation, ChannelBuffer result ) throws IOException
            {
                result.writeInt( idAllocation.getIds().length );
                for ( long id : idAllocation.getIds() )
                {
                    result.writeLong( id );
                }
                result.writeLong( idAllocation.getHighestIdInUse() );
                result.writeLong( idAllocation.getDefragCount() );
            }
        } ),
        CREATE_RELATIONSHIP_TYPE( new MasterCaller<Integer>()
        {
            public Response<Integer> callMaster( Master master, SlaveContext context,
                    ChannelBuffer input )
            {
                return master.createRelationshipType( context, readString( input ) );
            }
        }, INTEGER_SERIALIZER ),
        ACQUIRE_NODE_WRITE_LOCK( new AquireLockCall()
        {
            @Override
            Response<LockResult> lock( Master master, SlaveContext context, int eventIdentifier,
                    long... ids )
            {
                return master.acquireNodeWriteLock( context, eventIdentifier, ids );
            }
        }, LOCK_SERIALIZER ),
        ACQUIRE_NODE_READ_LOCK( new AquireLockCall()
        {
            @Override
            Response<LockResult> lock( Master master, SlaveContext context, int eventIdentifier,
                    long... ids )
            {
                return master.acquireNodeReadLock( context, eventIdentifier, ids );
            }
        }, LOCK_SERIALIZER ),
        ACQUIRE_RELATIONSHIP_WRITE_LOCK( new AquireLockCall()
        {
            @Override
            Response<LockResult> lock( Master master, SlaveContext context, int eventIdentifier,
                    long... ids )
            {
                return master.acquireRelationshipWriteLock( context, eventIdentifier, ids );
            }
        }, LOCK_SERIALIZER ),
        ACQUIRE_RELATIONSHIP_READ_LOCK( new AquireLockCall()
        {
            @Override
            Response<LockResult> lock( Master master, SlaveContext context, int eventIdentifier,
                    long... ids )
            {
                return master.acquireRelationshipReadLock( context, eventIdentifier, ids );
            }
        }, LOCK_SERIALIZER ),
        COMMIT( new MasterCaller<Long>()
        {
            public Response<Long> callMaster( Master master, SlaveContext context,
                    ChannelBuffer input )
            {
                int eventIdentifier = input.readInt();
                String resource = readString( input );
                TransactionStream transactionStream = readTransactionStream( input );
                return master.commitSingleResourceTransaction( context, eventIdentifier, resource,
                        transactionStream );
            }
        }, LONG_SERIALIZER ),
        ROLLBACK( new MasterCaller<Void>()
        {
            public Response<Void> callMaster( Master master, SlaveContext context,
                    ChannelBuffer input )
            {
                return master.rollbackTransaction( context, input.readInt() );
            }
        }, VOID_SERIALIZER ),
        PULL_UPDATES( new MasterCaller<Void>()
        {
            public Response<Void> callMaster( Master master, SlaveContext context,
                    ChannelBuffer input )
            {
                return master.pullUpdates( context );
            }
        }, VOID_SERIALIZER );

        @SuppressWarnings( "unchecked" )
        final MasterCaller caller;
        @SuppressWarnings( "unchecked" )
        final ObjectSerializer serializer;

        private <T> RequestType( MasterCaller<T> caller, ObjectSerializer<T> serializer )
        {
            this.caller = caller;
            this.serializer = serializer;
        }
    }

    @SuppressWarnings( "unchecked" )
    protected static ChannelBuffer handleRequest( Master realMaster,
            ChannelBuffer buffer ) throws IOException
    {
        RequestType type = RequestType.values()[buffer.readByte()];
        final SlaveContext context = readSlaveContext( buffer );
        Response<?> response = type.caller.callMaster( realMaster, context, buffer );
        
        ChannelBuffer targetBuffer = ChannelBuffers.dynamicBuffer();
        type.serializer.write( response.response(), targetBuffer );
        writeTransactionStreams( response.transactions(), targetBuffer );
        return targetBuffer;
    }

    private static <T> void writeTransactionStreams( TransactionStreams txStreams,
            ChannelBuffer buffer ) throws IOException
    {
        Collection<Pair<String, TransactionStream>> streams = txStreams.getStreams();
        buffer.writeByte( streams.size() );
        for ( Pair<String, TransactionStream> streamPair : streams )
        {
            writeString( buffer, streamPair.first() );
            writeTransactionStream( buffer, streamPair.other() );
        }
    }

    protected static TransactionStreams readTransactionStreams( ChannelBuffer buffer )
    {
        final TransactionStreams result = new TransactionStreams();
        for ( int count = buffer.readByte(); count > 0; count-- )
        {
            String resource = readString( buffer );
            TransactionStream stream = readTransactionStream( buffer );
            result.add( resource, stream );
        }
        return result;
    }

    protected static void writeTransactionStream( ChannelBuffer dest,
            TransactionStream transactionStream ) throws IOException
    {
        final Collection<ReadableByteChannel> channels = transactionStream.getChannels();
        dest.writeByte( channels.size() );
        for ( ReadableByteChannel channel : channels )
        {
            ByteData data = new ByteData( channel );
            dest.writeInt( data.size() );
            for ( byte[] bytes : data )
            {
                dest.writeBytes( bytes );
            }
            channel.close();
        }
    }

    private static TransactionStream readTransactionStream( ChannelBuffer buffer )
    {
        ReadableByteChannel[] channels = new ReadableByteChannel[buffer.readByte()];
        for ( int i = 0; i < channels.length; i++ )
        {
            byte[] data = new byte[buffer.readInt()];
            buffer.readBytes( data );
            channels[i] = new ByteArrayChannel( data );
        }
        // TODO Auto-generated method stub
        return new TransactionStream( Arrays.asList( channels ) );
    }

    private static class ByteArrayChannel implements ReadableByteChannel
    {
        private final byte[] data;
        private int pos;

        ByteArrayChannel( byte[] data )
        {
            this.data = data;
            this.pos = 0;
        }

        public int read( ByteBuffer dst ) throws IOException
        {
            if ( pos >= data.length ) return -1;
            int size = Math.min( data.length - pos, dst.limit() - dst.position() );
            dst.put( data, pos, size );
            pos += size;
            return size;
        }

        public void close() throws IOException
        {
            pos = -1;
        }

        public boolean isOpen()
        {
            return pos > 0;
        }
    }

    protected static class AcquireLockSerializer implements Serializer
    {
        private final long[] entities;
        private final int eventIdentifier;

        AcquireLockSerializer( int eventIdentifier, long... entities )
        {
            this.eventIdentifier = eventIdentifier;
            this.entities = entities;
        }

        public void write( ChannelBuffer buffer ) throws IOException
        {
            buffer.writeInt( eventIdentifier );
            buffer.writeInt( entities.length );
            for ( long entity : entities )
            {
                buffer.writeLong( entity );
            }
        }
    }

    static abstract class AquireLockCall implements MasterCaller<LockResult>
    {
        public Response<LockResult> callMaster( Master master, SlaveContext context,
                ChannelBuffer input )
        {
            int eventIdentifier = input.readInt();
            long[] ids = new long[input.readInt()];
            for ( int i = 0; i < ids.length; i++ )
            {
                ids[i] = input.readLong();
            }
            return lock( master, context, eventIdentifier, ids );
        }

        abstract Response<LockResult> lock( Master master, SlaveContext context,
                int eventIdentifier, long... ids );
    }

    protected static class ByteData implements Iterable<byte[]>
    {
        private final Collection<byte[]> data;
        private final int size;

        @SuppressWarnings( "hiding" )
        ByteData( ReadableByteChannel channel ) throws IOException
        {
            int size = 0, chunk = 0;
            List<byte[]> data = new LinkedList<byte[]>();
            ByteBuffer buffer = ByteBuffer.allocateDirect( 1 * MEGA );
            while ( ( chunk = channel.read( buffer ) ) >= 0 )
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

    protected static interface Serializer
    {
        void write( ChannelBuffer buffer ) throws IOException;
    }

    protected static interface Deserializer<T>
    {
        T read( ChannelBuffer buffer ) throws IOException;
    }

    protected interface ObjectSerializer<T>
    {
        void write( T responseObject, ChannelBuffer result ) throws IOException;
    }

    protected interface MasterCaller<T>
    {
        Response<T> callMaster( Master master, SlaveContext context, ChannelBuffer input );
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

    @SuppressWarnings( "boxing" )
    protected static void writeSlaveContext( ChannelBuffer buffer, SlaveContext context )
    {
        buffer.writeInt( context.machineId() );
        Map<String, Long> txs = context.lastAppliedTransactions();
        buffer.writeByte( txs.size() );
        for ( Map.Entry<String, Long> tx : txs.entrySet() )
        {
            writeString( buffer, tx.getKey() );
            buffer.writeLong( tx.getValue() );
        }
    }

    @SuppressWarnings( "boxing" )
    private static SlaveContext readSlaveContext( ChannelBuffer buffer )
    {
        int machineId = buffer.readInt();
        Map<String, Long> lastAppliedTransactions = new HashMap<String, Long>();
        int txsSize = buffer.readByte();
        for ( int i = 0; i < txsSize; i++ )
        {
            lastAppliedTransactions.put( readString( buffer ), buffer.readLong() );
        }
        return new SlaveContext( machineId, lastAppliedTransactions );
    }
}
