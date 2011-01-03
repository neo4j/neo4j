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
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.ha.MasterServer.PartialRequest;
import org.neo4j.kernel.impl.nioneo.store.IdRange;

public abstract class CommunicationProtocol
{
    public static final int PORT = 8901;
    private static final int MEGA = 1024 * 1024;
    static final int MAX_FRAME_LENGTH = 16*MEGA;
    
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
    protected static final Deserializer<Integer> INTEGER_DESERIALIZER = new Deserializer<Integer>()
    {
        public Integer read( ChannelBuffer buffer ) throws IOException
        {
            return buffer.readInt();
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
        public void write( ChannelBuffer buffer, ByteBuffer readBuffer ) throws IOException
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
                return Response.wrapResponseObjectOnly( master.allocateIds( idType ) );
            }
        }, new ObjectSerializer<IdAllocation>()
        {
            public void write( IdAllocation idAllocation, ChannelBuffer result ) throws IOException
            {
                IdRange idRange = idAllocation.getIdRange();
                result.writeInt( idRange.getDefragIds().length );
                for ( long id : idRange.getDefragIds() )
                {
                    result.writeLong( id );
                }
                result.writeLong( idRange.getRangeStart() );
                result.writeInt( idRange.getRangeLength() );
                result.writeLong( idAllocation.getHighestIdInUse() );
                result.writeLong( idAllocation.getDefragCount() );
            }
        }, false ),
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
            Response<LockResult> lock( Master master, SlaveContext context, long... ids )
            {
                return master.acquireNodeWriteLock( context, ids );
            }
        }, LOCK_SERIALIZER ),
        ACQUIRE_NODE_READ_LOCK( new AquireLockCall()
        {
            @Override
            Response<LockResult> lock( Master master, SlaveContext context, long... ids )
            {
                return master.acquireNodeReadLock( context, ids );
            }
        }, LOCK_SERIALIZER ),
        ACQUIRE_RELATIONSHIP_WRITE_LOCK( new AquireLockCall()
        {
            @Override
            Response<LockResult> lock( Master master, SlaveContext context, long... ids )
            {
                return master.acquireRelationshipWriteLock( context, ids );
            }
        }, LOCK_SERIALIZER ),
        ACQUIRE_RELATIONSHIP_READ_LOCK( new AquireLockCall()
        {
            @Override
            Response<LockResult> lock( Master master, SlaveContext context, long... ids )
            {
                return master.acquireRelationshipReadLock( context, ids );
            }
        }, LOCK_SERIALIZER ),
        COMMIT( new MasterCaller<Long>()
        {
            public Response<Long> callMaster( Master master, SlaveContext context,
                    ChannelBuffer input )
            {
                String resource = readString( input );
                TransactionStream transactionStream = readTransactionStream( input );
                return master.commitSingleResourceTransaction( context, resource, transactionStream );
            }
        }, LONG_SERIALIZER ),
        PULL_UPDATES( new MasterCaller<Void>()
        {
            public Response<Void> callMaster( Master master, SlaveContext context,
                    ChannelBuffer input )
            {
                return master.pullUpdates( context );
            }
        }, VOID_SERIALIZER ),
        FINISH( new MasterCaller<Void>()
        {
            public Response<Void> callMaster( Master master, SlaveContext context,
                    ChannelBuffer input )
            {
                return master.finishTransaction( context );
            }
        }, VOID_SERIALIZER ),
        GET_MASTER_ID_FOR_TX( new MasterCaller<Integer>()
        {
            public Response<Integer> callMaster( Master master, SlaveContext context,
                    ChannelBuffer input )
            {
                int masterId = master.getMasterIdForCommittedTx( input.readLong() );
                return Response.wrapResponseObjectOnly( masterId );
            }
        }, INTEGER_SERIALIZER, false );

        @SuppressWarnings( "unchecked" )
        final MasterCaller caller;
        @SuppressWarnings( "unchecked" )
        final ObjectSerializer serializer;
        private final boolean includesSlaveContext;

        private <T> RequestType( MasterCaller<T> caller, ObjectSerializer<T> serializer,
                boolean includesSlaveContext )
        {
            this.caller = caller;
            this.serializer = serializer;
            this.includesSlaveContext = includesSlaveContext;
        }
        
        private <T> RequestType( MasterCaller<T> caller, ObjectSerializer<T> serializer )
        {
            this( caller, serializer, true );
        }
        
        public boolean includesSlaveContext()
        {
            return this.includesSlaveContext;
        }
    }

    @SuppressWarnings( "unchecked" )
    protected static ChannelBuffer handleRequest( Master realMaster,
            ChannelBuffer buffer, Channel channel, MasterServer server ) throws IOException
    {
        // TODO Not very pretty solution (to pass in MasterServer here)
        // but what the heck.
        
        // TODO Too long method, refactor please
        byte continuation = buffer.readByte();
        Map<Channel, PartialRequest> partialRequests = server.getPartialRequests();
        if ( continuation == ChunkingChannelBuffer.CONTINUATION_MORE )
        {
            PartialRequest partialRequest = partialRequests.get( channel );
            if ( partialRequest == null )
            {
                // This is the first chunk
                RequestType type = RequestType.values()[buffer.readByte()];
                SlaveContext context = null;
                if ( type.includesSlaveContext() )
                {
                    context = readSlaveContext( buffer );
                }
                Pair<ChannelBuffer, ByteBuffer> targetBuffers = server.mapSlave( channel, context );
                partialRequest = new PartialRequest( type, context, targetBuffers );
                partialRequests.put( channel, partialRequest );
            }
            partialRequest.add( buffer );
            
            // TODO
            return null;
        }
        else
        {
            PartialRequest partialRequest = partialRequests.remove( channel );
            RequestType type = null;
            SlaveContext context = null;
            Pair<ChannelBuffer, ByteBuffer> targetBuffers;
            ChannelBuffer bufferToReadFrom = null;
            if ( partialRequest == null )
            {
                type = RequestType.values()[buffer.readByte()];
                if ( type.includesSlaveContext() )
                {
                    context = readSlaveContext( buffer );
                }
                targetBuffers = server.mapSlave( channel, context );
                bufferToReadFrom = buffer;
            }
            else
            {
                type = partialRequest.type;
                context = partialRequest.slaveContext;
                targetBuffers = partialRequest.buffers;
                partialRequest.add( buffer );
                bufferToReadFrom = targetBuffers.first();
            }
            
            Response<?> response = type.caller.callMaster( realMaster, context, bufferToReadFrom );
            targetBuffers.first().clear();
            ChannelBuffer theBuffer = new ChunkingChannelBuffer( targetBuffers.first(), channel, MAX_FRAME_LENGTH );
            type.serializer.write( response.response(), theBuffer );
            if ( type.includesSlaveContext() )
            {
                writeTransactionStreams( response.transactions(), theBuffer, targetBuffers.other() );
            }
            if ( type == RequestType.FINISH || type == RequestType.PULL_UPDATES )
            {
                server.unmapSlave( channel, context );
            }
            return theBuffer;
        }
    }
    
    private static <T> void writeTransactionStreams( TransactionStreams txStreams,
            ChannelBuffer buffer, ByteBuffer readBuffer ) throws IOException
    {
        Collection<Pair<String, TransactionStream>> streams = txStreams.getStreams();
        buffer.writeByte( streams.size() );
        for ( Pair<String, TransactionStream> streamPair : streams )
        {
            writeString( buffer, streamPair.first() );
            writeTransactionStream( buffer, readBuffer, streamPair.other() );
        }
        txStreams.close();
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

    protected static void writeTransactionStream( ChannelBuffer dest, ByteBuffer readBuffer,
            TransactionStream transactionStream ) throws IOException
    {
        Collection<Pair<Long, ReadableByteChannel>> channels = transactionStream.getChannels();
        dest.writeInt( channels.size() );
        for ( Pair<Long, ReadableByteChannel> channel : channels )
        {
            dest.writeLong( channel.first() );
            ByteData data = new ByteData( channel.other(), readBuffer );
            dest.writeInt( data.size() );
            for ( byte[] bytes : data )
            {
                dest.writeBytes( bytes );
            }
            channel.other().close();
        }
    }

    private static TransactionStream readTransactionStream( ChannelBuffer buffer )
    {
        Collection<Pair<Long, ReadableByteChannel>> channels =
                new ArrayList<Pair<Long,ReadableByteChannel>>();
        int size = buffer.readInt();
        for ( int i = 0; i < size; i++ )
        {
            long txId = buffer.readLong();
            byte[] data = new byte[buffer.readInt()];
            buffer.readBytes( data );
            ReadableByteChannel channel = new ByteArrayChannel( data );
            channels.add( new Pair<Long, ReadableByteChannel>( txId, channel ) );
        }
        return new TransactionStream( channels );
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

        AcquireLockSerializer( long... entities )
        {
            this.entities = entities;
        }

        public void write( ChannelBuffer buffer, ByteBuffer readBuffer ) throws IOException
        {
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
            long[] ids = new long[input.readInt()];
            for ( int i = 0; i < ids.length; i++ )
            {
                ids[i] = input.readLong();
            }
            return lock( master, context, ids );
        }

        abstract Response<LockResult> lock( Master master, SlaveContext context, long... ids );
    }

    protected static class ByteData implements Iterable<byte[]>
    {
        private final Collection<byte[]> data;
        private final int size;

        @SuppressWarnings( "hiding" )
        ByteData( ReadableByteChannel channel, ByteBuffer readBuffer ) throws IOException
        {
            int size = 0, chunk = 0;
            List<byte[]> data = new LinkedList<byte[]>();
            while ( ( chunk = channel.read( readBuffer ) ) >= 0 )
            {
                size += chunk;
                byte[] bytes = new byte[chunk];
                readBuffer.flip();
                readBuffer.get( bytes );
                readBuffer.clear();
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
        void write( ChannelBuffer buffer, ByteBuffer readBuffer ) throws IOException;
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
        int numberOfDefragIds = buffer.readInt();
        long[] defragIds = new long[numberOfDefragIds];
        for ( int i = 0; i < numberOfDefragIds; i++ )
        {
            defragIds[i] = buffer.readLong();
        }
        long rangeStart = buffer.readLong();
        int rangeLength = buffer.readInt();
        long highId = buffer.readLong();
        long defragCount = buffer.readLong();
        return new IdAllocation( new IdRange( defragIds, rangeStart, rangeLength ),
                highId, defragCount );
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
        buffer.writeInt( context.getEventIdentifier() );
        Pair<String, Long>[] txs = context.lastAppliedTransactions();
        buffer.writeByte( txs.length );
        for ( Pair<String, Long> tx : txs )
        {
            writeString( buffer, tx.first() );
            buffer.writeLong( tx.other() );
        }
    }

    @SuppressWarnings( "boxing" )
    private static SlaveContext readSlaveContext( ChannelBuffer buffer )
    {
        int machineId = buffer.readInt();
        int eventIdentifier = buffer.readInt();
        int txsSize = buffer.readByte();
        Pair<String, Long>[] lastAppliedTransactions = new Pair[txsSize];
        for ( int i = 0; i < txsSize; i++ )
        {
            lastAppliedTransactions[i] = new Pair<String, Long>(
                    readString( buffer ), buffer.readLong() );
        }
        return new SlaveContext( machineId, eventIdentifier, lastAppliedTransactions );
    }
}
