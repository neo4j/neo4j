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
package org.neo4j.kernel.ha;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Triplet;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.IdType;
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
                    ChannelBuffer input, ChannelBuffer target )
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
                    ChannelBuffer input, ChannelBuffer target )
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
                    ChannelBuffer input, ChannelBuffer target )
            {
                String resource = readString( input );
                final ReadableByteChannel reader = new BlockLogReader( input );
                return master.commitSingleResourceTransaction( context, resource,
                        TxExtractor.create( reader ) );
            }
        }, LONG_SERIALIZER ),
        PULL_UPDATES( new MasterCaller<Void>()
        {
            public Response<Void> callMaster( Master master, SlaveContext context,
                    ChannelBuffer input, ChannelBuffer target )
            {
                return master.pullUpdates( context );
            }
        }, VOID_SERIALIZER ),
        FINISH( new MasterCaller<Void>()
        {
            public Response<Void> callMaster( Master master, SlaveContext context,
                    ChannelBuffer input, ChannelBuffer target )
            {
                return master.finishTransaction( context );
            }
        }, VOID_SERIALIZER ),
        GET_MASTER_ID_FOR_TX( new MasterCaller<Integer>()
        {
            public Response<Integer> callMaster( Master master, SlaveContext context,
                    ChannelBuffer input, ChannelBuffer target )
            {
                int masterId = master.getMasterIdForCommittedTx( input.readLong() );
                return Response.wrapResponseObjectOnly( masterId );
            }
        }, INTEGER_SERIALIZER, false ),
        COPY_STORE( new MasterCaller<Void>()
        {
            public Response<Void> callMaster( Master master, SlaveContext context,
                    ChannelBuffer input, final ChannelBuffer target )
            {
                return master.copyStore( context, new StoreWriter()
                {
                    public void write( String path, ReadableByteChannel data, boolean hasData ) throws IOException
                    {
                        char[] chars = path.toCharArray();
                        target.writeShort( chars.length );
                        writeChars( target, chars );
                        target.writeByte( hasData ? 1 : 0 );
                        BlockLogBuffer buffer = new BlockLogBuffer( target );
                        if ( hasData )
                        {
                            buffer.write( data );
                            buffer.done();
                        }
                    }

                    public void done()
                    {
                        target.writeShort( 0 );
                    }
                } );
            }
        }, VOID_SERIALIZER );

        @SuppressWarnings( "rawtypes" )
        final MasterCaller caller;
        @SuppressWarnings( "rawtypes" )
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

    static void addLengthFieldPipes( ChannelPipeline pipeline )
    {
        pipeline.addLast( "frameDecoder",
                new LengthFieldBasedFrameDecoder( MAX_FRAME_LENGTH+4, 0, 4, 0, 4 ) );
        pipeline.addLast( "frameEncoder", new LengthFieldPrepender( 4 ) );
    }


    static <T> void writeTransactionStreams( TransactionStream txStream,
            ChannelBuffer buffer, ByteBuffer readBuffer ) throws IOException
    {
        String[] datasources = txStream.dataSourceNames();
        assert datasources.length <= 255 : "too many data sources";
        buffer.writeByte( datasources.length );
        Map<String, Integer> datasourceId = new HashMap<String, Integer>();
        for ( int i = 0; i < datasources.length; i++ )
        {
            String datasource = datasources[i];
            writeString( buffer, datasource );
            datasourceId.put( datasource, i + 1/*0 means "no more transactions"*/);
        }
        for ( Triplet<String, Long, TxExtractor> tx : IteratorUtil.asIterable( txStream ) )
        {
            buffer.writeByte( datasourceId.get( tx.first() ) );
            buffer.writeLong( tx.second() );
            BlockLogBuffer blockBuffer = new BlockLogBuffer( buffer );
            tx.third().extract( blockBuffer );
            blockBuffer.done();
        }
        buffer.writeByte( 0/*no more transactions*/);
    }

    protected static TransactionStream readTransactionStreams( final ChannelBuffer buffer )
    {
        final String[] datasources = readTransactionStreamHeader( buffer );
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

    private static void makeSureNextTransactionIsFullyFetched( ChannelBuffer buffer )
    {
        buffer.markReaderIndex();
        try
        {
            if ( buffer.readByte() > 0 /* datasource id */ )
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
    
    protected static String[] readTransactionStreamHeader( ChannelBuffer buffer )
    {
        final String[] datasources = new String[buffer.readUnsignedByte() + 1];
        datasources[0] = null; // identifier for "no more transactions"
        for ( int i = 1; i < datasources.length; i++ )
        {
            datasources[i] = readString( buffer );
        }
        return datasources;
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
                ChannelBuffer input, ChannelBuffer target )
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
        Response<T> callMaster( Master master, SlaveContext context, ChannelBuffer input,
                ChannelBuffer target );
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
        writeChars( buffer, chars );
    }

    private static void writeChars( ChannelBuffer buffer, char[] chars )
    {
        // TODO optimize?
        for ( char ch : chars )
        {
            buffer.writeChar( ch );
        }
    }

    protected static String readString( ChannelBuffer buffer )
    {
        return readString( buffer, buffer.readInt() );
    }

    protected static String readString( ChannelBuffer buffer, int length )
    {
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
    static SlaveContext readSlaveContext( ChannelBuffer buffer )
    {
        int machineId = buffer.readInt();
        int eventIdentifier = buffer.readInt();
        int txsSize = buffer.readByte();
        @SuppressWarnings( "unchecked" )
        Pair<String, Long>[] lastAppliedTransactions = new Pair[txsSize];
        for ( int i = 0; i < txsSize; i++ )
        {
            lastAppliedTransactions[i] = Pair.of( readString( buffer ), buffer.readLong() );
        }
        return new SlaveContext( machineId, eventIdentifier, lastAppliedTransactions );
    }
}
