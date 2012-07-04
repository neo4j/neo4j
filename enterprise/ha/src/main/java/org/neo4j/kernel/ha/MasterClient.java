/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.neo4j.com.Protocol.EMPTY_SERIALIZER;
import static org.neo4j.com.Protocol.INTEGER_SERIALIZER;
import static org.neo4j.com.Protocol.LONG_SERIALIZER;
import static org.neo4j.com.Protocol.VOID_DESERIALIZER;
import static org.neo4j.com.Protocol.VOID_SERIALIZER;
import static org.neo4j.com.Protocol.readBoolean;
import static org.neo4j.com.Protocol.readString;
import static org.neo4j.com.Protocol.writeString;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.jboss.netty.buffer.ChannelBuffer;
import org.neo4j.com.BlockLogBuffer;
import org.neo4j.com.BlockLogReader;
import org.neo4j.com.Client;
import org.neo4j.com.Deserializer;
import org.neo4j.com.ObjectSerializer;
import org.neo4j.com.Protocol;
import org.neo4j.com.RequestContext;
import org.neo4j.com.RequestType;
import org.neo4j.com.ResourceReleaser;
import org.neo4j.com.Response;
import org.neo4j.com.Serializer;
import org.neo4j.com.StoreWriter;
import org.neo4j.com.TargetCaller;
import org.neo4j.com.ToNetworkStoreWriter;
import org.neo4j.com.TransactionStream;
import org.neo4j.com.TxExtractor;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.nioneo.store.IdRange;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * The {@link Master} a slave should use to communicate with its master. It
 * serializes requests and sends them to the master, more specifically
 * {@link MasterServer} (which delegates to {@link MasterImpl}
 * on the master side.
 */
public class MasterClient extends Client<Master> implements Master
{
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
        public LockResult read( ChannelBuffer buffer, ByteBuffer temporaryBuffer ) throws IOException
        {
            LockStatus status = LockStatus.values()[buffer.readByte()];
            return status.hasMessage() ? new LockResult( readString( buffer ) ) : new LockResult(
                    status );
        }
    };
    private final int lockReadTimeout;

    public MasterClient( String hostNameOrIp, int port, StringLogger stringLogger, StoreId storeId,
            ConnectionLostHandler connectionLostHandler, int readTimeoutSeconds,
            int lockReadTimeout, int maxConcurrentChannels )
    {
        super( hostNameOrIp, port, stringLogger, storeId,
                MasterServer.FRAME_LENGTH, MasterServer.PROTOCOL_VERSION, readTimeoutSeconds,
                maxConcurrentChannels, Math.min( maxConcurrentChannels,
                        DEFAULT_MAX_NUMBER_OF_CONCURRENT_CHANNELS_PER_CLIENT ),connectionLostHandler );
        this.lockReadTimeout = lockReadTimeout;
    }

    @Override
    protected int getReadTimeout( RequestType<Master> type, int readTimeout )
    {
        return ((HaRequestType)type).isLock() ? lockReadTimeout : readTimeout;
    }

    @Override
    protected boolean shouldCheckStoreId( RequestType<Master> type )
    {
        return type != HaRequestType.COPY_STORE;
    }

    public Response<IdAllocation> allocateIds( final IdType idType )
    {
        return sendRequest( HaRequestType.ALLOCATE_IDS, RequestContext.EMPTY, new Serializer()
        {
            public void write( ChannelBuffer buffer, ByteBuffer readBuffer ) throws IOException
            {
                buffer.writeByte( idType.ordinal() );
            }
        }, new Deserializer<IdAllocation>()
        {
            public IdAllocation read( ChannelBuffer buffer, ByteBuffer temporaryBuffer ) throws IOException
            {
                return readIdAllocation( buffer );
            }
        } );
    }

    public Response<Integer> createRelationshipType( RequestContext context, final String name )
    {
        return sendRequest( HaRequestType.CREATE_RELATIONSHIP_TYPE, context, new Serializer()
        {
            public void write( ChannelBuffer buffer, ByteBuffer readBuffer ) throws IOException
            {
                writeString( buffer, name );
            }
        }, new Deserializer<Integer>()
        {
            @SuppressWarnings( "boxing" )
            public Integer read( ChannelBuffer buffer, ByteBuffer temporaryBuffer ) throws IOException
            {
                return buffer.readInt();
            }
        } );
    }

    @Override
    public Response<Void> initializeTx( RequestContext context )
    {
        return sendRequest( HaRequestType.INITIALIZE_TX, context, EMPTY_SERIALIZER, VOID_DESERIALIZER );
    }

    public Response<LockResult> acquireNodeWriteLock( RequestContext context, long... nodes )
    {
        return sendRequest( HaRequestType.ACQUIRE_NODE_WRITE_LOCK, context,
                new AcquireLockSerializer( nodes ), LOCK_RESULT_DESERIALIZER );
    }

    public Response<LockResult> acquireNodeReadLock( RequestContext context, long... nodes )
    {
        return sendRequest( HaRequestType.ACQUIRE_NODE_READ_LOCK, context,
                new AcquireLockSerializer( nodes ), LOCK_RESULT_DESERIALIZER );
    }

    public Response<LockResult> acquireRelationshipWriteLock( RequestContext context,
            long... relationships )
    {
        return sendRequest( HaRequestType.ACQUIRE_RELATIONSHIP_WRITE_LOCK, context,
                new AcquireLockSerializer( relationships ), LOCK_RESULT_DESERIALIZER );
    }

    public Response<LockResult> acquireRelationshipReadLock( RequestContext context,
            long... relationships )
    {
        return sendRequest( HaRequestType.ACQUIRE_RELATIONSHIP_READ_LOCK, context,
                new AcquireLockSerializer( relationships ), LOCK_RESULT_DESERIALIZER );
    }

    public Response<LockResult> acquireGraphWriteLock( RequestContext context )
    {
        return sendRequest( HaRequestType.ACQUIRE_GRAPH_WRITE_LOCK, context,
                EMPTY_SERIALIZER, LOCK_RESULT_DESERIALIZER );
    }

    public Response<LockResult> acquireGraphReadLock( RequestContext context )
    {
        return sendRequest( HaRequestType.ACQUIRE_GRAPH_READ_LOCK, context,
                EMPTY_SERIALIZER, LOCK_RESULT_DESERIALIZER );
    }

    @Override
    public Response<LockResult> acquireIndexReadLock( RequestContext context, String index, String key )
    {
        return sendRequest( HaRequestType.ACQUIRE_INDEX_READ_LOCK, context,
                new AcquireIndexLockSerializer( index, key ), LOCK_RESULT_DESERIALIZER );
    }

    @Override
    public Response<LockResult> acquireIndexWriteLock( RequestContext context, String index, String key )
    {
        return sendRequest( HaRequestType.ACQUIRE_INDEX_WRITE_LOCK, context,
                new AcquireIndexLockSerializer( index, key ), LOCK_RESULT_DESERIALIZER );
    }

    public Response<Long> commitSingleResourceTransaction( RequestContext context,
            final String resource, final TxExtractor txGetter )
    {
        return sendRequest( HaRequestType.COMMIT, context, new Serializer()
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
            public Long read( ChannelBuffer buffer, ByteBuffer temporaryBuffer ) throws IOException
            {
                return buffer.readLong();
            }
        });
    }

    public Response<Void> finishTransaction( RequestContext context, final boolean success )
    {
        try
        {
            return sendRequest( HaRequestType.FINISH, context, new Serializer()
            {
                public void write( ChannelBuffer buffer, ByteBuffer readBuffer ) throws IOException
                {
                    buffer.writeByte( success ? 1 : 0 );
                }
            }, VOID_DESERIALIZER );
        }
        catch ( UnableToResumeTransactionException e )
        {
            if ( !success )
            {
                /* Here we are in a state where the client failed while the request
                 * was processing on the server and the tx.finish() in the usual
                 * try-finally transaction block gets called, only to find that
                 * the transaction is already active... which is totally expected.
                 * The fact that the transaction is already active here shouldn't
                 * hide the original exception on the client, the exception which
                 * cause the client to fail while the request was processing on the master.
                 * This is effectively the use case of awaiting a lock that isn't granted
                 * within the lock read timeout period.
                 */
                return new Response<Void>( null, getStoreId(), TransactionStream.EMPTY, ResourceReleaser.NO_OP );
            }
            throw e;
        }
    }

    public void rollbackOngoingTransactions( RequestContext context )
    {
        throw new UnsupportedOperationException( "Should never be called from the client side" );
    }

    public Response<Void> pullUpdates( RequestContext context )
    {
        return sendRequest( HaRequestType.PULL_UPDATES, context, EMPTY_SERIALIZER, VOID_DESERIALIZER );
    }

    public Response<Pair<Integer,Long>> getMasterIdForCommittedTx( final long txId, StoreId storeId )
    {
        return sendRequest( HaRequestType.GET_MASTER_ID_FOR_TX, RequestContext.EMPTY, new Serializer()
        {
            public void write( ChannelBuffer buffer, ByteBuffer readBuffer ) throws IOException
            {
                buffer.writeLong( txId );
            }
        }, new Deserializer<Pair<Integer,Long>>()
        {
            @Override
            public Pair<Integer, Long> read( ChannelBuffer buffer, ByteBuffer temporaryBuffer ) throws IOException
            {
                return Pair.of( buffer.readInt(), buffer.readLong() );
            }
        }, storeId );
    }

    public Response<Void> copyStore( RequestContext context, final StoreWriter writer )
    {
        context = stripFromTransactions( context );
        return sendRequest( HaRequestType.COPY_STORE, context, EMPTY_SERIALIZER, new Protocol.FileStreamsDeserializer( writer ) );
    }

    private RequestContext stripFromTransactions( RequestContext context )
    {
        return new RequestContext( context.getSessionId(), context.machineId(),
                context.getEventIdentifier(), new RequestContext.Tx[0], context.getMasterId(), context.getChecksum() );
    }

    @Override
    public Response<Void> copyTransactions( RequestContext context,
            final String ds, final long startTxId, final long endTxId )
    {
        context = stripFromTransactions( context );
        return sendRequest( HaRequestType.COPY_TRANSACTIONS, context, new Serializer()
        {
            public void write( ChannelBuffer buffer, ByteBuffer readBuffer )
                    throws IOException
            {
                writeString( buffer, ds );
                buffer.writeLong( startTxId );
                buffer.writeLong( endTxId );
            }
        }, VOID_DESERIALIZER );
    }

    public static enum HaRequestType implements RequestType<Master>
    {
        //====
        ALLOCATE_IDS( new TargetCaller<Master, IdAllocation>()
        {
            public Response<IdAllocation> call( Master master, RequestContext context,
                    ChannelBuffer input, ChannelBuffer target )
            {
                IdType idType = IdType.values()[input.readByte()];
                return master.allocateIds( idType );
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

        //====
        CREATE_RELATIONSHIP_TYPE( new TargetCaller<Master, Integer>()
        {
            public Response<Integer> call( Master master, RequestContext context,
                    ChannelBuffer input, ChannelBuffer target )
            {
                return master.createRelationshipType( context, readString( input ) );
            }
        }, INTEGER_SERIALIZER, true ),

        //====
        ACQUIRE_NODE_WRITE_LOCK( new AquireLockCall()
        {
            @Override
            Response<LockResult> lock( Master master, RequestContext context, long... ids )
            {
                return master.acquireNodeWriteLock( context, ids );
            }
        }, LOCK_SERIALIZER, true )
        {
            @Override
            public boolean isLock()
            {
                return true;
            }
        },

        //====
        ACQUIRE_NODE_READ_LOCK( new AquireLockCall()
        {
            @Override
            Response<LockResult> lock( Master master, RequestContext context, long... ids )
            {
                return master.acquireNodeReadLock( context, ids );
            }
        }, LOCK_SERIALIZER, true )
        {
            @Override
            public boolean isLock()
            {
                return true;
            }
        },

        //====
        ACQUIRE_RELATIONSHIP_WRITE_LOCK( new AquireLockCall()
        {
            @Override
            Response<LockResult> lock( Master master, RequestContext context, long... ids )
            {
                return master.acquireRelationshipWriteLock( context, ids );
            }
        }, LOCK_SERIALIZER, true )
        {
            @Override
            public boolean isLock()
            {
                return true;
            }
        },

        //====
        ACQUIRE_RELATIONSHIP_READ_LOCK( new AquireLockCall()
        {
            @Override
            Response<LockResult> lock( Master master, RequestContext context, long... ids )
            {
                return master.acquireRelationshipReadLock( context, ids );
            }
        }, LOCK_SERIALIZER, true )
        {
            @Override
            public boolean isLock()
            {
                return true;
            }
        },

        //====
        COMMIT( new TargetCaller<Master, Long>()
        {
            public Response<Long> call( Master master, RequestContext context,
                    ChannelBuffer input, ChannelBuffer target )
            {
                String resource = readString( input );
                final ReadableByteChannel reader = new BlockLogReader( input );
                return master.commitSingleResourceTransaction( context, resource,
                        TxExtractor.create( reader ) );
            }
        }, LONG_SERIALIZER, true ),

        //====
        PULL_UPDATES( new TargetCaller<Master, Void>()
        {
            public Response<Void> call( Master master, RequestContext context,
                    ChannelBuffer input, ChannelBuffer target )
            {
                return master.pullUpdates( context );
            }
        }, VOID_SERIALIZER, true ),

        //====
        FINISH( new TargetCaller<Master, Void>()
        {
            public Response<Void> call( Master master, RequestContext context,
                    ChannelBuffer input, ChannelBuffer target )
            {
                return master.finishTransaction( context, readBoolean( input ) );
            }
        }, VOID_SERIALIZER, true ),

        //====
        GET_MASTER_ID_FOR_TX( new TargetCaller<Master, Pair<Integer,Long>>()
        {
            public Response<Pair<Integer,Long>> call( Master master, RequestContext context,
                    ChannelBuffer input, ChannelBuffer target )
            {
                return master.getMasterIdForCommittedTx( input.readLong(), null );
            }
        }, new ObjectSerializer<Pair<Integer,Long>>()
        {
            @Override
            public void write( Pair<Integer, Long> responseObject, ChannelBuffer result ) throws IOException
            {
                result.writeInt( responseObject.first() );
                result.writeLong( responseObject.other() );
            }
        }, false ),

        //====
        COPY_STORE( new TargetCaller<Master, Void>()
        {
            public Response<Void> call( Master master, RequestContext context,
                    ChannelBuffer input, final ChannelBuffer target )
            {
                return master.copyStore( context, new ToNetworkStoreWriter( target ) );
            }

        }, VOID_SERIALIZER, true ),

        // ====
        COPY_TRANSACTIONS( new TargetCaller<Master, Void>()
        {
            public Response<Void> call( Master master,
                    RequestContext context, ChannelBuffer input,
                    final ChannelBuffer target )
            {
                return master.copyTransactions( context, readString( input ),
                        input.readLong(), input.readLong() );
            }

        }, VOID_SERIALIZER, true ),

        //====
        INITIALIZE_TX( new TargetCaller<Master, Void>()
        {
            @Override
            public Response<Void> call( Master master, RequestContext context, ChannelBuffer input,
                    ChannelBuffer target )
            {
                return master.initializeTx( context );
            }
        }, VOID_SERIALIZER, true ),

        //====
        ACQUIRE_GRAPH_WRITE_LOCK( new TargetCaller<Master, LockResult>()
        {
            @Override
            public Response<LockResult> call( Master master, RequestContext context, ChannelBuffer input,
                    ChannelBuffer target )
            {
                return master.acquireGraphWriteLock( context );
            }
        }, LOCK_SERIALIZER, true )
        {
            @Override
            public boolean isLock()
            {
                return true;
            }
        },

        //====
        ACQUIRE_GRAPH_READ_LOCK( new TargetCaller<Master, LockResult>()
        {
            @Override
            public Response<LockResult> call( Master master, RequestContext context, ChannelBuffer input,
                    ChannelBuffer target )
            {
                return master.acquireGraphReadLock( context );
            }
        }, LOCK_SERIALIZER, true )
        {
            @Override
            public boolean isLock()
            {
                return true;
            }
        },

        //====
        ACQUIRE_INDEX_READ_LOCK( new TargetCaller<Master, LockResult>()
        {
            @Override
            public Response<LockResult> call( Master master, RequestContext context,
                    ChannelBuffer input, ChannelBuffer target )
            {
                return master.acquireIndexReadLock( context, readString( input ), readString( input ) );
            }

        }, LOCK_SERIALIZER, true )
        {
            @Override
            public boolean isLock()
            {
                return true;
            }
        },


        //====
        ACQUIRE_INDEX_WRITE_LOCK( new TargetCaller<Master, LockResult>()
        {
            @Override
            public Response<LockResult> call( Master master, RequestContext context,
                    ChannelBuffer input, ChannelBuffer target )
            {
                return master.acquireIndexWriteLock( context, readString( input ), readString( input ) );
            }

        }, LOCK_SERIALIZER, true )
        {
            @Override
            public boolean isLock()
            {
                return true;
            }
        };

        @SuppressWarnings( "rawtypes" )
        final TargetCaller caller;
        @SuppressWarnings( "rawtypes" )
        final ObjectSerializer serializer;
        private final boolean includesSlaveContext;

        private <T> HaRequestType( TargetCaller caller, ObjectSerializer<T> serializer,
                boolean includesSlaveContext )
        {
            this.caller = caller;
            this.serializer = serializer;
            this.includesSlaveContext = includesSlaveContext;
        }

        @Override
        public ObjectSerializer getObjectSerializer()
        {
            return serializer;
        }

        @Override
        public TargetCaller getTargetCaller()
        {
            return caller;
        }

        @Override
        public byte id()
        {
            return (byte) ordinal();
        }

        public boolean includesSlaveContext()
        {
            return this.includesSlaveContext;
        }

        public boolean isLock()
        {
            return false;
        }
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

    protected static class AcquireIndexLockSerializer implements Serializer
    {
        private final String index;
        private final String key;

        AcquireIndexLockSerializer( String index, String key )
        {
            this.index = index;
            this.key = key;
        }

        @Override
        public void write( ChannelBuffer buffer, ByteBuffer readBuffer ) throws IOException
        {
            writeString( buffer, index );
            writeString( buffer, key );
        }
    }

    static abstract class AquireLockCall implements TargetCaller<Master, LockResult>
    {
        public Response<LockResult> call( Master master, RequestContext context,
                ChannelBuffer input, ChannelBuffer target )
        {
            long[] ids = new long[input.readInt()];
            for ( int i = 0; i < ids.length; i++ )
            {
                ids[i] = input.readLong();
            }
            return lock( master, context, ids );
        }

        abstract Response<LockResult> lock( Master master, RequestContext context, long... ids );
    }
}
