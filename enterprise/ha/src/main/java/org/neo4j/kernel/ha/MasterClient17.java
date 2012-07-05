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
import static org.neo4j.com.Protocol.VOID_DESERIALIZER;
import static org.neo4j.com.Protocol.writeString;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.jboss.netty.buffer.ChannelBuffer;
import org.neo4j.com.BlockLogBuffer;
import org.neo4j.com.Client;
import org.neo4j.com.ConnectionLostHandler;
import org.neo4j.com.Deserializer;
import org.neo4j.com.Protocol;
import org.neo4j.com.RequestContext;
import org.neo4j.com.RequestType;
import org.neo4j.com.ResourceReleaser;
import org.neo4j.com.Response;
import org.neo4j.com.Serializer;
import org.neo4j.com.StoreWriter;
import org.neo4j.com.TargetCaller;
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
public class MasterClient17 extends Client<Master> implements MasterClient
{
    /* Version 1 first version
     * Version 2 since 2012-01-24
     * Version 3 since 2012-02-16 */
    public static final byte PROTOCOL_VERSION = 3;

    private final int lockReadTimeout;

    public MasterClient17( String hostNameOrIp, int port, StringLogger stringLogger, StoreId storeId, ConnectionLostHandler connectionLostHandler,
            int readTimeoutSeconds, int lockReadTimeout, int maxConcurrentChannels )
    {
        super( hostNameOrIp, port, stringLogger, storeId, MasterServer.FRAME_LENGTH, PROTOCOL_VERSION,
                readTimeoutSeconds, maxConcurrentChannels, Math.min( maxConcurrentChannels,
                        DEFAULT_MAX_NUMBER_OF_CONCURRENT_CHANNELS_PER_CLIENT ), connectionLostHandler );
        this.lockReadTimeout = lockReadTimeout;
    }

    @Override
    protected int getReadTimeout( RequestType<Master> type, int readTimeout )
    {
        return ( (HaRequestType17) type ).isLock() ? lockReadTimeout : readTimeout;
    }

    @Override
    protected boolean shouldCheckStoreId( RequestType<Master> type )
    {
        return type != HaRequestType17.COPY_STORE;
    }

    @Override
    public Response<IdAllocation> allocateIds( final IdType idType )
    {
        return sendRequest( HaRequestType17.ALLOCATE_IDS, RequestContext.EMPTY, new Serializer()
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

    @Override
    public Response<Integer> createRelationshipType( RequestContext context, final String name )
    {
        return sendRequest( HaRequestType17.CREATE_RELATIONSHIP_TYPE, context, new Serializer()
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
        return sendRequest( HaRequestType17.INITIALIZE_TX, context, EMPTY_SERIALIZER, VOID_DESERIALIZER );
    }

    @Override
    public Response<LockResult> acquireNodeWriteLock( RequestContext context, long... nodes )
    {
        return sendRequest( HaRequestType17.ACQUIRE_NODE_WRITE_LOCK, context,
                new AcquireLockSerializer( nodes ), LOCK_RESULT_DESERIALIZER );
    }

    @Override
    public Response<LockResult> acquireNodeReadLock( RequestContext context, long... nodes )
    {
        return sendRequest( HaRequestType17.ACQUIRE_NODE_READ_LOCK, context,
                new AcquireLockSerializer( nodes ), LOCK_RESULT_DESERIALIZER );
    }

    @Override
    public Response<LockResult> acquireRelationshipWriteLock( RequestContext context,
            long... relationships )
    {
        return sendRequest( HaRequestType17.ACQUIRE_RELATIONSHIP_WRITE_LOCK, context,
                new AcquireLockSerializer( relationships ), LOCK_RESULT_DESERIALIZER );
    }

    @Override
    public Response<LockResult> acquireRelationshipReadLock( RequestContext context,
            long... relationships )
    {
        return sendRequest( HaRequestType17.ACQUIRE_RELATIONSHIP_READ_LOCK, context,
                new AcquireLockSerializer( relationships ), LOCK_RESULT_DESERIALIZER );
    }

    @Override
    public Response<LockResult> acquireGraphWriteLock( RequestContext context )
    {
        return sendRequest( HaRequestType17.ACQUIRE_GRAPH_WRITE_LOCK, context,
                EMPTY_SERIALIZER, LOCK_RESULT_DESERIALIZER );
    }

    @Override
    public Response<LockResult> acquireGraphReadLock( RequestContext context )
    {
        return sendRequest( HaRequestType17.ACQUIRE_GRAPH_READ_LOCK, context,
                EMPTY_SERIALIZER, LOCK_RESULT_DESERIALIZER );
    }

    @Override
    public Response<LockResult> acquireIndexReadLock( RequestContext context, String index, String key )
    {
        return sendRequest( HaRequestType17.ACQUIRE_INDEX_READ_LOCK, context,
                new AcquireIndexLockSerializer( index, key ), LOCK_RESULT_DESERIALIZER );
    }

    @Override
    public Response<LockResult> acquireIndexWriteLock( RequestContext context, String index, String key )
    {
        return sendRequest( HaRequestType17.ACQUIRE_INDEX_WRITE_LOCK, context,
                new AcquireIndexLockSerializer( index, key ), LOCK_RESULT_DESERIALIZER );
    }

    @Override
    public Response<Long> commitSingleResourceTransaction( RequestContext context,
            final String resource, final TxExtractor txGetter )
    {
        return sendRequest( HaRequestType17.COMMIT, context, new Serializer()
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

    @Override
    public Response<Void> finishTransaction( RequestContext context, final boolean success )
    {
        try
        {
            return sendRequest( HaRequestType17.FINISH, context, new Serializer()
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

    @Override
    public void rollbackOngoingTransactions( RequestContext context )
    {
        throw new UnsupportedOperationException( "Should never be called from the client side" );
    }

    @Override
    public Response<Void> pullUpdates( RequestContext context )
    {
        return sendRequest( HaRequestType17.PULL_UPDATES, context, EMPTY_SERIALIZER, VOID_DESERIALIZER );
    }

    @Override
    public Response<Pair<Integer,Long>> getMasterIdForCommittedTx( final long txId, StoreId storeId )
    {
        return sendRequest( HaRequestType17.GET_MASTER_ID_FOR_TX, RequestContext.EMPTY, new Serializer()
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

    @Override
    public Response<Void> copyStore( RequestContext context, final StoreWriter writer )
    {
        context = stripFromTransactions( context );
        return sendRequest( HaRequestType17.COPY_STORE, context, EMPTY_SERIALIZER,
                new Protocol.FileStreamsDeserializer( writer ) );
    }

    private RequestContext stripFromTransactions( RequestContext context )
    {
        return new RequestContext( context.getSessionId(), context.machineId(), context.getEventIdentifier(),
                new RequestContext.Tx[0], context.getMasterId(), context.getChecksum() );
    }

    @Override
    public Response<Void> copyTransactions( RequestContext context,
            final String ds, final long startTxId, final long endTxId )
    {
        context = stripFromTransactions( context );
        return sendRequest( HaRequestType17.COPY_TRANSACTIONS, context, new Serializer()
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

    @Override
    public Response<Void> pushTransaction( RequestContext context, final String resourceName, final long tx )
    {
        throw new UnsupportedOperationException();
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
        @Override
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
