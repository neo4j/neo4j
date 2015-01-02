/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.ha.com.slave;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.jboss.netty.buffer.ChannelBuffer;
import org.neo4j.com.BlockLogBuffer;
import org.neo4j.com.Client;
import org.neo4j.com.Deserializer;
import org.neo4j.com.Protocol;
import org.neo4j.com.RequestContext;
import org.neo4j.com.RequestType;
import org.neo4j.com.ResourceReleaser;
import org.neo4j.com.Response;
import org.neo4j.com.Serializer;
import org.neo4j.com.TargetCaller;
import org.neo4j.com.TransactionStream;
import org.neo4j.com.TxExtractor;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.ha.com.HaRequestType18;
import org.neo4j.kernel.ha.com.master.HandshakeResult;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.master.MasterServer;
import org.neo4j.kernel.ha.id.IdAllocation;
import org.neo4j.kernel.ha.lock.LockResult;
import org.neo4j.kernel.impl.nioneo.store.IdRange;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.transaction.TransactionAlreadyActiveException;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.com.Protocol.EMPTY_SERIALIZER;
import static org.neo4j.com.Protocol.VOID_DESERIALIZER;
import static org.neo4j.com.Protocol.writeString;

/**
 * The {@link org.neo4j.kernel.ha.com.master.Master} a slave should use to communicate with its master. It
 * serializes requests and sends them to the master, more specifically
 * {@link org.neo4j.kernel.ha.com.master.MasterServer} (which delegates to {@link org.neo4j.kernel.ha.com.master.MasterImpl}
 * on the master side.
 */
public class MasterClient18 extends Client<Master> implements MasterClient
{
    /* Version 1 first version
     * Version 2 since 2012-01-24
     * Version 3 since 2012-02-16
     * Version 4 since 2012-07-05 */
    public static final byte PROTOCOL_VERSION = 4;

    private final long lockReadTimeout;
    private final ByteCounterMonitor monitor;

    public MasterClient18( String hostNameOrIp, int port, Logging logging, Monitors monitors, StoreId storeId,
                           long readTimeoutSeconds, long lockReadTimeout, int maxConcurrentChannels, int chunkSize )
    {
        super( hostNameOrIp, port, logging, monitors, storeId, MasterServer.FRAME_LENGTH, PROTOCOL_VERSION,
                readTimeoutSeconds, maxConcurrentChannels, chunkSize );
        this.lockReadTimeout = lockReadTimeout;
        this.monitor = monitors.newMonitor( ByteCounterMonitor.class, getClass() );
    }

    @Override
    protected long getReadTimeout( RequestType<Master> type, long readTimeout )
    {
        HaRequestType18 specificType = (HaRequestType18) type;
        if ( specificType.isLock() )
        {
            return lockReadTimeout;
        }
        if ( specificType == HaRequestType18.COPY_STORE )
        {
            return readTimeout * 2;
        }
        return readTimeout;
    }

    @Override
    protected boolean shouldCheckStoreId( RequestType<Master> type )
    {
        return type != HaRequestType18.COPY_STORE;
    }

    @Override
    public Response<IdAllocation> allocateIds( RequestContext context, final IdType idType )
    {
        return sendRequest( HaRequestType18.ALLOCATE_IDS, context, new Serializer()
                {
                    public void write( ChannelBuffer buffer ) throws IOException
                    {
                        buffer.writeByte( idType.ordinal() );
                    }
                }, new Deserializer<IdAllocation>()
                {
                    @Override
                    public IdAllocation read( ChannelBuffer buffer, ByteBuffer temporaryBuffer ) throws IOException
                    {
                        return readIdAllocation( buffer );
                    }
                }
        );
    }

    @Override
    public Response<Integer> createRelationshipType( RequestContext context, final String name )
    {
        return sendRequest( HaRequestType18.CREATE_RELATIONSHIP_TYPE, context, new Serializer()
                {
                    public void write( ChannelBuffer buffer ) throws IOException
                    {
                        writeString( buffer, name );
                    }
                }, new Deserializer<Integer>()
                {
                    @Override
                    @SuppressWarnings("boxing")
                    public Integer read( ChannelBuffer buffer, ByteBuffer temporaryBuffer ) throws IOException
                    {
                        return buffer.readInt();
                    }
                }
        );
    }

    @Override
    public Response<Integer> createPropertyKey( RequestContext context, String name )
    {
        throw new UnsupportedOperationException( "Should never be called from the client side" );
    }

    @Override
    public Response<Integer> createLabel( RequestContext context, String name )
    {
        throw new UnsupportedOperationException( "Should never be called from the client side" );
    }

    @Override
    public Response<Void> initializeTx( RequestContext context )
    {
        return sendRequest( HaRequestType18.INITIALIZE_TX, context, EMPTY_SERIALIZER, VOID_DESERIALIZER );
    }

    @Override
    public Response<LockResult> acquireNodeWriteLock( RequestContext context, long... nodes )
    {
        return sendRequest( HaRequestType18.ACQUIRE_NODE_WRITE_LOCK, context,
                new AcquireLockSerializer( nodes ), LOCK_RESULT_DESERIALIZER );
    }

    @Override
    public Response<LockResult> acquireNodeReadLock( RequestContext context, long... nodes )
    {
        return sendRequest( HaRequestType18.ACQUIRE_NODE_READ_LOCK, context,
                new AcquireLockSerializer( nodes ), LOCK_RESULT_DESERIALIZER );
    }

    @Override
    public Response<LockResult> acquireRelationshipWriteLock( RequestContext context,
                                                              long... relationships )
    {
        return sendRequest( HaRequestType18.ACQUIRE_RELATIONSHIP_WRITE_LOCK, context,
                new AcquireLockSerializer( relationships ), LOCK_RESULT_DESERIALIZER );
    }

    @Override
    public Response<LockResult> acquireRelationshipReadLock( RequestContext context,
                                                             long... relationships )
    {
        return sendRequest( HaRequestType18.ACQUIRE_RELATIONSHIP_READ_LOCK, context,
                new AcquireLockSerializer( relationships ), LOCK_RESULT_DESERIALIZER );
    }

    @Override
    public Response<LockResult> acquireGraphWriteLock( RequestContext context )
    {
        return sendRequest( HaRequestType18.ACQUIRE_GRAPH_WRITE_LOCK, context,
                EMPTY_SERIALIZER, LOCK_RESULT_DESERIALIZER );
    }

    @Override
    public Response<LockResult> acquireGraphReadLock( RequestContext context )
    {
        return sendRequest( HaRequestType18.ACQUIRE_GRAPH_READ_LOCK, context,
                EMPTY_SERIALIZER, LOCK_RESULT_DESERIALIZER );
    }

    @Override
    public Response<LockResult> acquireIndexReadLock( RequestContext context, String index, String key )
    {
        return sendRequest( HaRequestType18.ACQUIRE_INDEX_READ_LOCK, context,
                new AcquireIndexLockSerializer( index, key ), LOCK_RESULT_DESERIALIZER );
    }

    @Override
    public Response<LockResult> acquireIndexWriteLock( RequestContext context, String index, String key )
    {
        return sendRequest( HaRequestType18.ACQUIRE_INDEX_WRITE_LOCK, context,
                new AcquireIndexLockSerializer( index, key ), LOCK_RESULT_DESERIALIZER );
    }
    
    @Override
    public Response<LockResult> acquireSchemaReadLock( RequestContext context )
    {
        throw new UnsupportedOperationException( "Should never be called from the client side" );
    }
    
    @Override
    public Response<LockResult> acquireSchemaWriteLock( RequestContext context )
    {
        throw new UnsupportedOperationException( "Should never be called from the client side" );
    }

    @Override
    public Response<LockResult> acquireIndexEntryWriteLock( RequestContext context, long labelId, long propertyKeyId,
                                                            String propertyValue )
    {
        throw new UnsupportedOperationException( "Should never be called from the client side" );
    }

    @Override
    public Response<Long> commitSingleResourceTransaction( RequestContext context,
                                                           final String resource, final TxExtractor txGetter )
    {
        return sendRequest( HaRequestType18.COMMIT, context, new Serializer()
                {
                    public void write( ChannelBuffer buffer ) throws IOException
                    {
                        writeString( buffer, resource );
                        BlockLogBuffer blockLogBuffer = new BlockLogBuffer( buffer, monitor );
                        txGetter.extract( blockLogBuffer );
                        blockLogBuffer.done();
                    }
                }, new Deserializer<Long>()
                {
                    @Override
                    @SuppressWarnings("boxing")
                    public Long read( ChannelBuffer buffer, ByteBuffer temporaryBuffer ) throws IOException
                    {
                        return buffer.readLong();
                    }
                }
        );
    }

    @Override
    public Response<Void> finishTransaction( RequestContext context, final boolean success )
    {
        try
        {
            return sendRequest( HaRequestType18.FINISH, context, new Serializer()
            {
                public void write( ChannelBuffer buffer ) throws IOException
                {
                    buffer.writeByte( success ? 1 : 0 );
                }
            }, VOID_DESERIALIZER );
        }
        catch ( TransactionAlreadyActiveException e )
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
                return new Response<>( null, getStoreId(), TransactionStream.EMPTY, ResourceReleaser.NO_OP );
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
        return sendRequest( HaRequestType18.PULL_UPDATES, context, EMPTY_SERIALIZER, VOID_DESERIALIZER );
    }

    @Override
    public Response<HandshakeResult> handshake( final long txId, StoreId storeId )
    {
        return sendRequest( HaRequestType18.HANDSHAKE, RequestContext.EMPTY, new Serializer()
                {
                    public void write( ChannelBuffer buffer ) throws IOException
                    {
                        buffer.writeLong( txId );
                    }
                }, new Deserializer<HandshakeResult>()
                {
                    @Override
                    public HandshakeResult read( ChannelBuffer buffer, ByteBuffer temporaryBuffer ) throws
                            IOException
                    {
                        return new HandshakeResult( buffer.readInt(), buffer.readLong(), -1 );
                    }
                }, storeId
        );
    }

    @Override
    public Response<Void> copyStore( RequestContext context, final StoreWriter writer )
    {
        context = stripFromTransactions( context );
        return sendRequest( HaRequestType18.COPY_STORE, context, EMPTY_SERIALIZER,
                new Protocol.FileStreamsDeserializer( writer ) );
    }

    private RequestContext stripFromTransactions( RequestContext context )
    {
        return new RequestContext( context.getEpoch(), context.machineId(), context.getEventIdentifier(),
                new RequestContext.Tx[0], context.getMasterId(), context.getChecksum() );
    }

    @Override
    public Response<Void> copyTransactions( RequestContext context,
                                            final String ds, final long startTxId, final long endTxId )
    {
        context = stripFromTransactions( context );
        return sendRequest( HaRequestType18.COPY_TRANSACTIONS, context, new Serializer()
        {
            public void write( ChannelBuffer buffer )
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
        context = stripFromTransactions( context );
        return sendRequest( HaRequestType18.PUSH_TRANSACTION, context, new Serializer()
        {
            @Override
            public void write( ChannelBuffer buffer ) throws IOException
            {
                writeString( buffer, resourceName );
                buffer.writeLong( tx );
            }
        }, VOID_DESERIALIZER );
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

        public void write( ChannelBuffer buffer ) throws IOException
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
        public void write( ChannelBuffer buffer ) throws IOException
        {
            writeString( buffer, index );
            writeString( buffer, key );
        }
    }

    public static abstract class AquireLockCall implements TargetCaller<Master, LockResult>
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

        protected abstract Response<LockResult> lock( Master master, RequestContext context, long... ids );
    }
}
