/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.jboss.netty.buffer.ChannelBuffer;

import org.neo4j.com.Client;
import org.neo4j.com.Deserializer;
import org.neo4j.com.Protocol;
import org.neo4j.com.Protocol201;
import org.neo4j.com.ProtocolVersion;
import org.neo4j.com.RequestContext;
import org.neo4j.com.RequestType;
import org.neo4j.com.Response;
import org.neo4j.com.Serializer;
import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.com.storecopy.ResponseUnpacker;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.ha.com.master.HandshakeResult;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.master.MasterImpl;
import org.neo4j.kernel.ha.com.master.MasterServer;
import org.neo4j.kernel.ha.com.slave.MasterClient;
import org.neo4j.kernel.ha.id.IdAllocation;
import org.neo4j.kernel.ha.lock.LockResult;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.store.id.IdRange;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;

import static org.neo4j.com.Protocol.EMPTY_SERIALIZER;
import static org.neo4j.com.Protocol.VOID_DESERIALIZER;
import static org.neo4j.com.Protocol.writeString;
import static org.neo4j.com.ProtocolVersion.INTERNAL_PROTOCOL_VERSION;

/**
 * The {@link Master} a slave should use to communicate with its master. It
 * serializes requests and sends them to the master, more specifically
 * {@link MasterServer} (which delegates to {@link MasterImpl}
 * on the master side.
 */
public class MasterClient201 extends Client<Master> implements MasterClient
{
    /* Version 1 first version
     * Version 2 since 2012-01-24
     * Version 3 since 2012-02-16
     * Version 4 since 2012-07-05
     * Version 5 since ?
     * Version 6 since 2014-01-07
     */
    public static final ProtocolVersion PROTOCOL_VERSION = new ProtocolVersion( (byte) 6, INTERNAL_PROTOCOL_VERSION );

    private final long lockReadTimeout;

    public MasterClient201( String hostNameOrIp, int port, Logging logging, StoreId storeId, long readTimeoutSeconds,
                            long lockReadTimeout, int maxConcurrentChannels, int chunkSize, ResponseUnpacker unpacker,
                            ByteCounterMonitor byteCounterMonitor, RequestMonitor requestMonitor )
    {
        super( hostNameOrIp, port, logging, storeId, MasterServer.FRAME_LENGTH, PROTOCOL_VERSION, readTimeoutSeconds,
                maxConcurrentChannels, chunkSize, unpacker, byteCounterMonitor, requestMonitor );
        this.lockReadTimeout = lockReadTimeout;
    }

    @Override
    protected Protocol createProtocol( int chunkSize, byte applicationProtocolVersion )
    {
        return new Protocol201( chunkSize, applicationProtocolVersion, getInternalProtocolVersion() );
    }

    @Override
    protected long getReadTimeout( RequestType<Master> type, long readTimeout )
    {
        HaRequestType201 specificType = (HaRequestType201) type;
        if ( specificType.isLock() )
        {
            return lockReadTimeout;
        }
        if ( specificType == HaRequestType201.COPY_STORE )
        {
            return readTimeout * 2;
        }
        return readTimeout;
    }

    @Override
    protected boolean shouldCheckStoreId( RequestType<Master> type )
    {
        return type != HaRequestType201.COPY_STORE;
    }

    @Override
    public Response<IdAllocation> allocateIds( RequestContext context, final IdType idType )
    {
        return sendRequest( HaRequestType201.ALLOCATE_IDS, context, new Serializer()
                {
                    @Override
                    public void write( ChannelBuffer buffer ) throws IOException
                    {
                        buffer.writeByte( idType.ordinal() );
                    }
                }, new Deserializer<IdAllocation>()
                {
                    @Override
                    public IdAllocation read( ChannelBuffer buffer, ByteBuffer temporaryBuffer )
                    {
                        return readIdAllocation( buffer );
                    }
                }
        );
    }

    @Override
    public Response<Integer> createRelationshipType( RequestContext context, final String name )
    {
        return sendRequest( HaRequestType201.CREATE_RELATIONSHIP_TYPE, context, new Serializer()
        {
            @Override
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
        } );
    }

    @Override
    public Response<Integer> createPropertyKey( RequestContext context, final String name )
    {
        return sendRequest( HaRequestType201.CREATE_PROPERTY_KEY, context, new Serializer()
        {
            @Override
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
        } );
    }

    @Override
    public Response<Integer> createLabel( RequestContext context, final String name )
    {
        return sendRequest( HaRequestType201.CREATE_LABEL, context, new Serializer()
                {
                    @Override
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
        });
    }

    @Override
    public Response<Void> newLockSession( RequestContext context )
    {
        return sendRequest( HaRequestType201.INITIALIZE_TX, context, EMPTY_SERIALIZER, VOID_DESERIALIZER );
    }

    @Override
    public Response<LockResult> acquireExclusiveLock( RequestContext context, Locks.ResourceType resourceType, long...
            resourceIds )
    {
        if ( resourceType == ResourceTypes.NODE )
        {
            return sendRequest( HaRequestType201.ACQUIRE_NODE_WRITE_LOCK, context,
                    new AcquireLockSerializer( resourceIds ), LOCK_RESULT_DESERIALIZER );
        }
        else if ( resourceType == ResourceTypes.RELATIONSHIP )
        {
            return sendRequest( HaRequestType201.ACQUIRE_RELATIONSHIP_WRITE_LOCK, context,
                    new AcquireLockSerializer( resourceIds ), LOCK_RESULT_DESERIALIZER );
        }
        else if ( resourceType == ResourceTypes.GRAPH_PROPS )
        {
            return sendRequest( HaRequestType201.ACQUIRE_GRAPH_WRITE_LOCK, context, EMPTY_SERIALIZER,
                    LOCK_RESULT_DESERIALIZER );
        }
        else if( resourceType == ResourceTypes.SCHEMA )
        {
            return sendRequest( HaRequestType201.ACQUIRE_SCHEMA_WRITE_LOCK, context, EMPTY_SERIALIZER,
                    LOCK_RESULT_DESERIALIZER );
        }
        else
        {
            throw new IllegalArgumentException
                    ("Don't know how to take lock on resource: '" + resourceType + "'.");
        }
    }

    @Override
    public Response<LockResult> acquireSharedLock( RequestContext context, Locks.ResourceType resourceType, long...
            resourceIds )
    {
        if ( resourceType == ResourceTypes.NODE )
        {
            return sendRequest( HaRequestType201.ACQUIRE_NODE_READ_LOCK, context,
                    new AcquireLockSerializer( resourceIds ), LOCK_RESULT_DESERIALIZER );
        }
        else if ( resourceType == ResourceTypes.RELATIONSHIP )
        {
            return sendRequest( HaRequestType201.ACQUIRE_RELATIONSHIP_READ_LOCK, context,
                    new AcquireLockSerializer( resourceIds ), LOCK_RESULT_DESERIALIZER );
        }
        else if ( resourceType == ResourceTypes.GRAPH_PROPS )
        {
            return sendRequest( HaRequestType201.ACQUIRE_GRAPH_READ_LOCK, context, EMPTY_SERIALIZER,
                    LOCK_RESULT_DESERIALIZER );
        }
        else if( resourceType == ResourceTypes.SCHEMA )
        {
            return sendRequest( HaRequestType201.ACQUIRE_SCHEMA_READ_LOCK, context, EMPTY_SERIALIZER,
                    LOCK_RESULT_DESERIALIZER );
        }
        else
        {
            throw new IllegalArgumentException
                    ("Don't know how to take lock on resource: '" + resourceType + "'.");
        }
    }

    @Override
    public Response<Long> commit( RequestContext context, final TransactionRepresentation tx )
    {
//        return sendRequest( HaRequestType201.COMMIT, context, new Serializer()
//                {
//                    @Override
//                    public void write( ChannelBuffer buffer ) throws IOException
//                    {
//                        writeString( buffer, NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME );
//                        BlockLogBuffer blockLogBuffer = new BlockLogBuffer( buffer, monitor );
////                        txGetter.extract( blockLogBuffer );
////                        fds
//                        blockLogBuffer.done();
//                    }
//                }, new Deserializer<Long>()
//                {
//                    @Override
//                    @SuppressWarnings("boxing")
//                    public Long read( ChannelBuffer buffer, ByteBuffer temporaryBuffer ) throws IOException
//                    {
//                        return buffer.readLong();
//                    }
//                }
//        );
        // TODO 2.2-future - i have no idea how to fix this for backwards compatibility
        return null;
    }

    @Override
    public Response<Void> endLockSession( RequestContext context, final boolean success )
    {
        return sendRequest( HaRequestType201.FINISH, context, new Serializer()
        {
            @Override
            public void write( ChannelBuffer buffer ) throws IOException
            {
                buffer.writeByte( success ? 1 : 0 );
            }
        }, VOID_DESERIALIZER );
    }

    @Override
    public void rollbackOngoingTransactions( RequestContext context )
    {
        throw new UnsupportedOperationException( "Should never be called from the client side" );
    }

    @Override
    public Response<Void> pullUpdates( RequestContext context )
    {
        return sendRequest( HaRequestType201.PULL_UPDATES, context, EMPTY_SERIALIZER, VOID_DESERIALIZER );
    }

    @Override
    public Response<HandshakeResult> handshake( final long txId, StoreId storeId )
    {
        return sendRequest( HaRequestType201.HANDSHAKE, RequestContext.EMPTY, new Serializer()
                {
                    @Override
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
                        return new HandshakeResult( buffer.readInt(), buffer.readLong(), buffer.readLong() );
                    }
                }, storeId
        );
    }

    @Override
    public Response<Void> copyStore( RequestContext context, final StoreWriter writer )
    {
        context = stripFromTransactions( context );
        return sendRequest( HaRequestType201.COPY_STORE, context, EMPTY_SERIALIZER,
                new Protocol.FileStreamsDeserializer( writer ) );
    }

    private RequestContext stripFromTransactions( RequestContext context )
    {
        return new RequestContext( context.getEpoch(), context.machineId(), context.getEventIdentifier(),
                0, context.getMasterId(), context.getChecksum() );
    }

    @Override
    public ProtocolVersion getProtocolVersion()
    {
        return PROTOCOL_VERSION;
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

        @Override
        public void write( ChannelBuffer buffer ) throws IOException
        {
            buffer.writeInt( entities.length );
            for ( long entity : entities )
            {
                buffer.writeLong( entity );
            }
        }
    }
}
