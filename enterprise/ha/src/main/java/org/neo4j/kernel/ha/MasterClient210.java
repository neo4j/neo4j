/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.jboss.netty.buffer.ChannelBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;

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
import org.neo4j.com.storecopy.ResponseUnpacker.TxHandler;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.ha.com.master.HandshakeResult;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.master.MasterServer;
import org.neo4j.kernel.ha.com.slave.MasterClient;
import org.neo4j.kernel.ha.id.IdAllocation;
import org.neo4j.kernel.ha.lock.LockResult;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.store.id.IdRange;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.logging.LogProvider;

import static org.neo4j.com.Protocol.EMPTY_SERIALIZER;
import static org.neo4j.com.Protocol.VOID_DESERIALIZER;
import static org.neo4j.com.Protocol.writeString;
import static org.neo4j.com.ProtocolVersion.INTERNAL_PROTOCOL_VERSION;

/**
 * The {@link org.neo4j.kernel.ha.com.master.Master} a slave should use to communicate with its master. It
 * serializes requests and sends them to the master, more specifically
 * {@link org.neo4j.kernel.ha.com.master.MasterServer} (which delegates to {@link org.neo4j.kernel.ha.com.master.MasterImpl}
 * on the master side.
 */
public class MasterClient210 extends Client<Master> implements MasterClient
{
    /* Version 1 first version
     * Version 2 since 2012-01-24
     * Version 3 since 2012-02-16
     * Version 4 since 2012-07-05
     * Version 5 since ?
     * Version 6 since 2014-01-07
     * Version 7 since 2014-03-18
     */
    public static final ProtocolVersion PROTOCOL_VERSION = new ProtocolVersion( (byte) 7, INTERNAL_PROTOCOL_VERSION );

    private final long lockReadTimeoutMillis;

    public MasterClient210( String destinationHostNameOrIp, int destinationPort, String originHostNameOrIp,
                            LogProvider logProvider, StoreId storeId, long readTimeoutMillis,
                            long lockReadTimeoutMillis, int maxConcurrentChannels, int chunkSize,
                            ResponseUnpacker responseUnpacker,
                            ByteCounterMonitor byteCounterMonitor, RequestMonitor requestMonitor )
    {
        super( destinationHostNameOrIp, destinationPort, originHostNameOrIp, logProvider, storeId,
                MasterServer.FRAME_LENGTH, PROTOCOL_VERSION, readTimeoutMillis,
                maxConcurrentChannels, chunkSize, responseUnpacker, byteCounterMonitor, requestMonitor );
        this.lockReadTimeoutMillis = lockReadTimeoutMillis;
    }

    MasterClient210( String destinationHostNameOrIp, int destinationPort, String originHostNameOrIp,
                     LogProvider logProvider, StoreId storeId, long readTimeoutMillis,
                     long lockReadTimeoutMillis, int maxConcurrentChannels, int chunkSize,
                     ProtocolVersion protocolVersion, ResponseUnpacker responseUnpacker,
                     ByteCounterMonitor byteCounterMonitor, RequestMonitor requestMonitor )
    {
        super( destinationHostNameOrIp, destinationPort, originHostNameOrIp, logProvider, storeId,
                MasterServer.FRAME_LENGTH, protocolVersion, readTimeoutMillis, maxConcurrentChannels, chunkSize,
                responseUnpacker, byteCounterMonitor, requestMonitor );
        this.lockReadTimeoutMillis = lockReadTimeoutMillis;
    }

    @Override
    protected Protocol createProtocol( int chunkSize, byte applicationProtocolVersion )
    {
        return new Protocol201( chunkSize, applicationProtocolVersion, getInternalProtocolVersion() );
    }

    @Override
    protected long getReadTimeout( RequestType<Master> type, long readTimeout )
    {
        HaRequestType210 specificType = (HaRequestType210) type;
        if ( specificType.isLock() )
        {
            return lockReadTimeoutMillis;
        }
        if ( specificType == HaRequestType210.COPY_STORE )
        {
            return readTimeout * 2;
        }
        return readTimeout;
    }

    @Override
    protected boolean shouldCheckStoreId( RequestType<Master> type )
    {
        return type != HaRequestType210.COPY_STORE;
    }

    @Override
    public Response<IdAllocation> allocateIds( RequestContext context, final IdType idType )
    {
        return sendRequest( HaRequestType210.ALLOCATE_IDS, context, new Serializer()
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
        return sendRequest( HaRequestType210.CREATE_RELATIONSHIP_TYPE, context, new Serializer()
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
        return sendRequest( HaRequestType210.CREATE_PROPERTY_KEY, context, new Serializer()
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
        return sendRequest( HaRequestType210.CREATE_LABEL, context, new Serializer()
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
        return sendRequest( HaRequestType210.NEW_LOCK_SESSION, context, EMPTY_SERIALIZER, VOID_DESERIALIZER );
    }

    @Override
    public Response<LockResult> acquireSharedLock( RequestContext context, Locks.ResourceType type, long...
            resourceIds )
    {
        return sendRequest( HaRequestType210.ACQUIRE_SHARED_LOCK, context,
                new AcquireLockSerializer( type, resourceIds ), LOCK_RESULT_DESERIALIZER );
    }

    @Override
    public Response<LockResult> acquireExclusiveLock( RequestContext context, Locks.ResourceType type, long...
            resourceIds )
    {
        return sendRequest( HaRequestType210.ACQUIRE_EXCLUSIVE_LOCK, context,
                new AcquireLockSerializer( type, resourceIds ), LOCK_RESULT_DESERIALIZER );
    }

    @Override
    public Response<Long> commit( RequestContext context, TransactionRepresentation tx )
    {
        return sendRequest( HaRequestType210.COMMIT, context, new Protocol.TransactionSerializer( tx ),
                new Deserializer<Long>()
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
    public Response<Void> endLockSession( RequestContext context, final boolean success )
    {
        return sendRequest( HaRequestType210.END_LOCK_SESSION, context, new Serializer()
        {
            @Override
            public void write( ChannelBuffer buffer ) throws IOException
            {
                buffer.writeByte( success ? 1 : 0 );
            }
        }, VOID_DESERIALIZER );
    }

    @Override
    public Response<Void> pullUpdates( RequestContext context )
    {
        return pullUpdates( context, ResponseUnpacker.NO_OP_TX_HANDLER );
    }

    @Override
    public Response<Void> pullUpdates( RequestContext context, TxHandler txHandler )
    {
        return sendRequest( HaRequestType210.PULL_UPDATES, context, EMPTY_SERIALIZER, VOID_DESERIALIZER, null, txHandler );
    }

    @Override
    public Response<HandshakeResult> handshake( final long txId, StoreId storeId )
    {
        return sendRequest( HaRequestType210.HANDSHAKE, RequestContext.EMPTY, new Serializer()
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
                        return new HandshakeResult( buffer.readLong(), buffer.readLong() );
                    }
                }, storeId, ResponseUnpacker.NO_OP_TX_HANDLER
        );
    }

    @Override
    public Response<Void> copyStore( RequestContext context, final StoreWriter writer )
    {
        context = stripFromTransactions( context );
        return sendRequest( HaRequestType210.COPY_STORE, context, EMPTY_SERIALIZER,
                new Protocol.FileStreamsDeserializer( writer ) );
    }

    private RequestContext stripFromTransactions( RequestContext context )
    {
        return new RequestContext( context.getEpoch(), context.machineId(), context.getEventIdentifier(),
                0, context.getChecksum() );
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
        private final Locks.ResourceType type;
        private final long[] resourceIds;

        AcquireLockSerializer( Locks.ResourceType type, long... resourceIds )
        {
            this.type = type;
            this.resourceIds = resourceIds;
        }

        @Override
        public void write( ChannelBuffer buffer ) throws IOException
        {
            buffer.writeInt( type.typeId() );
            buffer.writeInt( resourceIds.length );
            for ( long entity : resourceIds )
            {
                buffer.writeLong( entity );
            }
        }
    }
}
