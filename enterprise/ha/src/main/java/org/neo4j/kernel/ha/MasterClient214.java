/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.ha;

import org.jboss.netty.buffer.ChannelBuffer;

import org.neo4j.com.Client;
import org.neo4j.com.Deserializer;
import org.neo4j.com.ObjectSerializer;
import org.neo4j.com.Protocol;
import org.neo4j.com.Protocol214;
import org.neo4j.com.ProtocolVersion;
import org.neo4j.com.RequestContext;
import org.neo4j.com.RequestType;
import org.neo4j.com.Response;
import org.neo4j.com.Serializer;
import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.com.storecopy.ResponseUnpacker;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.ha.com.master.HandshakeResult;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.master.MasterServer;
import org.neo4j.kernel.ha.com.slave.MasterClient;
import org.neo4j.kernel.ha.id.IdAllocation;
import org.neo4j.kernel.ha.lock.LockResult;
import org.neo4j.kernel.ha.lock.LockStatus;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.store.id.IdRange;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.lock.ResourceType;

import static java.lang.String.format;
import static org.neo4j.com.Protocol.EMPTY_SERIALIZER;
import static org.neo4j.com.Protocol.VOID_DESERIALIZER;
import static org.neo4j.com.Protocol.readString;
import static org.neo4j.com.Protocol.writeString;
import static org.neo4j.com.ProtocolVersion.INTERNAL_PROTOCOL_VERSION;

/**
 * The {@link org.neo4j.kernel.ha.com.master.Master} a slave should use to communicate with its master. It
 * serializes requests and sends them to the master, more specifically
 * {@link org.neo4j.kernel.ha.com.master.MasterServer} (which delegates to
 * {@link org.neo4j.kernel.ha.com.master.MasterImpl}
 * on the master side.
 */
public class MasterClient214 extends Client<Master> implements MasterClient
{
    public static final ProtocolVersion PROTOCOL_VERSION = new ProtocolVersion( (byte) 8, INTERNAL_PROTOCOL_VERSION );

    public static final ObjectSerializer<LockResult> LOCK_RESULT_OBJECT_SERIALIZER = ( responseObject, result ) ->
    {
        result.writeByte( responseObject.getStatus().ordinal() );
        if ( responseObject.getStatus() == LockStatus.DEAD_LOCKED )
        {
            writeString( result, responseObject.getMessage() );
        }
    };

    public static final Deserializer<LockResult> LOCK_RESULT_DESERIALIZER = ( buffer, temporaryBuffer ) ->
    {
        byte statusOrdinal = buffer.readByte();
        LockStatus status;
        try
        {
            status = LockStatus.values()[statusOrdinal];
        }
        catch ( ArrayIndexOutOfBoundsException e )
        {
            throw withInvalidOrdinalMessage( buffer, statusOrdinal, e );
        }
        return status == LockStatus.DEAD_LOCKED ? new LockResult( LockStatus.DEAD_LOCKED, readString( buffer ) )
                                   : new LockResult( status );
    };

    protected static ArrayIndexOutOfBoundsException withInvalidOrdinalMessage(
            ChannelBuffer buffer, byte statusOrdinal, ArrayIndexOutOfBoundsException e )
    {
        int maxBytesToPrint = 1024 * 40;
        return Exceptions.withMessage( e,
                format( "%s | read invalid ordinal %d. First %db of this channel buffer is:%n%s",
                        e.getMessage(), statusOrdinal, maxBytesToPrint,
                        beginningOfBufferAsHexString( buffer, maxBytesToPrint ) ) );
    }

    private final long lockReadTimeoutMillis;
    private final HaRequestTypes requestTypes;
    private final Deserializer<LockResult> lockResultDeserializer;

    public MasterClient214( String destinationHostNameOrIp, int destinationPort, String originHostNameOrIp,
                            LogProvider logProvider, StoreId storeId, long readTimeoutMillis,
                            long lockReadTimeoutMillis, int maxConcurrentChannels, int chunkSize,
                            ResponseUnpacker responseUnpacker,
                            ByteCounterMonitor byteCounterMonitor, RequestMonitor requestMonitor,
                            LogEntryReader<ReadableClosablePositionAwareChannel> entryReader )
    {
        super( destinationHostNameOrIp, destinationPort, originHostNameOrIp, logProvider, storeId,
                MasterServer.FRAME_LENGTH, readTimeoutMillis, maxConcurrentChannels, chunkSize,
                responseUnpacker, byteCounterMonitor, requestMonitor, entryReader );
        this.lockReadTimeoutMillis = lockReadTimeoutMillis;
        this.requestTypes = new HaRequestType210( entryReader, createLockResultSerializer() );
        this.lockResultDeserializer = createLockResultDeserializer();
    }

    @Override
    protected Protocol createProtocol( int chunkSize, byte applicationProtocolVersion )
    {
        return new Protocol214( chunkSize, applicationProtocolVersion, getInternalProtocolVersion() );
    }

    @Override
    public ProtocolVersion getProtocolVersion()
    {
        return PROTOCOL_VERSION;
    }

    @Override
    public ObjectSerializer<LockResult> createLockResultSerializer()
    {
        return LOCK_RESULT_OBJECT_SERIALIZER;
    }

    @Override
    public Deserializer<LockResult> createLockResultDeserializer()
    {
        return LOCK_RESULT_DESERIALIZER;
    }

    @Override
    protected long getReadTimeout( RequestType type, long readTimeout )
    {
        if ( HaRequestTypes.Type.ACQUIRE_EXCLUSIVE_LOCK.is( type ) ||
             HaRequestTypes.Type.ACQUIRE_SHARED_LOCK.is( type ) )
        {
            return lockReadTimeoutMillis;
        }
        if ( HaRequestTypes.Type.COPY_STORE.is( type ) )
        {
            return readTimeout * 2;
        }
        return readTimeout;
    }

    @Override
    protected boolean shouldCheckStoreId( RequestType type )
    {
        return !HaRequestTypes.Type.COPY_STORE.is( type );
    }

    @Override
    public Response<IdAllocation> allocateIds( RequestContext context, final IdType idType )
    {
        Serializer serializer = buffer -> buffer.writeByte( idType.ordinal() );
        Deserializer<IdAllocation> deserializer = ( buffer, temporaryBuffer ) -> readIdAllocation( buffer );
        return sendRequest( requestTypes.type( HaRequestTypes.Type.ALLOCATE_IDS ), context, serializer, deserializer );
    }

    @Override
    public Response<Integer> createRelationshipType( RequestContext context, final String name )
    {
        Serializer serializer = buffer -> writeString( buffer, name );
        Deserializer<Integer> deserializer = ( buffer, temporaryBuffer ) -> buffer.readInt();
        return sendRequest( requestTypes.type( HaRequestTypes.Type.CREATE_RELATIONSHIP_TYPE ), context, serializer,
                deserializer );
    }

    @Override
    public Response<Integer> createPropertyKey( RequestContext context, final String name )
    {
        Serializer serializer = buffer -> writeString( buffer, name );
        Deserializer<Integer> deserializer = ( buffer, temporaryBuffer ) -> buffer.readInt();
        return sendRequest( requestTypes.type( HaRequestTypes.Type.CREATE_PROPERTY_KEY ), context, serializer,
                deserializer );
    }

    @Override
    public Response<Integer> createLabel( RequestContext context, final String name )
    {
        Serializer serializer = buffer -> writeString( buffer, name );
        Deserializer<Integer> deserializer = ( buffer, temporaryBuffer ) -> buffer.readInt();
        return sendRequest( requestTypes.type( HaRequestTypes.Type.CREATE_LABEL ), context, serializer, deserializer );
    }

    @Override
    public Response<Void> newLockSession( RequestContext context )
    {
        return sendRequest( requestTypes.type( HaRequestTypes.Type.NEW_LOCK_SESSION ), context, EMPTY_SERIALIZER,
                VOID_DESERIALIZER );
    }

    @Override
    public Response<LockResult> acquireSharedLock(
            RequestContext context, ResourceType type, long... resourceIds )
    {
        return sendRequest( requestTypes.type( HaRequestTypes.Type.ACQUIRE_SHARED_LOCK ), context,
                new AcquireLockSerializer( type, resourceIds ), lockResultDeserializer );
    }

    @Override
    public Response<LockResult> acquireExclusiveLock(
            RequestContext context, ResourceType type, long... resourceIds )
    {
        return sendRequest( requestTypes.type( HaRequestTypes.Type.ACQUIRE_EXCLUSIVE_LOCK ), context,
                new AcquireLockSerializer( type, resourceIds ), lockResultDeserializer );
    }

    @Override
    public Response<Long> commit( RequestContext context, TransactionRepresentation tx )
    {
        Serializer serializer = new Protocol.TransactionSerializer( tx );
        Deserializer<Long> deserializer = ( buffer, temporaryBuffer ) -> buffer.readLong();
        return sendRequest( requestTypes.type( HaRequestTypes.Type.COMMIT ), context, serializer, deserializer );
    }

    @Override
    public Response<Void> endLockSession( RequestContext context, final boolean success )
    {
        Serializer serializer = buffer -> buffer.writeByte( success ? 1 : 0 );
        return sendRequest( requestTypes.type( HaRequestTypes.Type.END_LOCK_SESSION ), context, serializer,
                VOID_DESERIALIZER );
    }

    @Override
    public Response<Void> pullUpdates( RequestContext context )
    {
        return pullUpdates( context, ResponseUnpacker.TxHandler.NO_OP_TX_HANDLER );
    }

    @Override
    public Response<Void> pullUpdates( RequestContext context, ResponseUnpacker.TxHandler txHandler )
    {
        return sendRequest( requestTypes.type( HaRequestTypes.Type.PULL_UPDATES ), context,
                EMPTY_SERIALIZER, VOID_DESERIALIZER, null, txHandler );
    }

    @Override
    public Response<HandshakeResult> handshake( final long txId, StoreId storeId )
    {
        Serializer serializer = buffer -> buffer.writeLong( txId );
        Deserializer<HandshakeResult> deserializer =
                ( buffer, temporaryBuffer ) -> new HandshakeResult( buffer.readLong(), buffer.readLong() );
        return sendRequest( requestTypes.type( HaRequestTypes.Type.HANDSHAKE ), RequestContext.EMPTY,
                serializer, deserializer, storeId, ResponseUnpacker.TxHandler.NO_OP_TX_HANDLER );
    }

    @Override
    public Response<Void> copyStore( RequestContext context, final StoreWriter writer )
    {
        context = stripFromTransactions( context );
        return sendRequest( requestTypes.type( HaRequestTypes.Type.COPY_STORE ), context, EMPTY_SERIALIZER,
                createFileStreamDeserializer( writer ) );
    }

    protected Deserializer<Void> createFileStreamDeserializer( StoreWriter writer )
    {
        return new Protocol.FileStreamsDeserializer210( writer );
    }

    private RequestContext stripFromTransactions( RequestContext context )
    {
        return new RequestContext( context.getEpoch(), context.machineId(), context.getEventIdentifier(),
                0, context.getChecksum() );
    }

    private static IdAllocation readIdAllocation( ChannelBuffer buffer )
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

    private static class AcquireLockSerializer implements Serializer
    {
        private final ResourceType type;
        private final long[] resourceIds;

        AcquireLockSerializer( ResourceType type, long... resourceIds )
        {
            this.type = type;
            this.resourceIds = resourceIds;
        }

        @Override
        public void write( ChannelBuffer buffer )
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
