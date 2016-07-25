/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.com.Deserializer;
import org.neo4j.com.ObjectSerializer;
import org.neo4j.com.Protocol;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.TargetCaller;
import org.neo4j.com.storecopy.ToNetworkStoreWriter;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.ha.com.master.HandshakeResult;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.id.IdAllocation;
import org.neo4j.kernel.ha.lock.LockResult;
import org.neo4j.kernel.impl.store.id.IdRange;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.storageengine.api.lock.ResourceType;

import static org.neo4j.com.Protocol.INTEGER_SERIALIZER;
import static org.neo4j.com.Protocol.LONG_SERIALIZER;
import static org.neo4j.com.Protocol.VOID_SERIALIZER;
import static org.neo4j.com.Protocol.readBoolean;
import static org.neo4j.com.Protocol.readString;
import static org.neo4j.kernel.ha.com.slave.MasterClient.LOCK_SERIALIZER;

public class HaRequestType210 extends AbstractHaRequestTypes
{
    public HaRequestType210( LogEntryReader<ReadableClosablePositionAwareChannel> entryReader )
    {
        register( Type.ALLOCATE_IDS, new TargetCaller<Master, IdAllocation>()
        {
            @Override
            public Response<IdAllocation> call( Master master, RequestContext context, ChannelBuffer input,
                    ChannelBuffer target )
            {
                IdType idType = IdType.values()[input.readByte()];
                return master.allocateIds( context, idType );
            }
        }, new ObjectSerializer<IdAllocation>()
        {
            @Override
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
        } );
        register( Type.CREATE_RELATIONSHIP_TYPE, new TargetCaller<Master, Integer>()
        {
            @Override
            public Response<Integer> call( Master master, RequestContext context, ChannelBuffer input,
                    ChannelBuffer target )
            {
                return master.createRelationshipType( context, readString( input ) );
            }
        }, INTEGER_SERIALIZER );
        register( Type.ACQUIRE_EXCLUSIVE_LOCK, new AquireLockCall()
        {
            @Override
            protected Response<LockResult> lock( Master master, RequestContext context, ResourceType type,
                    long... ids )
            {
                return master.acquireExclusiveLock( context, type, ids );
            }
        }, LOCK_SERIALIZER, true );
        register( Type.ACQUIRE_SHARED_LOCK, new AquireLockCall()
        {
            @Override
            protected Response<LockResult> lock( Master master, RequestContext context, ResourceType type,
                    long... ids )
            {
                return master.acquireSharedLock( context, type, ids );
            }
        }, LOCK_SERIALIZER, true );
        register( Type.COMMIT, new TargetCaller<Master, Long>()
        {
            @Override
            public Response<Long> call( Master master, RequestContext context, ChannelBuffer input,
                                        ChannelBuffer target ) throws IOException, TransactionFailureException
            {
                readString( input ); // Always neostorexadatasource

                TransactionRepresentation tx = null;
                try
                {
                    Deserializer<TransactionRepresentation> deserializer =
                            new Protocol.TransactionRepresentationDeserializer( entryReader );
                    tx = deserializer.read( input, null );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }

                return master.commit( context, tx );
            }
        }, LONG_SERIALIZER );
        register( Type.PULL_UPDATES, new TargetCaller<Master, Void>()
        {
            @Override
            public Response<Void> call( Master master, RequestContext context, ChannelBuffer input,
                    ChannelBuffer target )
            {
                return master.pullUpdates( context );
            }
        }, VOID_SERIALIZER );
        register(
                Type.END_LOCK_SESSION,
                new TargetCaller<Master,Void>()
                {
                    @Override
                    public Response<Void> call( Master master, RequestContext context, ChannelBuffer input,
                                                ChannelBuffer target )
                    {
                        return master.endLockSession( context, readBoolean( input ) );
                    }
                },
                VOID_SERIALIZER,
                /*
                A 'false' argument here means we won't unpack the response.

                We do this because END_LOCK_SESSION request can be send in 3 cases:
                 1) transaction committed successfully
                 2) transaction rolled back successfully
                 3) transaction was terminated

                Master's response for this call is an obligation to pull up to a specified txId.
                Processing/unpacking of this response is not needed in all 3 cases:
                 1) committed transaction pulls transaction stream as part of COMMIT call
                 2) rolled back transaction does not care about reading any more
                 3) terminated transaction does not care about reading any more
                */
                false );
        register( Type.HANDSHAKE, new TargetCaller<Master, HandshakeResult>()
        {
            @Override
            public Response<HandshakeResult> call( Master master, RequestContext context, ChannelBuffer input,
                    ChannelBuffer target )
            {
                return master.handshake( input.readLong(), null );
            }
        }, new ObjectSerializer<HandshakeResult>()
        {
            @Override
            public void write( HandshakeResult responseObject, ChannelBuffer result ) throws IOException
            {
                result.writeLong( responseObject.txChecksum() );
                result.writeLong( responseObject.epoch() );
            }
        } );
        register( Type.COPY_STORE, new TargetCaller<Master, Void>()
        {
            @Override
            public Response<Void> call( Master master, RequestContext context, ChannelBuffer input,
                    final ChannelBuffer target )
            {
                return master.copyStore( context, new ToNetworkStoreWriter( target, new Monitors() ) );
            }

        }, VOID_SERIALIZER, false );
        register( Type.NEW_LOCK_SESSION, new TargetCaller<Master, Void>()
        {
            @Override
            public Response<Void> call( Master master, RequestContext context, ChannelBuffer input,
                    ChannelBuffer target ) throws TransactionFailureException
            {
                return master.newLockSession( context );
            }
        }, VOID_SERIALIZER );
        register( Type.CREATE_PROPERTY_KEY, new TargetCaller<Master, Integer>()
        {
            @Override
            public Response<Integer> call( Master master, RequestContext context, ChannelBuffer input,
                    ChannelBuffer target )
            {
                return master.createPropertyKey( context, readString( input ) );
            }
        }, INTEGER_SERIALIZER );
        register( Type.CREATE_LABEL, new TargetCaller<Master, Integer>()
        {
            @Override
            public Response<Integer> call( Master master, RequestContext context, ChannelBuffer input,
                                           ChannelBuffer target )
            {
                return master.createLabel( context, readString( input ) );
            }
        }, INTEGER_SERIALIZER );
    }
}
