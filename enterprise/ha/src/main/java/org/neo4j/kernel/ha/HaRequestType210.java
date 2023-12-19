/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.ha;

import java.io.IOException;

import org.neo4j.com.Deserializer;
import org.neo4j.com.ObjectSerializer;
import org.neo4j.com.Protocol;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.TargetCaller;
import org.neo4j.com.storecopy.ToNetworkStoreWriter;
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

public class HaRequestType210 extends AbstractHaRequestTypes
{
    public HaRequestType210(
            LogEntryReader<ReadableClosablePositionAwareChannel> entryReader,
            ObjectSerializer<LockResult> lockResultObjectSerializer )
    {
        registerAllocateIds();
        registerCreateRelationshipType();
        registerAcquireExclusiveLock( lockResultObjectSerializer );
        registerAcquireSharedLock( lockResultObjectSerializer );
        registerCommit( entryReader );
        registerPullUpdates();
        registerEndLockSession();
        registerHandshake();
        registerCopyStore();
        registerNewLockSession();
        registerCreatePropertyKey();
        registerCreateLabel();
    }

    private void registerAllocateIds()
    {
        TargetCaller<Master,IdAllocation> allocateIdTarget = ( master, context, input, target ) ->
        {
            IdType idType = IdType.values()[input.readByte()];
            return master.allocateIds( context, idType );
        };
        ObjectSerializer<IdAllocation> allocateIdSerializer = ( idAllocation, result ) ->
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
        };
        register( Type.ALLOCATE_IDS, allocateIdTarget, allocateIdSerializer );
    }

    private void registerCreateRelationshipType()
    {
        TargetCaller<Master,Integer> createRelationshipTypeTarget =
                ( master, context, input, target ) -> master.createRelationshipType( context, readString( input ) );
        register( Type.CREATE_RELATIONSHIP_TYPE, createRelationshipTypeTarget, INTEGER_SERIALIZER );
    }

    private void registerAcquireExclusiveLock( ObjectSerializer<LockResult> lockResultObjectSerializer )
    {
        register( Type.ACQUIRE_EXCLUSIVE_LOCK, new AquireLockCall()
        {
            @Override
            protected Response<LockResult> lock( Master master, RequestContext context, ResourceType type,
                                                 long... ids )
            {
                return master.acquireExclusiveLock( context, type, ids );
            }
        }, lockResultObjectSerializer, true );
    }

    private void registerAcquireSharedLock( ObjectSerializer<LockResult> lockResultObjectSerializer )
    {
        register( Type.ACQUIRE_SHARED_LOCK, new AquireLockCall()
        {
            @Override
            protected Response<LockResult> lock( Master master, RequestContext context, ResourceType type,
                                                 long... ids )
            {
                return master.acquireSharedLock( context, type, ids );
            }
        }, lockResultObjectSerializer, true );
    }

    private void registerCommit( LogEntryReader<ReadableClosablePositionAwareChannel> entryReader )
    {
        TargetCaller<Master,Long> commitTarget = ( master, context, input, target ) ->
        {
            readString( input ); // Always neostorexadatasource

            TransactionRepresentation tx;
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
        };
        register( Type.COMMIT, commitTarget, LONG_SERIALIZER );
    }

    private void registerPullUpdates()
    {
        TargetCaller<Master,Void> pullUpdatesTarget =
                ( master, context, input, target ) -> master.pullUpdates( context );
        register( Type.PULL_UPDATES, pullUpdatesTarget, VOID_SERIALIZER );
    }

    private void registerEndLockSession()
    {
        // NOTE <1>: A 'false' argument for 'unpack' means we won't unpack the response.
        // We do this because END_LOCK_SESSION request can be send in 3 cases:
        //  1) transaction committed successfully
        //  2) transaction rolled back successfully
        //  3) transaction was terminated
        // Master's response for this call is an obligation to pull up to a specified txId.
        // Processing/unpacking of this response is not needed in all 3 cases:
        //  1) committed transaction pulls transaction stream as part of COMMIT call
        //  2) rolled back transaction does not care about reading any more
        //  3) terminated transaction does not care about reading any more
        TargetCaller<Master,Void> endLockSessionTarget =
                ( master, context, input, target ) -> master.endLockSession( context, readBoolean( input ) );
        register( Type.END_LOCK_SESSION, endLockSessionTarget, VOID_SERIALIZER, false /* <1> */);
    }

    private void registerHandshake()
    {
        TargetCaller<Master,HandshakeResult> handshakeTarget =
                ( master, context, input, target ) -> master.handshake( input.readLong(), null );
        ObjectSerializer<HandshakeResult> handshakeResultObjectSerializer = ( responseObject, result ) ->
        {
            result.writeLong( responseObject.txChecksum() );
            result.writeLong( responseObject.epoch() );
        };
        register( Type.HANDSHAKE, handshakeTarget, handshakeResultObjectSerializer );
    }

    private void registerCopyStore()
    {
        TargetCaller<Master,Void> copyStoreTarget = ( master, context, input, target ) ->
                master.copyStore( context, new ToNetworkStoreWriter( target, new Monitors() ) );
        register( Type.COPY_STORE, copyStoreTarget, VOID_SERIALIZER, false );
    }

    private void registerNewLockSession()
    {
        TargetCaller<Master,Void> newLockSessionTarget =
                ( master, context, input, target ) -> master.newLockSession( context );
        register( Type.NEW_LOCK_SESSION, newLockSessionTarget, VOID_SERIALIZER );
    }

    private void registerCreatePropertyKey()
    {
        TargetCaller<Master,Integer> createPropertyKeyTarget =
                ( master, context, input, target ) -> master.createPropertyKey( context, readString( input ) );
        register( Type.CREATE_PROPERTY_KEY, createPropertyKeyTarget, INTEGER_SERIALIZER );
    }

    private void registerCreateLabel()
    {
        TargetCaller<Master,Integer> createLabelTarget =
                ( master, context, input, target ) -> master.createLabel( context, readString( input ) );
        register( Type.CREATE_LABEL, createLabelTarget, INTEGER_SERIALIZER );
    }
}
