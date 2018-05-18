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
package org.neo4j.causalclustering.messaging.marshalling.v2;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.UUID;

import org.neo4j.causalclustering.core.consensus.NewLeaderBarrier;
import org.neo4j.causalclustering.core.consensus.membership.MemberIdSet;
import org.neo4j.causalclustering.core.consensus.membership.MemberIdSetSerializer;
import org.neo4j.causalclustering.core.replication.DistributedOperation;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.core.replication.session.GlobalSession;
import org.neo4j.causalclustering.core.replication.session.LocalOperationId;
import org.neo4j.causalclustering.core.state.machines.dummy.DummyRequest;
import org.neo4j.causalclustering.core.state.machines.id.ReplicatedIdAllocationRequest;
import org.neo4j.causalclustering.core.state.machines.id.ReplicatedIdAllocationRequestSerializer;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenRequest;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenSerializer;
import org.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequest;
import org.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequestSerializer;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionSerializer;
import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.causalclustering.messaging.marshalling.ChunkedReplicatedContent;
import org.neo4j.causalclustering.messaging.marshalling.SerializableContent;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

import static java.util.Collections.singleton;
import static org.neo4j.causalclustering.messaging.marshalling.Serializer.simple;

public class CoreReplicatedContentSerializer extends SafeChannelMarshal<ReplicatedContent>
{
    private static final byte TX_CONTENT_TYPE = 0;
    private static final byte RAFT_MEMBER_SET_TYPE = 1;
    private static final byte ID_RANGE_REQUEST_TYPE = 2;
    private static final byte TOKEN_REQUEST_TYPE = 4;
    private static final byte NEW_LEADER_BARRIER_TYPE = 5;
    private static final byte LOCK_TOKEN_REQUEST = 6;
    private static final byte DISTRIBUTED_OPERATION = 7;
    private static final byte DUMMY_REQUEST = 8;

    public Collection<SerializableContent> toSerializable( ReplicatedContent content )
    {
        if ( content instanceof ReplicatedTransaction )
        {
            return singleton( new ChunkedReplicatedContent( TX_CONTENT_TYPE, ReplicatedTransactionSerializer.serializer( (ReplicatedTransaction) content ) ) );
        }
        else if ( content instanceof MemberIdSet )
        {
            return singleton( new ChunkedReplicatedContent( RAFT_MEMBER_SET_TYPE,
                    simple( channel -> MemberIdSetSerializer.marshal( (MemberIdSet) content, channel ) ) ) );
        }
        else if ( content instanceof ReplicatedIdAllocationRequest )
        {
            return singleton( new ChunkedReplicatedContent( ID_RANGE_REQUEST_TYPE,
                    simple( channel -> ReplicatedIdAllocationRequestSerializer.marshal( (ReplicatedIdAllocationRequest) content, channel ) ) ) );
        }
        else if ( content instanceof ReplicatedTokenRequest )
        {
            return singleton( new ChunkedReplicatedContent( TOKEN_REQUEST_TYPE,
                    simple( channel -> ReplicatedTokenRequestSerializer.marshal( (ReplicatedTokenRequest) content, channel ) ) ) );
        }
        else if ( content instanceof NewLeaderBarrier )
        {
            return singleton( new ChunkedReplicatedContent( NEW_LEADER_BARRIER_TYPE, simple( channel ->
            {
            } ) ) );
        }
        else if ( content instanceof ReplicatedLockTokenRequest )
        {
            return singleton( new ChunkedReplicatedContent( LOCK_TOKEN_REQUEST,
                    simple( channel -> ReplicatedLockTokenSerializer.marshal( (ReplicatedLockTokenRequest) content, channel ) ) ) );
        }
        else if ( content instanceof DistributedOperation )
        {
            LinkedList<SerializableContent> list = new LinkedList<>( toSerializable( ((DistributedOperation) content).content() ) );
            list.add( 0, new ChunkedReplicatedContent( DISTRIBUTED_OPERATION, simple( channel ->
            {
                channel.putLong( ((DistributedOperation) content).globalSession().sessionId().getMostSignificantBits() );
                channel.putLong( ((DistributedOperation) content).globalSession().sessionId().getLeastSignificantBits() );
                new MemberId.Marshal().marshal( ((DistributedOperation) content).globalSession().owner(), channel );

                channel.putLong( ((DistributedOperation) content).operationId().localSessionId() );
                channel.putLong( ((DistributedOperation) content).operationId().sequenceNumber() );
            } ) ) );
            return list;
        }
        else if ( content instanceof DummyRequest )
        {
            return singleton( new ChunkedReplicatedContent( DUMMY_REQUEST,
                    simple( channel -> DummyRequest.Marshal.INSTANCE.marshal( (DummyRequest) content, channel ) ) ) );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown content type " + content.getClass() );
        }
    }

    public ContentBuilder<ReplicatedContent> read( byte contentType, ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        switch ( contentType )
        {
        case TX_CONTENT_TYPE:
            return new ContentBuilder<>( ReplicatedTransactionSerializer.unmarshal( channel ) );
        case RAFT_MEMBER_SET_TYPE:
            return new ContentBuilder<>( MemberIdSetSerializer.unmarshal( channel ) );
        case ID_RANGE_REQUEST_TYPE:
            return new ContentBuilder<>( ReplicatedIdAllocationRequestSerializer.unmarshal( channel ) );
        case TOKEN_REQUEST_TYPE:
            return new ContentBuilder<>( ReplicatedTokenRequestSerializer.unmarshal( channel ) );
        case NEW_LEADER_BARRIER_TYPE:
            return new ContentBuilder<>( new NewLeaderBarrier() );
        case LOCK_TOKEN_REQUEST:
            return new ContentBuilder<>( ReplicatedLockTokenSerializer.unmarshal( channel ) );
        case DISTRIBUTED_OPERATION:
        {
            long mostSigBits = channel.getLong();
            long leastSigBits = channel.getLong();
            MemberId owner = new MemberId.Marshal().unmarshal( channel );
            GlobalSession globalSession = new GlobalSession( new UUID( mostSigBits, leastSigBits ), owner );

            long localSessionId = channel.getLong();
            long sequenceNumber = channel.getLong();
            LocalOperationId localOperationId = new LocalOperationId( localSessionId, sequenceNumber );

            return new ContentBuilder<>( replicatedContent -> new DistributedOperation( replicatedContent, globalSession, localOperationId ), false );
        }
        case DUMMY_REQUEST:
            return new ContentBuilder<>( DummyRequest.Marshal.INSTANCE.unmarshal( channel ) );
        default:
            throw new IllegalStateException( "Not a recognized content type: " + contentType );
        }
    }

    @Override
    public void marshal( ReplicatedContent coreReplicatedContent, WritableChannel channel ) throws IOException
    {
        for ( SerializableContent serializableContent : toSerializable( coreReplicatedContent ) )
        {
            serializableContent.serialize( channel );
        }
    }

    @Override
    protected ReplicatedContent unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        byte type = channel.get();
        ContentBuilder<ReplicatedContent> contentBuilder = read( type, channel );
        while ( !contentBuilder.isComplete() )
        {
            type = channel.get();
            contentBuilder = contentBuilder.combine( read( type, channel ) );
        }
        return contentBuilder.build();
    }
}
