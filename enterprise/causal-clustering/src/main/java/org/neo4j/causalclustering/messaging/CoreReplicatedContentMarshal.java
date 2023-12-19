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
package org.neo4j.causalclustering.messaging;

import java.io.IOException;

import org.neo4j.causalclustering.core.consensus.NewLeaderBarrier;
import org.neo4j.causalclustering.core.consensus.membership.MemberIdSet;
import org.neo4j.causalclustering.core.consensus.membership.MemberIdSetSerializer;
import org.neo4j.causalclustering.core.replication.DistributedOperation;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.core.state.machines.id.ReplicatedIdAllocationRequest;
import org.neo4j.causalclustering.core.state.machines.id.ReplicatedIdAllocationRequestSerializer;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenRequest;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenSerializer;
import org.neo4j.causalclustering.core.state.machines.dummy.DummyRequest;
import org.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequest;
import org.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequestSerializer;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionSerializer;
import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class CoreReplicatedContentMarshal extends SafeChannelMarshal<ReplicatedContent>
{
    private static final byte TX_CONTENT_TYPE = 0;
    private static final byte RAFT_MEMBER_SET_TYPE = 1;
    private static final byte ID_RANGE_REQUEST_TYPE = 2;
    private static final byte TOKEN_REQUEST_TYPE = 4;
    private static final byte NEW_LEADER_BARRIER_TYPE = 5;
    private static final byte LOCK_TOKEN_REQUEST = 6;
    private static final byte DISTRIBUTED_OPERATION = 7;
    private static final byte DUMMY_REQUEST = 8;

    @Override
    public void marshal( ReplicatedContent content, WritableChannel channel ) throws IOException
    {
        if ( content instanceof ReplicatedTransaction )
        {
            channel.put( TX_CONTENT_TYPE );
            ReplicatedTransactionSerializer.marshal( (ReplicatedTransaction) content, channel );
        }
        else if ( content instanceof MemberIdSet )
        {
            channel.put( RAFT_MEMBER_SET_TYPE );
            MemberIdSetSerializer.marshal( (MemberIdSet) content, channel );
        }
        else if ( content instanceof ReplicatedIdAllocationRequest )
        {
            channel.put( ID_RANGE_REQUEST_TYPE );
            ReplicatedIdAllocationRequestSerializer.marshal( (ReplicatedIdAllocationRequest) content, channel );
        }
        else if ( content instanceof ReplicatedTokenRequest )
        {
            channel.put( TOKEN_REQUEST_TYPE );
            ReplicatedTokenRequestSerializer.marshal( (ReplicatedTokenRequest) content, channel );
        }
        else if ( content instanceof NewLeaderBarrier )
        {
            channel.put( NEW_LEADER_BARRIER_TYPE );
        }
        else if ( content instanceof ReplicatedLockTokenRequest )
        {
            channel.put( LOCK_TOKEN_REQUEST );
            ReplicatedLockTokenSerializer.marshal( (ReplicatedLockTokenRequest) content, channel );
        }
        else if ( content instanceof DistributedOperation )
        {
            channel.put( DISTRIBUTED_OPERATION );
            ((DistributedOperation) content).serialize( channel );
        }
        else if ( content instanceof DummyRequest )
        {
            channel.put( DUMMY_REQUEST );
            DummyRequest.Marshal.INSTANCE.marshal( (DummyRequest) content, channel );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown content type " + content.getClass() );
        }
    }

    @Override
    public ReplicatedContent unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        byte type = channel.get();
        final ReplicatedContent content;
        switch ( type )
        {
            case TX_CONTENT_TYPE:
                content = ReplicatedTransactionSerializer.unmarshal( channel );
                break;
            case RAFT_MEMBER_SET_TYPE:
                content = MemberIdSetSerializer.unmarshal( channel );
                break;
            case ID_RANGE_REQUEST_TYPE:
                content = ReplicatedIdAllocationRequestSerializer.unmarshal( channel );
                break;
            case TOKEN_REQUEST_TYPE:
                content = ReplicatedTokenRequestSerializer.unmarshal( channel );
                break;
            case NEW_LEADER_BARRIER_TYPE:
                content = new NewLeaderBarrier();
                break;
            case LOCK_TOKEN_REQUEST:
                content = ReplicatedLockTokenSerializer.unmarshal( channel );
                break;
            case DISTRIBUTED_OPERATION:
                content = DistributedOperation.deserialize( channel );
                break;
            case DUMMY_REQUEST:
                content = DummyRequest.Marshal.INSTANCE.unmarshal( channel );
                break;
            default:
                throw new IllegalArgumentException( String.format( "Unknown content type 0x%x", type ) );
        }
        return content;
    }
}
