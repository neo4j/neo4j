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
package org.neo4j.coreedge.raft.net;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import org.neo4j.coreedge.raft.NewLeaderBarrier;
import org.neo4j.coreedge.raft.membership.CoreMemberSet;
import org.neo4j.coreedge.raft.membership.CoreMemberSetSerializer;
import org.neo4j.coreedge.raft.replication.DistributedOperation;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.replication.id.ReplicatedIdAllocationRequest;
import org.neo4j.coreedge.raft.replication.id.ReplicatedIdAllocationRequestSerializer;
import org.neo4j.coreedge.raft.replication.token.ReplicatedTokenRequest;
import org.neo4j.coreedge.raft.replication.token.ReplicatedTokenRequestSerializer;
import org.neo4j.coreedge.raft.replication.tx.ReplicatedTransaction;
import org.neo4j.coreedge.raft.replication.tx.ReplicatedTransactionSerializer;
import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.coreedge.server.ByteBufMarshal;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.server.core.locks.ReplicatedLockTokenRequest;
import org.neo4j.storageengine.api.ReadPastEndException;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class CoreReplicatedContentMarshal implements ChannelMarshal<ReplicatedContent>, ByteBufMarshal<ReplicatedContent>
{
    private static final byte TX_CONTENT_TYPE = 0;
    private static final byte RAFT_MEMBER_SET_TYPE = 1;
    private static final byte ID_RANGE_REQUEST_TYPE = 2;
    private static final byte TOKEN_REQUEST_TYPE = 4;
    private static final byte NEW_LEADER_BARRIER_TYPE = 5;
    private static final byte LOCK_TOKEN_REQUEST = 6;
    private static final byte DISTRIBUTED_OPERATION = 7;

    @Override
    public void marshal( ReplicatedContent content, WritableChannel channel ) throws IOException
    {
        if ( content instanceof ReplicatedTransaction )
        {
            channel.put( TX_CONTENT_TYPE );
            ReplicatedTransactionSerializer.marshal( (ReplicatedTransaction) content, channel );
        }
        else if ( content instanceof CoreMemberSet )
        {
            channel.put( RAFT_MEMBER_SET_TYPE );
            CoreMemberSetSerializer.marshal( (CoreMemberSet) content, channel );
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
        else if( content instanceof ReplicatedLockTokenRequest )
        {
            channel.put( LOCK_TOKEN_REQUEST );
            ReplicatedLockTokenSerializer.marshal( (ReplicatedLockTokenRequest<CoreMember>) content, channel );
        }
        else if( content instanceof DistributedOperation )
        {
            channel.put( DISTRIBUTED_OPERATION );
            ((DistributedOperation) content).serialize( channel );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown content type " + content.getClass() );
        }
    }

    @Override
    public ReplicatedContent unmarshal( ReadableChannel channel ) throws IOException
    {
        try
        {
            byte type = channel.get();
            final ReplicatedContent content;
            switch ( type )
            {
                case TX_CONTENT_TYPE:
                    content = ReplicatedTransactionSerializer.unmarshal( channel );
                    break;
                case RAFT_MEMBER_SET_TYPE:
                    content = CoreMemberSetSerializer.unmarshal( channel );
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
                default:
                    throw new IllegalArgumentException( String.format( "Unknown content type 0x%x", type ) );
            }
            return content;
        }
        catch( ReadPastEndException notEnoughBytes )
        {
            return null;
        }
    }

    @Override
    public void marshal( ReplicatedContent content, ByteBuf buffer )
    {
        if ( content instanceof ReplicatedTransaction )
        {
            buffer.writeByte( TX_CONTENT_TYPE );
            ReplicatedTransactionSerializer.marshal( (ReplicatedTransaction) content, buffer );
        }
        else if ( content instanceof CoreMemberSet )
        {
            buffer.writeByte( RAFT_MEMBER_SET_TYPE );
            CoreMemberSetSerializer.marshal( (CoreMemberSet) content, buffer );
        }
        else if ( content instanceof ReplicatedIdAllocationRequest )
        {
            buffer.writeByte( ID_RANGE_REQUEST_TYPE );
            ReplicatedIdAllocationRequestSerializer.marshal( (ReplicatedIdAllocationRequest) content, buffer );
        }
        else if ( content instanceof ReplicatedTokenRequest )
        {
            buffer.writeByte( TOKEN_REQUEST_TYPE );
            ReplicatedTokenRequestSerializer.marshal( (ReplicatedTokenRequest) content, buffer );
        }
        else if ( content instanceof NewLeaderBarrier )
        {
            buffer.writeByte( NEW_LEADER_BARRIER_TYPE );
        }
        else if( content instanceof ReplicatedLockTokenRequest )
        {
            buffer.writeByte( LOCK_TOKEN_REQUEST );
            ReplicatedLockTokenSerializer.marshal( (ReplicatedLockTokenRequest<CoreMember>) content, buffer );
        }
        else if( content instanceof DistributedOperation )
        {
            buffer.writeByte( DISTRIBUTED_OPERATION );
            ((DistributedOperation)content).serialize( buffer );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown content type " + content.getClass() );
        }
    }

    @Override
    public ReplicatedContent unmarshal( ByteBuf buffer )
    {
        try
        {
            byte type = buffer.readByte();
            final ReplicatedContent content;
            switch ( type )
            {
                case TX_CONTENT_TYPE:
                    content = ReplicatedTransactionSerializer.unmarshal( buffer );
                    break;
                case RAFT_MEMBER_SET_TYPE:
                    content = CoreMemberSetSerializer.unmarshal( buffer );
                    break;
                case ID_RANGE_REQUEST_TYPE:
                    content = ReplicatedIdAllocationRequestSerializer.unmarshal( buffer );
                    break;
                case TOKEN_REQUEST_TYPE:
                    content = ReplicatedTokenRequestSerializer.unmarshal( buffer );
                    break;
                case NEW_LEADER_BARRIER_TYPE:
                    content = new NewLeaderBarrier();
                    break;
                case LOCK_TOKEN_REQUEST:
                    content = ReplicatedLockTokenSerializer.unmarshal( buffer );
                    break;
                case DISTRIBUTED_OPERATION:
                    content = DistributedOperation.deserialize( buffer );
                    break;
                default:
                    throw new IllegalArgumentException( String.format( "Unknown content type 0x%x", type ) );
            }
            return content;
        }
        catch( IndexOutOfBoundsException notEnoughBytes )
        {
            return null;
        }
    }
}
