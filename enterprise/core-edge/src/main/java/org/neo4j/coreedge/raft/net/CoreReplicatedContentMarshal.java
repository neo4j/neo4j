/*
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
package org.neo4j.coreedge.raft.net;

import io.netty.buffer.ByteBuf;

import org.neo4j.coreedge.raft.NewLeaderBarrier;
import org.neo4j.coreedge.raft.membership.CoreMemberSet;
import org.neo4j.coreedge.raft.membership.CoreMemberSetSerializer;
import org.neo4j.coreedge.raft.replication.MarshallingException;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.replication.ReplicatedContentMarshal;
import org.neo4j.coreedge.raft.replication.id.ReplicatedIdAllocationRequest;
import org.neo4j.coreedge.raft.replication.id.ReplicatedIdAllocationRequestSerializer;
import org.neo4j.coreedge.raft.replication.storeid.SeedStoreId;
import org.neo4j.coreedge.raft.replication.storeid.SeedStoreIdSerializer;
import org.neo4j.coreedge.raft.replication.token.ReplicatedTokenRequest;
import org.neo4j.coreedge.raft.replication.token.ReplicatedTokenRequestSerializer;
import org.neo4j.coreedge.raft.replication.tx.ReplicatedTransaction;
import org.neo4j.coreedge.raft.replication.tx.ReplicatedTransactionSerializer;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.server.core.ReplicatedLockRequest;

public class CoreReplicatedContentMarshal implements ReplicatedContentMarshal<ByteBuf>
{
    private static final byte TX_CONTENT_TYPE = 0;
    private static final byte RAFT_MEMBER_SET_TYPE = 1;
    private static final byte ID_RANGE_REQUEST_TYPE = 2;
    private static final byte SEED_STORE_ID_TYPE = 3;
    private static final byte TOKEN_REQUEST_TYPE = 4;
    private static final byte NEW_LEADER_BARRIER_TYPE = 5;
    private static final byte LOCK_REQUEST = 6;

    @Override
    public void serialize( ReplicatedContent content, ByteBuf buffer ) throws MarshallingException
    {
        if ( content instanceof ReplicatedTransaction )
        {
            buffer.writeByte( TX_CONTENT_TYPE );
            ReplicatedTransactionSerializer.serialize( (ReplicatedTransaction) content, buffer );
        }
        else if ( content instanceof CoreMemberSet )
        {
            buffer.writeByte( RAFT_MEMBER_SET_TYPE );
            CoreMemberSetSerializer.serialize( (CoreMemberSet) content, buffer );
        }
        else if ( content instanceof ReplicatedIdAllocationRequest )
        {
            buffer.writeByte( ID_RANGE_REQUEST_TYPE );
            ReplicatedIdAllocationRequestSerializer.serialize( (ReplicatedIdAllocationRequest) content, buffer );
        }
        else if ( content instanceof SeedStoreId )
        {
            buffer.writeByte( SEED_STORE_ID_TYPE );
            SeedStoreIdSerializer.serialize( (SeedStoreId) content, buffer );
        }
        else if ( content instanceof ReplicatedTokenRequest )
        {
            buffer.writeByte( TOKEN_REQUEST_TYPE );
            ReplicatedTokenRequestSerializer.serialize( (ReplicatedTokenRequest) content, buffer );
        }
        else if ( content instanceof NewLeaderBarrier )
        {
            buffer.writeByte( NEW_LEADER_BARRIER_TYPE );
        }
        else if( content instanceof ReplicatedLockRequest )
        {
            buffer.writeByte( LOCK_REQUEST );
            ReplicatedLockRequestSerializer.serialize( (ReplicatedLockRequest<CoreMember>) content, buffer );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown content type " + content.getClass() );
        }
    }

    @Override
    public ReplicatedContent deserialize( ByteBuf buffer ) throws MarshallingException
    {
        if ( buffer.readableBytes() < 1 )
        {
            throw new MarshallingException( "Cannot read content type" );
        }

        byte type = buffer.readByte();
        final ReplicatedContent content;
        switch ( type )
        {
            case TX_CONTENT_TYPE:
                content = ReplicatedTransactionSerializer.deserialize( buffer );
                break;
            case RAFT_MEMBER_SET_TYPE:
                content = CoreMemberSetSerializer.deserialize( buffer );
                break;
            case ID_RANGE_REQUEST_TYPE:
                content = ReplicatedIdAllocationRequestSerializer.deserialize( buffer );
                break;
            case SEED_STORE_ID_TYPE:
                content = SeedStoreIdSerializer.deserialize( buffer );
                break;
            case TOKEN_REQUEST_TYPE:
                content = ReplicatedTokenRequestSerializer.deserialize( buffer );
                break;
            case NEW_LEADER_BARRIER_TYPE:
                content = new NewLeaderBarrier();
                break;
            case LOCK_REQUEST:
                content = ReplicatedLockRequestSerializer.deserialize( buffer );
                break;
            default:
                throw new MarshallingException( String.format( "Unknown content type 0x%x", type ) );
        }

        return content;
    }
}
