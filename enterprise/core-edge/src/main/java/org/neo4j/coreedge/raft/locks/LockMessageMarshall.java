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
package org.neo4j.coreedge.raft.locks;

import io.netty.buffer.ByteBuf;

import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.raft.membership.CoreMemberMarshal;
import org.neo4j.coreedge.raft.replication.StringMarshal;
import org.neo4j.kernel.ha.lock.LockResult;
import org.neo4j.kernel.ha.lock.LockStatus;

import static org.neo4j.kernel.impl.locking.ResourceTypes.fromId;

public class LockMessageMarshall
{
    public static void serialize( ByteBuf buffer, LockMessage.Request msg )
    {
        buffer.writeInt( msg.action.ordinal() );
        CoreMemberMarshal.serialize( msg.lockSession.owner, buffer );
        buffer.writeInt( msg.lockSession.localSessionId );
        if ( msg.type == null )
        {
            buffer.writeInt( -1 );
        }
        else
        {
            buffer.writeInt( msg.type.typeId() );
        }
        buffer.writeInt( msg.resourceIds.length );
        for ( long resourceId : msg.resourceIds )
        {
            buffer.writeLong( resourceId );
        }
    }
    
    public static LockMessage.Request deserializeRequest( ByteBuf buffer )
    {
        int actionOrdinal = buffer.readInt();
        CoreMember owner = CoreMemberMarshal.deserialize( buffer );
        int localSessionId = buffer.readInt();
        int typeId = buffer.readInt();
        int resourceIdCount = buffer.readInt();
        long[] resourceIds = new long[resourceIdCount];
        for ( int i = 0; i < resourceIdCount; i++ )
        {
            resourceIds[i] = buffer.readLong();
        }

        LockMessage.Action action = LockMessage.Action.values()[actionOrdinal];
        LockSession lockSession = new LockSession( owner, localSessionId );
        switch ( action )
        {
            case NEW_SESSION:
                return LockMessage.Request.newLockSession( lockSession );

            case END_SESSION:
                return LockMessage.Request.endLockSession( lockSession );

            case ACQUIRE_EXCLUSIVE:
                return LockMessage.Request.acquireExclusiveLock( lockSession, fromId( typeId ), resourceIds );

            case ACQUIRE_SHARED:
                return LockMessage.Request.acquireSharedLock( lockSession, fromId( typeId ), resourceIds );

            default:
                throw new IllegalArgumentException( "Unknown action: " + action );
        }
    }

    public static void serialize( ByteBuf buffer, LockMessage.Response msg )
    {
        serialize( buffer, msg.request );
        buffer.writeInt( msg.result.getStatus().ordinal() );
        StringMarshal.serialize( buffer, msg.result.getMessage() );
    }

    public static LockMessage.Response deserializeResponse( ByteBuf buffer )
    {
        LockMessage.Request request = deserializeRequest( buffer );

        int lockStatusOrdinal = buffer.readInt();
        String message = StringMarshal.deserialize( buffer );

        LockResult lockResult = new LockResult( LockStatus.values()[lockStatusOrdinal], message );

        return new LockMessage.Response( request, lockResult );
    }
}
