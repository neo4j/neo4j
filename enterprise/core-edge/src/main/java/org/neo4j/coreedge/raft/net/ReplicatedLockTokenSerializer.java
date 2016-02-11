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

import java.io.IOException;

import io.netty.buffer.ByteBuf;

import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.server.core.locks.ReplicatedLockTokenRequest;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class ReplicatedLockTokenSerializer
{
    public static void marshal( ReplicatedLockTokenRequest<CoreMember> tokenRequest, WritableChannel channel)
            throws IOException
    {
        channel.putInt( tokenRequest.id() );
        new CoreMember.CoreMemberMarshal().marshal( tokenRequest.owner(), channel );
    }

    public static ReplicatedLockTokenRequest<CoreMember> unmarshal( ReadableChannel channel ) throws IOException
    {
        int candidateId = channel.getInt();
        CoreMember owner = new CoreMember.CoreMemberMarshal().unmarshal( channel );

        return new ReplicatedLockTokenRequest<>( owner, candidateId );
    }

    public static void marshal( ReplicatedLockTokenRequest<CoreMember> tokenRequest, ByteBuf buffer )
    {
        buffer.writeInt( tokenRequest.id() );
        new CoreMember.CoreMemberMarshal().marshal( tokenRequest.owner(), buffer );
    }

    public static ReplicatedLockTokenRequest<CoreMember> unmarshal( ByteBuf buffer )
    {
        int candidateId = buffer.readInt();
        CoreMember owner = new CoreMember.CoreMemberMarshal().unmarshal( buffer );

        return new ReplicatedLockTokenRequest<>( owner, candidateId );
    }
}
