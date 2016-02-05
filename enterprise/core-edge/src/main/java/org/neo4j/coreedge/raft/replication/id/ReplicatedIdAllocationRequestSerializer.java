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
package org.neo4j.coreedge.raft.replication.id;

import java.io.IOException;

import io.netty.buffer.ByteBuf;

import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class ReplicatedIdAllocationRequestSerializer
{
    public static void marshal( ReplicatedIdAllocationRequest idRangeRequest, WritableChannel channel )
            throws IOException
    {
        new CoreMember.CoreMemberMarshal().marshal( idRangeRequest.owner(), channel );
        channel.putInt( idRangeRequest.idType().ordinal() );
        channel.putLong( idRangeRequest.idRangeStart() );
        channel.putInt( idRangeRequest.idRangeLength() );
    }

    public static ReplicatedIdAllocationRequest unmarshal( ReadableChannel channel ) throws IOException
    {
        CoreMember owner = new CoreMember.CoreMemberMarshal().unmarshal( channel );
        IdType idType = IdType.values()[ channel.getInt() ];
        long idRangeStart = channel.getLong();
        int idRangeLength = channel.getInt();

        return new ReplicatedIdAllocationRequest( owner, idType, idRangeStart, idRangeLength );
    }

    public static void marshal( ReplicatedIdAllocationRequest idRangeRequest, ByteBuf buffer )
    {
        new CoreMember.CoreMemberMarshal().marshal( idRangeRequest.owner(), buffer );
        buffer.writeInt( idRangeRequest.idType().ordinal() );
        buffer.writeLong( idRangeRequest.idRangeStart() );
        buffer.writeInt( idRangeRequest.idRangeLength() );
    }

    public static ReplicatedIdAllocationRequest unmarshal( ByteBuf buffer )
    {
        CoreMember owner = new CoreMember.CoreMemberMarshal().unmarshal( buffer );
        IdType idType = IdType.values()[ buffer.readInt() ];
        long idRangeStart = buffer.readLong();
        int idRangeLength = buffer.readInt();

        return new ReplicatedIdAllocationRequest( owner, idType, idRangeStart, idRangeLength );
    }
}
