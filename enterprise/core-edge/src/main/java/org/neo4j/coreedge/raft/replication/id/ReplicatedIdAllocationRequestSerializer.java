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
package org.neo4j.coreedge.raft.replication.id;

import io.netty.buffer.ByteBuf;

import org.neo4j.coreedge.raft.membership.CoreMemberMarshal;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.raft.replication.id.ReplicatedIdAllocationRequest;
import org.neo4j.kernel.IdType;

public class ReplicatedIdAllocationRequestSerializer
{
    public static void serialize( ReplicatedIdAllocationRequest idRangeRequest, ByteBuf buffer )
    {
        CoreMemberMarshal.serialize( idRangeRequest.owner(), buffer );
        buffer.writeInt( idRangeRequest.idType().ordinal() );
        buffer.writeLong( idRangeRequest.idRangeStart() );
        buffer.writeInt( idRangeRequest.idRangeLength() );
    }

    public static ReplicatedIdAllocationRequest deserialize( ByteBuf buffer )
    {
        CoreMember owner = CoreMemberMarshal.deserialize( buffer );
        IdType idType = IdType.values()[buffer.readInt()];
        long idRangeStart = buffer.readLong();
        int idRangeLength = buffer.readInt();

        return new ReplicatedIdAllocationRequest( owner, idType, idRangeStart, idRangeLength );
    }
}
