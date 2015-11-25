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

import java.util.UUID;

import io.netty.buffer.ByteBuf;

import org.neo4j.coreedge.raft.membership.CoreMemberMarshal;
import org.neo4j.coreedge.server.CoreMember;

public class CoreServiceAssignmentSerializer
{
    public static void serialize( CoreServiceAssignment serviceAssignment, ByteBuf buffer )
    {
        buffer.writeByte( serviceAssignment.serviceType().ordinal() );
        CoreMemberMarshal.serialize( serviceAssignment.provider(), buffer );

        UUID assignmentId = serviceAssignment.assignmentId();
        buffer.writeLong( assignmentId.getMostSignificantBits() );
        buffer.writeLong( assignmentId.getLeastSignificantBits() );
    }

    public static CoreServiceAssignment deserialize( ByteBuf buffer )
    {
        CoreServiceRegistry.ServiceType serviceType = CoreServiceRegistry.ServiceType.values()[buffer.readByte()];
        CoreMember coreMember = CoreMemberMarshal.deserialize( buffer );

        long uuidMSB = buffer.readLong();
        long uuidLSB = buffer.readLong();
        UUID assignmentId = new UUID( uuidMSB, uuidLSB );

        return new CoreServiceAssignment( serviceType, coreMember, assignmentId );
    }
}
