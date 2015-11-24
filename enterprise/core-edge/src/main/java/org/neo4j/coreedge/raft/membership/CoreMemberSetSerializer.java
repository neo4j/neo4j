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
package org.neo4j.coreedge.raft.membership;

import java.util.HashSet;
import java.util.Set;

import io.netty.buffer.ByteBuf;

import org.neo4j.coreedge.server.CoreMember;

/**
 * Format:
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │contentLength                                                  8 bytes   │
 * │contentType                                                    1 bytes   │
 * │content      ┌──────────────────────────────────────────────────────────┐│
 * │             │ memberCount                                     4 bytes  ││
 * │             │ member 0   ┌────────────────────────────────────────────┐││
 * │             │            │core address ┌─────────────────────────────┐│││
 * │             │            │             │hostnameLength        4 bytes││││
 * │             │            │             │hostnameBytes        variable││││
 * │             │            │             │port                  4 bytes││││
 * │             │            │             └─────────────────────────────┘│││
 * │             │            │raft address ┌─────────────────────────────┐│││
 * │             │            │             │hostnameLength        4 bytes││││
 * │             │            │             │hostnameBytes        variable││││
 * │             │            │             │port                  4 bytes││││
 * │             │            │             └─────────────────────────────┘│││
 * │             │            └────────────────────────────────────────────┘││
 * │             │ ...                                                      ││
 * │             │ member n   ┌────────────────────────────────────────────┐││
 * │             │            │core address ┌─────────────────────────────┐│││
 * │             │            │             │hostnameLength        4 bytes││││
 * │             │            │             │hostnameBytes        variable││││
 * │             │            │             │port                  4 bytes││││
 * │             │            │             └─────────────────────────────┘│││
 * │             │            │raft address ┌─────────────────────────────┐│││
 * │             │            │             │hostnameLength        4 bytes││││
 * │             │            │             │hostnameBytes        variable││││
 * │             │            │             │port                  4 bytes││││
 * │             │            │             └─────────────────────────────┘│││
 * │             │            └────────────────────────────────────────────┘││
 * │             └──────────────────────────────────────────────────────────┘│
 * └─────────────────────────────────────────────────────────────────────────┘
 */
public class CoreMemberSetSerializer
{
    public static void serialize( CoreMemberSet memberSet, ByteBuf buffer )
    {
        Set<CoreMember> members = memberSet.getMembers();
        buffer.writeInt( members.size() );

        for ( CoreMember member : members )
        {
            CoreMemberMarshal.serialize( member, buffer );
        }
    }

    public static CoreMemberSet deserialize( ByteBuf buffer )
    {
        HashSet<CoreMember> members = new HashSet<>();
        int memberCount = buffer.readInt();

        for ( int i = 0; i < memberCount; i++ )
        {
            members.add( CoreMemberMarshal.deserialize( buffer ) );
        }

        return new CoreMemberSet( members );
    }
}
