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
import io.netty.buffer.Unpooled;
import org.junit.Test;

import org.neo4j.coreedge.server.CoreMember;

import static org.junit.Assert.assertEquals;

import static org.neo4j.coreedge.raft.locks.CoreServiceRegistry.ServiceType.LOCK_MANAGER;
import static org.neo4j.coreedge.server.AdvertisedSocketAddress.address;

public class CoreServiceAssignmentSerializerTest
{
    @Test
    public void shouldSerializeCoreServiceAssignment() throws Exception
    {
        // given
        CoreMember member = new CoreMember( address( "host1:10" ), address( "host1:20" ), address( "host1:30" ) );
        CoreServiceAssignment input = new CoreServiceAssignment( LOCK_MANAGER, member, UUID.randomUUID() );
        ByteBuf buffer = Unpooled.buffer();

        // when
        CoreServiceAssignmentSerializer.serialize( input, buffer );
        CoreServiceAssignment output = CoreServiceAssignmentSerializer.deserialize( buffer );

        // then
        assertEquals( input, output );
    }
}