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
package org.neo4j.coreedge.raft.membership;

import java.nio.ByteBuffer;

import org.junit.Test;

import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreMember;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CoreMemberMarshalTest
{
    @Test
    public void shouldSerializeAndDeserializeUsingByteBuffer() throws Exception
    {
        // given
        CoreMember.CoreMemberMarshal marshal = new CoreMember.CoreMemberMarshal();

        final CoreMember member = new CoreMember( new AdvertisedSocketAddress( "host1:1001" ),
                new AdvertisedSocketAddress( "host1:2001" ) );

        // when
        final ByteBuffer buffer = ByteBuffer.allocate( 1_000 );
        marshal.marshal( member, buffer );
        buffer.flip();
        final CoreMember recovered = marshal.unmarshal( buffer );

        // then
        assertEquals( member, recovered );
    }

    @Test
    public void shouldManageNull() throws Exception
    {
        // given
        CoreMember.CoreMemberMarshal marshal = new CoreMember.CoreMemberMarshal();

        final CoreMember aRealMember = new CoreMember( new AdvertisedSocketAddress( "host1:1001" ),
                new AdvertisedSocketAddress( "host1:2001" ) );

        final CoreMember aNullMember = null;

        final CoreMember anotherRealMember = new CoreMember( new AdvertisedSocketAddress( "host1:1001" ),
                new AdvertisedSocketAddress( "host1:2001" ) );

        // when
        final ByteBuffer buffer = ByteBuffer.allocate( 1_000 );

        marshal.marshal( aRealMember, buffer );
        marshal.marshal( aNullMember, buffer );
        marshal.marshal( anotherRealMember, buffer );

        buffer.flip();
        final CoreMember theRestoredRealMember = marshal.unmarshal( buffer );
        final CoreMember theRestoredNullMember = marshal.unmarshal( buffer );
        final CoreMember theRestoredAnotherRealMember = marshal.unmarshal( buffer );

        // then
        assertEquals( aRealMember, theRestoredRealMember );
        assertEquals( aNullMember, theRestoredNullMember );
        assertEquals( anotherRealMember, theRestoredAnotherRealMember );
    }

    @Test
    public void shouldReturnNullForHalfWrittenInstance() throws Exception
    {
        // given
        // a CoreMember and a ByteBuffer to write it to
        CoreMember.CoreMemberMarshal marshal = new CoreMember.CoreMemberMarshal();
        final CoreMember aRealMember = new CoreMember( new AdvertisedSocketAddress( "host1:1001" ),
                new AdvertisedSocketAddress( "host1:2001" ) );

        final ByteBuffer buffer = ByteBuffer.allocate( 1_000 );

        // and the CoreMember is serialized but for 5 bytes at the end
        marshal.marshal( aRealMember, buffer );
        buffer.limit( buffer.position() - 5 );
        buffer.flip();

        // when
        // that member is read back
        CoreMember member = marshal.unmarshal( buffer );

        // then
        // it should be null and not a half written instance
        assertNull( member );
    }
}
