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
package org.neo4j.coreedge.server;

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

public class AdvertisedSocketAddressMarshallingTest
{
    @Test
    public void shouldMarshalAndUnmarshalFromByteBuffer()
    {
        // given
        ByteBuffer buffer = ByteBuffer.allocate( 2_000 );
        AdvertisedSocketAddress sent = new AdvertisedSocketAddress( "test-hostname:1234" );
        AdvertisedSocketAddress.AdvertisedSocketAddressByteBufferMarshal marshal = new AdvertisedSocketAddress.AdvertisedSocketAddressByteBufferMarshal();

        // when
        marshal.marshal( sent, buffer );
        buffer.flip();
        AdvertisedSocketAddress received = marshal.unmarshal( buffer );

        // then
        assertNotSame( sent, received );
        assertEquals( sent, received );
    }

    @Test
    public void shouldMarshalAndUnmarshalFromByteBuf()
    {
        // given
        ByteBuf buffer = Unpooled.buffer();
        AdvertisedSocketAddress sent = new AdvertisedSocketAddress( "test-hostname:1234" );
        AdvertisedSocketAddress.AdvertisedSocketAddressByteBufferMarshal marshal = new AdvertisedSocketAddress.AdvertisedSocketAddressByteBufferMarshal();

        // when
        marshal.marshal( sent, buffer );
        AdvertisedSocketAddress received = marshal.unmarshal( buffer );

        // then
        assertNotSame( sent, received );
        assertEquals( sent, received );
    }

    @Test
    public void shouldReturnNullOnHalfWrittenEntryInByteBuffer() throws Exception
    {
        // given
        ByteBuffer buffer = ByteBuffer.allocate( 2_000 );
        AdvertisedSocketAddress sent = new AdvertisedSocketAddress( "test-hostname:1234" );
        AdvertisedSocketAddress.AdvertisedSocketAddressByteBufferMarshal marshal = new AdvertisedSocketAddress.AdvertisedSocketAddressByteBufferMarshal();

        // when
        marshal.marshal( sent, buffer );
        buffer.limit( buffer.position() - 4 );
        buffer.flip();
        AdvertisedSocketAddress received = marshal.unmarshal( buffer );

        // then
        assertNull( received );
    }

    @Test
    public void shouldReturnNullOnHalfWrittenEntryInByteBuf() throws Exception
    {
        // given
        ByteBuf buffer = Unpooled.buffer();
        AdvertisedSocketAddress sent = new AdvertisedSocketAddress( "test-hostname:1234" );
        AdvertisedSocketAddress.AdvertisedSocketAddressByteBufferMarshal marshal = new AdvertisedSocketAddress.AdvertisedSocketAddressByteBufferMarshal();

        // when
        marshal.marshal( sent, buffer );
        buffer = buffer.slice( 0, buffer.writerIndex() - 4 );
        AdvertisedSocketAddress received = marshal.unmarshal( buffer );

        // then
        assertNull( received );
    }
}
