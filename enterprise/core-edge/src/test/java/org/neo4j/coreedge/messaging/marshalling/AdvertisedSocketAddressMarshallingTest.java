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
package org.neo4j.coreedge.messaging.marshalling;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import org.neo4j.coreedge.messaging.address.AdvertisedSocketAddress;
import org.neo4j.coreedge.messaging.NetworkFlushableChannelNetty4;
import org.neo4j.coreedge.messaging.NetworkReadableClosableChannelNetty4;
import org.neo4j.coreedge.messaging.EndOfStreamException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;

public class AdvertisedSocketAddressMarshallingTest
{
    @Test
    public void shouldMarshalAndUnmarshalFromChannel() throws Exception
    {
        // given
        ByteBuf buffer = Unpooled.buffer( 2_000 );
        AdvertisedSocketAddress sent = new AdvertisedSocketAddress( "test-hostname:1234" );
        AdvertisedSocketAddress.AdvertisedSocketAddressChannelMarshal marshal =
                new AdvertisedSocketAddress.AdvertisedSocketAddressChannelMarshal();

        // when
        marshal.marshal( sent, new NetworkFlushableChannelNetty4( buffer ) );
        AdvertisedSocketAddress received = marshal.unmarshal( new NetworkReadableClosableChannelNetty4( buffer ) );

        // then
        assertNotSame( sent, received );
        assertEquals( sent, received );
    }

    @Test
    public void shouldThrowExceptionOnHalfWrittenEntryInByteBuffer() throws Exception
    {
        // given
        ByteBuf buffer = Unpooled.buffer( 2_000 );
        AdvertisedSocketAddress sent = new AdvertisedSocketAddress( "test-hostname:1234" );
        AdvertisedSocketAddress.AdvertisedSocketAddressChannelMarshal marshal =
                new AdvertisedSocketAddress.AdvertisedSocketAddressChannelMarshal();

        // when
        marshal.marshal( sent, new NetworkFlushableChannelNetty4( buffer ) );
        ByteBuf bufferWithMissingBytes = buffer.copy( 0, buffer.writerIndex() - 5 );
        try
        {
            marshal.unmarshal( new NetworkReadableClosableChannelNetty4( bufferWithMissingBytes ) );
            fail();
        }
        catch ( EndOfStreamException e )
        {
            // expected.
        }
    }
}
