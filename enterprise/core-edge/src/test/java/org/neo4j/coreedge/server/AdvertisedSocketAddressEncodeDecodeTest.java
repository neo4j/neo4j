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

public class AdvertisedSocketAddressEncodeDecodeTest
{
    @Test
    public void shouldEncodeAndDecodeHostnamePort()
    {
        // given
        ByteBuffer buffer = ByteBuffer.allocate( 2_000_000 );
        AdvertisedSocketAddress sent = new AdvertisedSocketAddress( "test-hostname:1234" );

        // when
//        sent.encoder().encode( buffer );

        // then
//        AdvertisedSocketAddress received = new AdvertisedSocketAddressDecoder().decode(  );
//        assertNotSame( sent, received );
//        assertEquals( sent, received );
    }
}