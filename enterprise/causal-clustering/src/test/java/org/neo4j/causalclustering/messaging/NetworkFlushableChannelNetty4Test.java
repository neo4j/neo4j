/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.messaging;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import static org.junit.Assert.fail;

public class NetworkFlushableChannelNetty4Test
{
    @Test
    public void shouldRespectSizeLimit() throws Exception
    {
        // Given
        int sizeLimit = 100;
        NetworkFlushableChannelNetty4 channel = new NetworkFlushableChannelNetty4( Unpooled.buffer(), sizeLimit );

        // when
        for ( int i = 0; i < sizeLimit; i++ )
        {
            channel.put( (byte) 1 );
        }

        try
        {
            channel.put( (byte) 1 );
            fail("Should not allow more bytes than what the limit dictates");
        }
        catch ( MessageTooBigException e )
        {
            // then
        }
    }

    @Test
    public void sizeLimitShouldWorkWithArrays() throws Exception
    {
        // Given
        int sizeLimit = 100;
        NetworkFlushableChannelNetty4 channel = new NetworkFlushableChannelNetty4( Unpooled.buffer(), sizeLimit );

        // When
        int padding = 10;
        for ( int i = 0; i < sizeLimit - padding; i++ )
        {
            channel.put( (byte) 0 );
        }

        try
        {
            channel.put( new byte[padding * 2], padding * 2 );
            fail("Should not allow more bytes than what the limit dictates");
        }
        catch ( MessageTooBigException e )
        {
            // then
        }
    }

    @Test
    public void shouldNotCountBytesAlreadyInBuffer() throws Exception
    {
        // Given
        int sizeLimit = 100;
        ByteBuf buffer = Unpooled.buffer();

        int padding = Long.BYTES;
        buffer.writeLong( 0 );

        NetworkFlushableChannelNetty4 channel = new NetworkFlushableChannelNetty4( buffer, sizeLimit );

        // When
        for ( int i = 0; i < sizeLimit - padding; i++ )
        {
            channel.put( (byte) 0 );
        }
        // then it should be ok
        // again, when
        for ( int i = 0; i < padding; i++ )
        {
            channel.put( (byte) 0 );
        }
        // then again, it should work
        // finally, when we pass the limit
        try
        {
            channel.put( (byte) 0 );
            fail("Should not allow more bytes than what the limit dictates");
        }
        catch ( MessageTooBigException e )
        {
            // then
        }
    }
}
