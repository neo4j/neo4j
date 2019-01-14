/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
