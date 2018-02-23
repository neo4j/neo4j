/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.logging.NullLog.getInstance;

class SimpleNettyChannelTest
{
    private EmbeddedChannel nettyChannel = new EmbeddedChannel();

    @Test
    void shouldWriteOnNettyChannel()
    {
        // given
        SimpleNettyChannel channel = new SimpleNettyChannel( nettyChannel, getInstance() );

        // when
        Object msg = new Object();
        Future<Void> writeComplete = channel.write( msg );

        // then
        assertNull( nettyChannel.readOutbound() );
        assertFalse( writeComplete.isDone() );

        // when
        nettyChannel.flush();

        // then
        assertTrue( writeComplete.isDone() );
        assertEquals( msg, nettyChannel.readOutbound() );
    }

    @Test
    void shouldWriteAndFlushOnNettyChannel()
    {
        // given
        SimpleNettyChannel channel = new SimpleNettyChannel( nettyChannel, getInstance() );

        // when
        Object msg = new Object();
        Future<Void> writeComplete = channel.writeAndFlush( msg );

        // then
        assertTrue( writeComplete.isDone() );
        assertEquals( msg, nettyChannel.readOutbound() );
    }

    @Test
    void shouldThrowWhenWritingOnDisposedChannel()
{
    assertThrows( IllegalStateException.class, () ->
    {
        // given
        SimpleNettyChannel channel = new SimpleNettyChannel( nettyChannel, getInstance() );
        channel.dispose();

        // when
        channel.write( new Object() );

        // then expected to throw

    } );
}

    @Test
    void shouldThrowWhenWriteAndFlushingOnDisposedChannel()
    {
        assertThrows( IllegalStateException.class, () -> {
            // given
            SimpleNettyChannel channel = new SimpleNettyChannel( nettyChannel, getInstance() );
            channel.dispose();

            // when
            channel.writeAndFlush( new Object() );

            // then expected to throw

        } );
    }
}
