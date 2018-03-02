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
import org.junit.Test;

import java.util.concurrent.Future;

import org.neo4j.logging.NullLog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SimpleNettyChannelTest
{
    private EmbeddedChannel nettyChannel = new EmbeddedChannel();

    @Test
    public void shouldWriteOnNettyChannel()
    {
        // given
        SimpleNettyChannel channel = new SimpleNettyChannel( nettyChannel, NullLog.getInstance() );

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
    public void shouldWriteAndFlushOnNettyChannel()
    {
        // given
        SimpleNettyChannel channel = new SimpleNettyChannel( nettyChannel, NullLog.getInstance() );

        // when
        Object msg = new Object();
        Future<Void> writeComplete = channel.writeAndFlush( msg );

        // then
        assertTrue( writeComplete.isDone() );
        assertEquals( msg, nettyChannel.readOutbound() );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldThrowWhenWritingOnDisposedChannel()
    {
        // given
        SimpleNettyChannel channel = new SimpleNettyChannel( nettyChannel, NullLog.getInstance() );
        channel.dispose();

        // when
        channel.write( new Object() );

        // then expected to throw
    }

    @Test( expected = IllegalStateException.class )
    public void shouldThrowWhenWriteAndFlushingOnDisposedChannel()
    {
        // given
        SimpleNettyChannel channel = new SimpleNettyChannel( nettyChannel, NullLog.getInstance() );
        channel.dispose();

        // when
        channel.writeAndFlush( new Object() );

        // then expected to throw
    }
}
