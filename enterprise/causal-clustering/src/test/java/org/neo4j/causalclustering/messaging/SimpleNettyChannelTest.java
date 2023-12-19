/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
