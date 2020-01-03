/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.v1;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.BoltProtocol;
import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.bolt.runtime.BoltConnectionFactory;
import org.neo4j.bolt.runtime.BoltStateMachineFactory;
import org.neo4j.bolt.transport.pipeline.ChunkDecoder;
import org.neo4j.bolt.transport.pipeline.HouseKeeper;
import org.neo4j.bolt.transport.pipeline.MessageAccumulator;
import org.neo4j.bolt.transport.pipeline.MessageDecoder;
import org.neo4j.logging.internal.NullLogService;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BoltProtocolV1Test
{
    private final EmbeddedChannel channel = new EmbeddedChannel();

    @After
    public void cleanup()
    {
        channel.finishAndReleaseAll();
    }

    @Test
    public void shouldInstallChannelHandlersInCorrectOrder() throws Throwable
    {
        // Given
        BoltChannel boltChannel = newBoltChannel( channel );
        BoltConnectionFactory connectionFactory = mock( BoltConnectionFactory.class );
        when( connectionFactory.newConnection( eq( boltChannel ), any() ) ).thenReturn( mock( BoltConnection.class ) );
        BoltProtocol boltProtocol = new BoltProtocolV1( boltChannel, connectionFactory, mock( BoltStateMachineFactory.class ), NullLogService.getInstance() );

        // When
        boltProtocol.install();

        Iterator<Map.Entry<String,ChannelHandler>> handlers = channel.pipeline().iterator();
        assertThat( handlers.next().getValue(), instanceOf( ChunkDecoder.class ) );
        assertThat( handlers.next().getValue(), instanceOf( MessageAccumulator.class ) );
        assertThat( handlers.next().getValue(), instanceOf( MessageDecoder.class ) );
        assertThat( handlers.next().getValue(), instanceOf( HouseKeeper.class ) );

        assertFalse( handlers.hasNext() );
    }

    private static BoltChannel newBoltChannel( Channel rawChannel )
    {
        return new BoltChannel( "bolt-1", "bolt", rawChannel );
    }
}
