/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.bolt.transport;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Iterator;
import java.util.Map;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.logging.NullBoltMessageLogger;
import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.bolt.transport.pipeline.ChunkDecoder;
import org.neo4j.bolt.transport.pipeline.HouseKeeper;
import org.neo4j.bolt.transport.pipeline.MessageAccumulator;
import org.neo4j.bolt.transport.pipeline.MessageDecoder;
import org.neo4j.bolt.v1.messaging.Neo4jPack;
import org.neo4j.bolt.v1.messaging.Neo4jPackV1;
import org.neo4j.bolt.v2.messaging.Neo4jPackV2;
import org.neo4j.kernel.impl.logging.NullLogService;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

@RunWith( Parameterized.class )
public class DefaultBoltProtocolPipelineInstallerTest
{
    private final EmbeddedChannel channel = new EmbeddedChannel();

    @Parameterized.Parameter( 0 )
    public Neo4jPack pack;

    @Parameterized.Parameter( 1 )
    public String name;

    @Parameterized.Parameters( name = "{1}" )
    public static Object[][] testParameters()
    {
        return new Object[][]{new Object[]{new Neo4jPackV1(), "V1"}, new Object[]{new Neo4jPackV2(), "V2"}};
    }

    @After
    public void cleanup()
    {
        channel.finishAndReleaseAll();
    }

    @Test
    public void shouldReportCorrectVersion()
    {
        // When
        DefaultBoltProtocolPipelineInstaller protocol =
                new DefaultBoltProtocolPipelineInstaller( newBoltChannel( channel ), mock( BoltConnection.class ), pack, TransportThrottleGroup.NO_THROTTLE,
                        NullLogService.getInstance() );
        // Then
        assertEquals( pack.version(), protocol.version() );
    }

    @Test
    public void shouldInstallChannelHandlersInCorrectOrder()
    {
        // Given
        DefaultBoltProtocolPipelineInstaller protocol =
                new DefaultBoltProtocolPipelineInstaller( newBoltChannel( channel ), mock( BoltConnection.class ), pack, TransportThrottleGroup.NO_THROTTLE,
                        NullLogService.getInstance() );
        // When
        protocol.install();

        Iterator<Map.Entry<String,ChannelHandler>> handlers = channel.pipeline().iterator();
        assertThat( handlers.next().getValue(), instanceOf( ChunkDecoder.class ) );
        assertThat( handlers.next().getValue(), instanceOf( MessageAccumulator.class ) );
        assertThat( handlers.next().getValue(), instanceOf( MessageDecoder.class ) );
        assertThat( handlers.next().getValue(), instanceOf( HouseKeeper.class ) );

        assertFalse( handlers.hasNext() );
    }

    private static BoltChannel newBoltChannel( Channel rawChannel )
    {
        return BoltChannel.open( "bolt", rawChannel, NullBoltMessageLogger.getInstance() );
    }
}
