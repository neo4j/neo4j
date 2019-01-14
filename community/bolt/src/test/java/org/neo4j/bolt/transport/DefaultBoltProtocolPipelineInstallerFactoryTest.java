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

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.logging.NullBoltMessageLogger;
import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.bolt.runtime.BoltConnectionFactory;
import org.neo4j.bolt.v1.messaging.Neo4jPackV1;
import org.neo4j.bolt.v2.messaging.Neo4jPackV2;
import org.neo4j.kernel.impl.logging.NullLogService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultBoltProtocolPipelineInstallerFactoryTest
{
    private static final String CONNECTOR = "default";

    @Test
    public void shouldCreateV1Handler()
    {
        testHandlerCreation( Neo4jPackV1.VERSION );
    }

    @Test
    public void shouldCreateV2Handler()
    {
        testHandlerCreation( Neo4jPackV2.VERSION );
    }

    @Test
    public void shouldCreateNothingForUnknownProtocolVersion()
    {
        int protocolVersion = 42;
        BoltChannel channel = mock( BoltChannel.class );
        BoltProtocolPipelineInstallerFactory factory = new DefaultBoltProtocolPipelineInstallerFactory( mock( BoltConnectionFactory.class ),
                TransportThrottleGroup.NO_THROTTLE, NullLogService.getInstance() );

        BoltProtocolPipelineInstaller handler = factory.create( protocolVersion, channel );

        // handler is not created
        assertNull( handler );
    }

    private static void testHandlerCreation( long protocolVersion )
    {
        EmbeddedChannel channel = new EmbeddedChannel();

        BoltChannel boltChannel = BoltChannel.open( CONNECTOR, channel, NullBoltMessageLogger.getInstance() );
        BoltConnectionFactory connectionFactory = mock( BoltConnectionFactory.class );

        BoltConnection connection = mock( BoltConnection.class );
        when( connectionFactory.newConnection( boltChannel ) ).thenReturn( connection );

        BoltProtocolPipelineInstallerFactory factory = new DefaultBoltProtocolPipelineInstallerFactory( connectionFactory,
                TransportThrottleGroup.NO_THROTTLE, NullLogService.getInstance() );

        BoltProtocolPipelineInstaller handler = factory.create( protocolVersion, boltChannel );

        handler.install();

        // handler with correct version is created
        assertEquals( protocolVersion, handler.version() );
        // it uses the expected worker
        verify( connectionFactory ).newConnection( boltChannel );

        // and halts this same worker when closed
        verify( connection, never() ).stop();
        channel.close();
        verify( connection ).stop();

        channel.finishAndReleaseAll();
    }
}
