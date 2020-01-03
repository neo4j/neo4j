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
package org.neo4j.bolt.transport;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.BoltProtocol;
import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.bolt.runtime.BoltConnectionFactory;
import org.neo4j.bolt.runtime.BoltStateMachine;
import org.neo4j.bolt.runtime.BoltStateMachineFactory;
import org.neo4j.bolt.testing.BoltTestUtil;
import org.neo4j.bolt.v1.BoltProtocolV1;
import org.neo4j.bolt.v2.BoltProtocolV2;
import org.neo4j.bolt.v3.BoltProtocolV3;
import org.neo4j.logging.internal.NullLogService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DefaultBoltProtocolFactoryTest
{
    @Test
    void shouldCreateNothingForUnknownProtocolVersion()
    {
        int protocolVersion = 42;
        BoltChannel channel = BoltTestUtil.newTestBoltChannel();
        BoltProtocolFactory factory =
                new DefaultBoltProtocolFactory( mock( BoltConnectionFactory.class ), mock( BoltStateMachineFactory.class ),
                        NullLogService.getInstance() );

        BoltProtocol protocol = factory.create( protocolVersion, channel );

        // handler is not created
        assertNull( protocol );
    }

    @ParameterizedTest( name = "V{0}" )
    @ValueSource( longs = {BoltProtocolV1.VERSION, BoltProtocolV2.VERSION, BoltProtocolV3.VERSION} )
    void shouldCreateBoltProtocol( long protocolVersion ) throws Throwable
    {
        EmbeddedChannel channel = new EmbeddedChannel();
        BoltChannel boltChannel = new BoltChannel( "bolt-1", "bolt", channel );

        BoltStateMachineFactory stateMachineFactory = mock( BoltStateMachineFactory.class );
        BoltStateMachine stateMachine = mock( BoltStateMachine.class );
        when( stateMachineFactory.newStateMachine( protocolVersion, boltChannel ) ).thenReturn( stateMachine );

        BoltConnectionFactory connectionFactory = mock( BoltConnectionFactory.class );
        BoltConnection connection = mock( BoltConnection.class );
        when( connectionFactory.newConnection( boltChannel, stateMachine ) ).thenReturn( connection );

        BoltProtocolFactory factory =
                new DefaultBoltProtocolFactory( connectionFactory, stateMachineFactory, NullLogService.getInstance() );

        BoltProtocol protocol = factory.create( protocolVersion, boltChannel );

        protocol.install();

        // handler with correct version is created
        assertEquals( protocolVersion, protocol.version() );
        // it uses the expected worker
        verify( connectionFactory ).newConnection( eq( boltChannel ), any( BoltStateMachine.class ) );

        // and halts this same worker when closed
        verify( connection, never() ).stop();
        channel.close();
        verify( connection ).stop();

        channel.finishAndReleaseAll();
    }
}
