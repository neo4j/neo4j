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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.BoltProtocol;
import org.neo4j.bolt.BoltProtocolVersion;
import org.neo4j.bolt.dbapi.CustomBookmarkFormatParser;
import org.neo4j.bolt.messaging.BoltResponseMessageWriter;
import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.bolt.runtime.BoltConnectionFactory;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachine;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachineFactory;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.time.Clocks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.testing.BoltTestUtil.newTestBoltChannel;

class DefaultBoltProtocolFactoryTest
{
    @Test
    void shouldCreateNothingForUnknownProtocolVersion()
    {
        int protocolVersion = 42;
        BoltChannel channel = newTestBoltChannel();
        BoltProtocolFactory factory =
                new DefaultBoltProtocolFactory( mock( BoltConnectionFactory.class ), mock( BoltStateMachineFactory.class ),
                        NullLogService.getInstance(), new TestDatabaseIdRepository(), CustomBookmarkFormatParser.DEFAULT,
                        mock( TransportThrottleGroup.class ), Clocks.fakeClock(), Duration.ZERO );

        BoltProtocol protocol = factory.create( new BoltProtocolVersion( protocolVersion, 0 ), channel );
        // handler is not created
        assertNull( protocol );
    }

    @ParameterizedTest( name = "V{0}.{1}" )
    @CsvSource( { "3, 0", "4, 0", "4, 1"} )
    void shouldCreateBoltProtocol( int majorVersion, int minorVersion ) throws Throwable
    {
        EmbeddedChannel channel = new EmbeddedChannel();
        BoltChannel boltChannel = newTestBoltChannel( channel );
        BoltProtocolVersion boltProtocolVersion = new BoltProtocolVersion( majorVersion, minorVersion );

        BoltStateMachineFactory stateMachineFactory = mock( BoltStateMachineFactory.class );
        BoltStateMachine stateMachine = mock( BoltStateMachine.class );
        when( stateMachineFactory.newStateMachine( boltProtocolVersion, boltChannel ) ).thenReturn( stateMachine );

        BoltConnectionFactory connectionFactory = mock( BoltConnectionFactory.class );
        BoltConnection connection = mock( BoltConnection.class );
        when( connectionFactory.newConnection( eq( boltChannel ), eq( stateMachine ), any() ) )
                .thenReturn( connection );

        BoltProtocolFactory factory =
                new DefaultBoltProtocolFactory( connectionFactory, stateMachineFactory, NullLogService.getInstance(),
                        new TestDatabaseIdRepository(), CustomBookmarkFormatParser.DEFAULT,
                        mock( TransportThrottleGroup.class ), Clocks.fakeClock(), Duration.ZERO );

        BoltProtocol protocol = factory.create( boltProtocolVersion, boltChannel );

        protocol.install();

        // handler with correct version is created
        assertEquals( boltProtocolVersion, protocol.version() );
        // it uses the expected worker
        verify( connectionFactory ).newConnection( eq( boltChannel ), any( BoltStateMachine.class ),
                any( BoltResponseMessageWriter.class ) );

        // and halts this same worker when closed
        verify( connection, never() ).stop();
        channel.close();
        verify( connection ).stop();

        channel.finishAndReleaseAll();
    }
}
