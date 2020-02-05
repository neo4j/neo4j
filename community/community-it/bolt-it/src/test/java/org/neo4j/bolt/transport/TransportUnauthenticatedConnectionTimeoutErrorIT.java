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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.bolt.AbstractBoltTransportsTest;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.test.rule.OtherThreadRule;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.bolt.testing.MessageConditions.msgSuccess;
import static org.neo4j.bolt.testing.TransportTestUtil.eventuallyDisconnects;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.OPTIONAL;

public class TransportUnauthenticatedConnectionTimeoutErrorIT extends AbstractBoltTransportsTest
{
    @Rule
    public Neo4jWithSocket server = new Neo4jWithSocket( getClass(), getSettingsFunction() );

    @Rule
    public OtherThreadRule<Void> otherThread = new OtherThreadRule<>( 1, MINUTES );

    @Before
    public void setup()
    {
        address = server.lookupDefaultConnector();
    }

    protected Consumer<Map<Setting<?>,Object>> getSettingsFunction()
    {
        return settings -> {
            settings.put( BoltConnector.encryption_level, OPTIONAL );
            settings.put( BoltConnector.unsupported_bolt_unauth_connection_timeout, Duration.ofSeconds( 1 ) );
        };
    }

    @Test
    public void shouldFinishHelloMessage() throws Throwable
    {
        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth() );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );

        connection.disconnect();
        assertThat( connection ).satisfies( eventuallyDisconnects() );
    }

    @Test
    public void shouldTimeoutTooSlowConnection() throws Throwable
    {
        // Given
        var handshakeBytes = util.defaultAcceptedVersions();

        // When
        var conn = connection.connect( address );
        otherThread.execute( w -> {
            // Each byte will arrive within BoltConnector.unsupported_bolt_auth_timeout.
            // However the timeout is for all bytes.
            byte[] buffer = new byte[1];
            for ( var aByte : handshakeBytes )
            {
                buffer[0] = aByte;
                conn.send( buffer );
                Thread.sleep( 500 );
            }
            return null;
        } );

        // Then
        assertThat( connection ).satisfies( eventuallyDisconnects() );
    }

    @Test
    public void shouldTimeoutToHandshakeForHalfHandshake() throws Throwable
    {
        // Given half written bolt handshake message
        ByteBuffer bb = ByteBuffers.allocate( Integer.BYTES, BIG_ENDIAN );
        bb.putInt( 0x6060B017 );
        // When
        connection.connect( address ).send( bb.array() );

        // Then
        assertThat( connection ).satisfies( eventuallyDisconnects() );
    }

    @Test
    public void shouldTimeoutToAuthForHalfHelloMessage() throws Throwable
    {
        // Given half written hello message
        var helloMessage = util.defaultAuth();
        var buffer = ByteBuffer.wrap( helloMessage, 0, helloMessage.length / 2 );
        byte[] halfMessage = new byte[buffer.limit()];
        buffer.get( halfMessage );

        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( halfMessage );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( eventuallyDisconnects() );
    }
}
