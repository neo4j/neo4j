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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.bolt.AbstractBoltTransportsTest;
import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.OtherThreadExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.rule.OtherThreadRule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.bolt.testing.MessageConditions.msgSuccess;
import static org.neo4j.bolt.testing.TransportTestUtil.eventuallyDisconnects;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.OPTIONAL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@ExtendWith( OtherThreadExtension.class )
public class TransportUnauthenticatedConnectionTimeoutErrorIT extends AbstractBoltTransportsTest
{
    @Inject
    private Neo4jWithSocket server;

    @Inject
    private OtherThreadRule<Void> otherThread;

    @BeforeEach
    public void setup( TestInfo testInfo ) throws IOException
    {
        server.setConfigure( getSettingsFunction() );
        server.init( testInfo );
        address = server.lookupDefaultConnector();
    }

    @AfterEach
    public void cleanup()
    {
        server.shutdownDatabase();
    }

    protected Consumer<Map<Setting<?>,Object>> getSettingsFunction()
    {
        return settings -> {
            settings.put( BoltConnector.encryption_level, OPTIONAL );
            settings.put( BoltConnectorInternalSettings.unsupported_bolt_unauth_connection_timeout, Duration.ofSeconds( 5 ) );
            settings.put( BoltConnectorInternalSettings.unsupported_bolt_unauth_connection_max_inbound_bytes, ByteUnit.kibiBytes( 1 ) );
        };
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldFinishHelloMessage( Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

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

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldTimeoutTooSlowConnection( Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

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

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldTimeoutToHandshakeForHalfHandshake(
            Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        // Given half written bolt handshake message
        ByteBuffer bb = ByteBuffers.allocate( Integer.BYTES, INSTANCE );
        bb.putInt( 0x6060B017 );
        // When
        connection.connect( address ).send( bb.array() );

        // Then
        assertThat( connection ).satisfies( eventuallyDisconnects() );
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldTimeoutToAuthForHalfHelloMessage(
            Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

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

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldCloseConnectionDueToTooBigHelloMessage(
            Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        // When
        Map<String,Object> authMeta = new HashMap<>();
        for ( int i = 0; i < 100; i++ )
        {
            authMeta.put( "index-" + i, i );
        }
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.defaultAuth( authMeta ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( eventuallyDisconnects() );
    }
}
