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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;

import org.neo4j.bolt.AbstractBoltTransportsTest;
import org.neo4j.bolt.messaging.RecordingByteChannel;
import org.neo4j.bolt.packstream.BufferedChannelOutput;
import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.packstream.PackStream;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.v4.messaging.RunMessage;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.bolt.testing.MessageConditions.serialize;
import static org.neo4j.bolt.testing.TransportTestUtil.eventuallyDisconnects;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
public class TransportErrorIT extends AbstractBoltTransportsTest
{
    @Inject
    private Neo4jWithSocket server;

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

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldHandleIncorrectFraming( Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        // Given I have a message that gets truncated in the chunking, so part of it is missing
        byte[] truncated = serialize( util.getNeo4jPack(), new RunMessage( "UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared" ) );
        truncated = Arrays.copyOf(truncated, truncated.length - 12);

        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.chunk( 32, truncated ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( eventuallyDisconnects() );
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldHandleMessagesWithIncorrectFields(
            Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        // Given I send a message with the wrong types in its fields
        final RecordingByteChannel rawData = new RecordingByteChannel();
        final PackStream.Packer packer = new PackStream.Packer( new BufferedChannelOutput( rawData ) );

        packer.packStructHeader( 2, RunMessage.SIGNATURE );
        packer.pack( "RETURN 1" );
        packer.pack( 1234 ); // Should've been a map
        packer.flush();

        byte[] invalidMessage = rawData.getBytes();

        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.chunk( 32, invalidMessage ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( eventuallyDisconnects() );
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldHandleUnknownMessages( Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        // Given I send a message with an invalid type
        final RecordingByteChannel rawData = new RecordingByteChannel();
        final PackStream.Packer packer = new PackStream.Packer( new BufferedChannelOutput( rawData ) );

        packer.packStructHeader( 1, (byte)0x66 ); // Invalid message type
        packer.pack( 1234 );
        packer.flush();

        byte[] invalidMessage = rawData.getBytes();

        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.chunk( 32, invalidMessage ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( eventuallyDisconnects() );
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldHandleUnknownMarkerBytes( Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        // Given I send a message with an invalid type
        final RecordingByteChannel rawData = new RecordingByteChannel();
        final BufferedChannelOutput out = new BufferedChannelOutput( rawData );
        final PackStream.Packer packer = new PackStream.Packer( out );

        packer.packStructHeader( 2, RunMessage.SIGNATURE );
        out.writeByte( PackStream.RESERVED_C7 ); // Invalid marker byte
        out.flush();

        byte[] invalidMessage = rawData.getBytes();

        // When
        connection.connect( address )
                .send( util.defaultAcceptedVersions() )
                .send( util.chunk( 32, invalidMessage ) );

        // Then
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        assertThat( connection ).satisfies( eventuallyDisconnects() );
    }
}
