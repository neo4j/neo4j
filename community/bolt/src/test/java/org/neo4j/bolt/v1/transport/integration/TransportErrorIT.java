/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.bolt.v1.transport.integration;

import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import org.neo4j.bolt.v1.messaging.PackStreamMessageFormatV1;
import org.neo4j.bolt.v1.messaging.RecordingByteChannel;
import org.neo4j.bolt.v1.packstream.BufferedChannelOutput;
import org.neo4j.bolt.v1.packstream.PackStream;
import org.neo4j.bolt.v1.transport.socket.client.Connection;
import org.neo4j.function.Factory;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.api.exceptions.Status;

import static org.neo4j.bolt.v1.messaging.message.Messages.run;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgFailure;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.serialize;

@RunWith( Parameterized.class )
public class TransportErrorIT
{
    @Rule
    public Neo4jWithSocket server = new Neo4jWithSocket();

    @Parameterized.Parameter(0)
    public Factory<Connection> cf;

    @Parameterized.Parameter(1)
    public HostnamePort address;

    private Connection client;

    @Parameterized.Parameters
    public static Collection<Object[]> transports()
    {
        return TransportSessionIT.transports();
    }

    @Test
    public void shouldHandleIncorrectFraming() throws Throwable
    {
        // Given I have a message that gets truncated in the chunking, so part of it is missing
        byte[] truncated = serialize( run( "UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared" ) );
        truncated = Arrays.copyOf(truncated, truncated.length - 12);

        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk( 32, truncated ) );

        // Then
        MatcherAssert.assertThat( client, TransportTestUtil.eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
        MatcherAssert.assertThat( client, TransportTestUtil.eventuallyRecieves(
                msgFailure( Status.Request.InvalidFormat,
                        "Unable to deserialize request, message boundary found before message ended. This indicates " +
                        "a serialization or framing problem with your client driver." ) ) );
    }

    @Test
    public void shouldHandleMessagesWithIncorrectFields() throws Throwable
    {
        // Given I send a message with the wrong types in its fields
        final RecordingByteChannel rawData = new RecordingByteChannel();
        final PackStream.Packer packer = new PackStream.Packer( new BufferedChannelOutput( rawData ) );

        packer.packStructHeader( 2, PackStreamMessageFormatV1.MessageTypes.MSG_RUN );
        packer.pack( "RETURN 1" );
        packer.pack( 1234 ); // Should've been a map
        packer.flush();

        byte[] invalidMessage = rawData.getBytes();

        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk( 32, invalidMessage ) );

        // Then
        MatcherAssert.assertThat( client, TransportTestUtil.eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
        MatcherAssert.assertThat( client, TransportTestUtil.eventuallyRecieves(
                msgFailure( Status.Request.InvalidFormat,
                        "Unable to read MSG_RUN message. Error was: Wrong type received. Expected MAP, received: " +
                        "INTEGER (0xff)." ) ) );
    }

    @Test
    public void shouldHandleUnknownMessages() throws Throwable
    {
        // Given I send a message with an invalid type
        final RecordingByteChannel rawData = new RecordingByteChannel();
        final PackStream.Packer packer = new PackStream.Packer( new BufferedChannelOutput( rawData ) );

        packer.packStructHeader( 1, (byte)0x66 ); // Invalid message type
        packer.pack( 1234 );
        packer.flush();

        byte[] invalidMessage = rawData.getBytes();

        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk( 32, invalidMessage ) );

        // Then
        MatcherAssert.assertThat( client, TransportTestUtil.eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
        MatcherAssert.assertThat( client, TransportTestUtil.eventuallyRecieves(
                msgFailure( Status.Request.Invalid, "0x66 is not a valid message type." ) ) );
    }

    @Test
    public void shouldHandleUnknownMarkerBytes() throws Throwable
    {
        // Given I send a message with an invalid type
        final RecordingByteChannel rawData = new RecordingByteChannel();
        final BufferedChannelOutput out = new BufferedChannelOutput( rawData );
        final PackStream.Packer packer = new PackStream.Packer( out );

        packer.packStructHeader( 2, PackStreamMessageFormatV1.MessageTypes.MSG_RUN );
        out.writeByte( PackStream.RESERVED_C4 ); // Invalid marker byte
        out.flush();

        byte[] invalidMessage = rawData.getBytes();

        // When
        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk( 32, invalidMessage ) );

        // Then
        MatcherAssert.assertThat( client, TransportTestUtil.eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
        MatcherAssert.assertThat( client, TransportTestUtil.eventuallyRecieves(
                msgFailure( Status.Request.InvalidFormat,
                        "Unable to read MSG_RUN message. Error was: Wrong type received. Expected STRING, received: " +
                        "RESERVED (0xff)." ) ) );
    }

    @Before
    public void setup()
    {
        this.client = cf.newInstance();
    }

    @After
    public void teardown() throws Exception
    {
        if(client != null) client.disconnect();
    }
}
