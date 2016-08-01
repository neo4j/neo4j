/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.bolt.v1.messaging;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.neo4j.bolt.v1.messaging.infrastructure.ValueNode;
import org.neo4j.bolt.v1.messaging.infrastructure.ValueRelationship;
import org.neo4j.bolt.v1.messaging.message.*;
import org.neo4j.bolt.v1.packstream.BufferedChannelInput;
import org.neo4j.bolt.v1.packstream.BufferedChannelOutput;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.impl.util.HexPrinter;

import java.io.IOException;
import java.util.Map;

import static java.lang.System.lineSeparator;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.bolt.v1.messaging.BoltResponseMessageWriter.NO_BOUNDARY_HOOK;
import static org.neo4j.bolt.v1.messaging.message.AckFailureMessage.ackFailure;
import static org.neo4j.bolt.v1.messaging.message.DiscardAllMessage.discardAll;
import static org.neo4j.bolt.v1.messaging.message.InitMessage.init;
import static org.neo4j.bolt.v1.messaging.message.PullAllMessage.pullAll;
import static org.neo4j.bolt.v1.messaging.message.ResetMessage.reset;
import static org.neo4j.bolt.v1.messaging.message.RunMessage.run;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.serialize;
import static org.neo4j.bolt.v1.runtime.spi.Records.record;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.MapUtil.map;

public class BoltRequestMessageTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldHandleCommonMessages() throws Throwable
    {
        assertSerializes( init( "MyClient/1.0", map("scheme", "basic") ) );
        assertSerializes( ackFailure() );
        assertSerializes( reset() );
        assertSerializes( run( "CREATE (n) RETURN åäö" ) );
        assertSerializes( discardAll() );
        assertSerializes( pullAll() );
    }

    @Test
    public void shouldHandleParameterizedStatements() throws Throwable
    {
        // Given
        Map<String,Object> expected = map( "n", 12L );

        // When
        RunMessage msg = serializeAndDeserialize( run( "asd", expected ) );

        // Then
        assertThat( msg.params().entrySet(), equalTo( expected.entrySet() ) );
    }

    @Test
    public void shouldSerializeNode() throws Throwable
    {
        ValueNode valueNode = new ValueNode( 12L, asList( label( "User" ), label( "Banana" ) ),
                map( "name", "Bob", "age", 14 ) );

        assertThat( serialized( valueNode ),
                equalTo( "B1 71 91 B3 4E 0C 92 84 55 73 65 72 86 42 61 6E" + lineSeparator() +
                         "61 6E 61 A2 84 6E 61 6D 65 83 42 6F 62 83 61 67" + lineSeparator() +
                         "65 0E" ) );
    }

    @Test
    public void shouldSerializeRelationship() throws Throwable
    {
        ValueRelationship valueRelationship = new ValueRelationship( 12L, 1L, 2L, RelationshipType.withName( "KNOWS" ),
                map( "name", "Bob", "age", 14 ) );

        assertThat( serialized( valueRelationship ),
                equalTo( "B1 71 91 B5 52 0C 01 02 85 4B 4E 4F 57 53 A2 84" + lineSeparator() +
                         "6E 61 6D 65 83 42 6F 62 83 61 67 65 0E" ) );
    }

    private String serialized( Object object ) throws IOException
    {
        RecordMessage message =
                new RecordMessage( record( object ) );
        return HexPrinter.hex( serialize( message ), 4, " " );
    }

    private void assertSerializes( RequestMessage msg ) throws IOException
    {
        assertThat( serializeAndDeserialize( msg ), equalTo( msg ) );
    }

    private <T extends RequestMessage> T serializeAndDeserialize( T msg ) throws IOException
    {
        RecordingByteChannel channel = new RecordingByteChannel();
        BoltRequestMessageReader reader = new BoltRequestMessageReader(
                new Neo4jPack.Unpacker( new BufferedChannelInput( 16 ).reset( channel ) ) );
        BoltRequestMessageWriter writer = new BoltRequestMessageWriter(
                new Neo4jPack.Packer( new BufferedChannelOutput( channel ) ), NO_BOUNDARY_HOOK );

        writer.write( msg ).flush();

        channel.eof();
        return unpack( reader, channel );
    }

    private <T extends RequestMessage> T unpack( BoltRequestMessageReader reader, RecordingByteChannel channel )
    {
        // Unpack
        String serialized = HexPrinter.hex( channel.getBytes() );
        BoltRequestMessageRecorder messages = new BoltRequestMessageRecorder();
        try
        {
            reader.read( messages );
        }
        catch ( Throwable e )
        {
            throw new AssertionError( "Failed to unpack message, wire data was:\n" + serialized + "[" + channel
                    .getBytes().length + "b]", e );
        }

        return (T) messages.asList().get( 0 );
    }

}
