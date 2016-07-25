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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.bolt.v1.messaging.infrastructure.ValueNode;
import org.neo4j.bolt.v1.messaging.infrastructure.ValueRelationship;
import org.neo4j.bolt.v1.messaging.message.AckFailureMessage;
import org.neo4j.bolt.v1.messaging.message.DiscardAllMessage;
import org.neo4j.bolt.v1.messaging.message.FailureMessage;
import org.neo4j.bolt.v1.messaging.message.IgnoredMessage;
import org.neo4j.bolt.v1.messaging.message.InitMessage;
import org.neo4j.bolt.v1.messaging.message.Message;
import org.neo4j.bolt.v1.messaging.message.PullAllMessage;
import org.neo4j.bolt.v1.messaging.message.RecordMessage;
import org.neo4j.bolt.v1.messaging.message.ResetMessage;
import org.neo4j.bolt.v1.messaging.message.RunMessage;
import org.neo4j.bolt.v1.messaging.message.SuccessMessage;
import org.neo4j.bolt.v1.packstream.BufferedChannelInput;
import org.neo4j.bolt.v1.packstream.BufferedChannelOutput;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.HexPrinter;

import static java.lang.System.lineSeparator;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.bolt.v1.messaging.PackStreamMessageFormatV1.Writer.NO_OP;
import static org.neo4j.bolt.v1.messaging.example.Paths.PATH_WITH_LENGTH_ONE;
import static org.neo4j.bolt.v1.messaging.example.Paths.PATH_WITH_LENGTH_TWO;
import static org.neo4j.bolt.v1.messaging.example.Paths.PATH_WITH_LENGTH_ZERO;
import static org.neo4j.bolt.v1.messaging.example.Paths.PATH_WITH_LOOP;
import static org.neo4j.bolt.v1.messaging.example.Paths.PATH_WITH_NODES_VISITED_MULTIPLE_TIMES;
import static org.neo4j.bolt.v1.messaging.example.Paths.PATH_WITH_RELATIONSHIP_TRAVERSED_AGAINST_ITS_DIRECTION;
import static org.neo4j.bolt.v1.messaging.example.Paths
        .PATH_WITH_RELATIONSHIP_TRAVERSED_MULTIPLE_TIMES_IN_SAME_DIRECTION;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.serialize;
import static org.neo4j.bolt.v1.runtime.spi.Records.record;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.MapUtil.map;

public class MessageFormatTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldHandleCommonMessages() throws Throwable
    {
        assertSerializes( new RunMessage( "CREATE (n) RETURN åäö" ) );
        assertSerializes( new DiscardAllMessage() );
        assertSerializes( new PullAllMessage() );
        assertSerializes( new RecordMessage( record( 1L, "b", 2L ) ) );
        assertSerializes( new SuccessMessage( new HashMap<>() ) );
        assertSerializes( new FailureMessage( Status.General.UnknownError, "Err" ) );
        assertSerializes( new IgnoredMessage() );
        assertSerializes( new ResetMessage() );
        assertSerializes( new AckFailureMessage() );
        assertSerializes( new InitMessage( "MyClient/1.0", map("scheme", "basic") ) );
    }

    @Test
    public void shouldHandleParameterizedStatements() throws Throwable
    {
        // Given
        Map<String,Object> expected = map( "n", 12L );

        // When
        RunMessage msg = serializeAndDeserialize( new RunMessage( "asd", expected ) );

        // Then
        assertThat( msg.params().entrySet(), equalTo( expected.entrySet() ) );
    }

    @Test
    public void shouldSerializeBasicTypes() throws Throwable
    {
        assertSerializesNeoValue( null );
        assertSerializesNeoValue( true );
        assertSerializesNeoValue( false );

        assertSerializesNeoValue( Long.MAX_VALUE );
        assertSerializesNeoValue( 1337L );
        assertSerializesNeoValue( Long.MIN_VALUE );

        assertSerializesNeoValue( Double.MIN_VALUE );
        assertSerializesNeoValue( 13.37d );
        assertSerializesNeoValue( Double.MAX_VALUE );

        assertSerializesNeoValue( "" );
        assertSerializesNeoValue( "A basic piece of text" );
        assertSerializesNeoValue( new String( new byte[16000], StandardCharsets.UTF_8 ) );

        assertSerializesNeoValue( emptyList() );
        assertSerializesNeoValue( asList( null, null ) );
        assertSerializesNeoValue( asList( true, false ) );
        assertSerializesNeoValue( asList( "one", "", "three" ) );
        assertSerializesNeoValue( asList( 12.4d, 0.0d ) );

        assertSerializesNeoValue( map( "k", null ) );
        assertSerializesNeoValue( map( "k", true ) );
        assertSerializesNeoValue( map( "k", false ) );
        assertSerializesNeoValue( map( "k", 1337L ) );
        assertSerializesNeoValue( map( "k", 133.7d ) );
        assertSerializesNeoValue( map( "k", "Hello" ) );
        assertSerializesNeoValue( map( "k", asList( "one", "", "three" ) ) );
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

    @Test
    public void shouldSerializePaths() throws Throwable
    {
        assertThat( serialized( PATH_WITH_LENGTH_ZERO ),
                equalTo( "B1 71 91 B3 50 91 B3 4E C9 03 E9 92 86 50 65 72" + lineSeparator() +
                         "73 6F 6E 88 45 6D 70 6C 6F 79 65 65 A2 84 6E 61" + lineSeparator() +
                         "6D 65 85 41 6C 69 63 65 83 61 67 65 21 90 90"  ) );
        assertThat( serialized( PATH_WITH_LENGTH_ONE ),
                equalTo( "B1 71 91 B3 50 92 B3 4E C9 03 E9 92 86 50 65 72" + lineSeparator() +
                         "73 6F 6E 88 45 6D 70 6C 6F 79 65 65 A2 84 6E 61" + lineSeparator() +
                         "6D 65 85 41 6C 69 63 65 83 61 67 65 21 B3 4E C9" + lineSeparator() +
                         "03 EA 92 86 50 65 72 73 6F 6E 88 45 6D 70 6C 6F" + lineSeparator() +
                         "79 65 65 A2 84 6E 61 6D 65 83 42 6F 62 83 61 67" + lineSeparator() +
                         "65 2C 91 B3 72 0C 85 4B 4E 4F 57 53 A1 85 73 69" + lineSeparator() +
                         "6E 63 65 C9 07 CF 92 01 01"
        ) );
        assertThat( serialized( PATH_WITH_LENGTH_TWO ),
                equalTo( "B1 71 91 B3 50 93 B3 4E C9 03 E9 92 86 50 65 72"+ lineSeparator() +
                         "73 6F 6E 88 45 6D 70 6C 6F 79 65 65 A2 84 6E 61"+ lineSeparator() +
                         "6D 65 85 41 6C 69 63 65 83 61 67 65 21 B3 4E C9"+ lineSeparator() +
                         "03 EB 91 86 50 65 72 73 6F 6E A1 84 6E 61 6D 65"+ lineSeparator() +
                         "85 43 61 72 6F 6C B3 4E C9 03 EC 90 A1 84 6E 61"+ lineSeparator() +
                         "6D 65 84 44 61 76 65 92 B3 72 0D 85 4C 49 4B 45"+ lineSeparator() +
                         "53 A0 B3 72 22 8A 4D 41 52 52 49 45 44 5F 54 4F"+ lineSeparator() +
                         "A0 94 01 01 02 02"  ) );
        assertThat( serialized( PATH_WITH_RELATIONSHIP_TRAVERSED_AGAINST_ITS_DIRECTION),
                equalTo( "B1 71 91 B3 50 94 B3 4E C9 03 E9 92 86 50 65 72"+ lineSeparator() +
                         "73 6F 6E 88 45 6D 70 6C 6F 79 65 65 A2 84 6E 61"+ lineSeparator() +
                         "6D 65 85 41 6C 69 63 65 83 61 67 65 21 B3 4E C9"+ lineSeparator() +
                         "03 EA 92 86 50 65 72 73 6F 6E 88 45 6D 70 6C 6F"+ lineSeparator() +
                         "79 65 65 A2 84 6E 61 6D 65 83 42 6F 62 83 61 67"+ lineSeparator() +
                         "65 2C B3 4E C9 03 EB 91 86 50 65 72 73 6F 6E A1"+ lineSeparator() +
                         "84 6E 61 6D 65 85 43 61 72 6F 6C B3 4E C9 03 EC"+ lineSeparator() +
                         "90 A1 84 6E 61 6D 65 84 44 61 76 65 93 B3 72 0C"+ lineSeparator() +
                         "85 4B 4E 4F 57 53 A1 85 73 69 6E 63 65 C9 07 CF"+ lineSeparator() +
                         "B3 72 20 88 44 49 53 4C 49 4B 45 53 A0 B3 72 22"+ lineSeparator() +
                         "8A 4D 41 52 52 49 45 44 5F 54 4F A0 96 01 01 FE"+ lineSeparator() +
                         "02 03 03" ) );
        assertThat( serialized( PATH_WITH_NODES_VISITED_MULTIPLE_TIMES ),
                equalTo( "B1 71 91 B3 50 93 B3 4E C9 03 E9 92 86 50 65 72" + lineSeparator() +
                         "73 6F 6E 88 45 6D 70 6C 6F 79 65 65 A2 84 6E 61" + lineSeparator() +
                         "6D 65 85 41 6C 69 63 65 83 61 67 65 21 B3 4E C9" + lineSeparator() +
                         "03 EA 92 86 50 65 72 73 6F 6E 88 45 6D 70 6C 6F" + lineSeparator() +
                         "79 65 65 A2 84 6E 61 6D 65 83 42 6F 62 83 61 67" + lineSeparator() +
                         "65 2C B3 4E C9 03 EB 91 86 50 65 72 73 6F 6E A1" + lineSeparator() +
                         "84 6E 61 6D 65 85 43 61 72 6F 6C 93 B3 72 0C 85" + lineSeparator() +
                         "4B 4E 4F 57 53 A1 85 73 69 6E 63 65 C9 07 CF B3" + lineSeparator() +
                         "72 0D 85 4C 49 4B 45 53 A0 B3 72 20 88 44 49 53" + lineSeparator() +
                         "4C 49 4B 45 53 A0 9A 01 01 FF 00 02 02 03 01 FD" + lineSeparator() +
                         "02" ) );
        assertThat( serialized( PATH_WITH_RELATIONSHIP_TRAVERSED_MULTIPLE_TIMES_IN_SAME_DIRECTION ),
                equalTo( "B1 71 91 B3 50 94 B3 4E C9 03 E9 92 86 50 65 72" + lineSeparator() +
                         "73 6F 6E 88 45 6D 70 6C 6F 79 65 65 A2 84 6E 61" + lineSeparator() +
                         "6D 65 85 41 6C 69 63 65 83 61 67 65 21 B3 4E C9" + lineSeparator() +
                         "03 EB 91 86 50 65 72 73 6F 6E A1 84 6E 61 6D 65" + lineSeparator() +
                         "85 43 61 72 6F 6C B3 4E C9 03 EA 92 86 50 65 72" + lineSeparator() +
                         "73 6F 6E 88 45 6D 70 6C 6F 79 65 65 A2 84 6E 61" + lineSeparator() +
                         "6D 65 83 42 6F 62 83 61 67 65 2C B3 4E C9 03 EC" + lineSeparator() +
                         "90 A1 84 6E 61 6D 65 84 44 61 76 65 94 B3 72 0D" + lineSeparator() +
                         "85 4C 49 4B 45 53 A0 B3 72 20 88 44 49 53 4C 49" + lineSeparator() +
                         "4B 45 53 A0 B3 72 0C 85 4B 4E 4F 57 53 A1 85 73" + lineSeparator() +
                         "69 6E 63 65 C9 07 CF B3 72 22 8A 4D 41 52 52 49" + lineSeparator() +
                         "45 44 5F 54 4F A0 9A 01 01 02 02 FD 00 01 01 04" + lineSeparator() +
                         "03") );
        assertThat( serialized( PATH_WITH_LOOP ),
                equalTo( "B1 71 91 B3 50 92 B3 4E C9 03 EB 91 86 50 65 72" + lineSeparator() +
                         "73 6F 6E A1 84 6E 61 6D 65 85 43 61 72 6F 6C B3" + lineSeparator() +
                         "4E C9 03 EC 90 A1 84 6E 61 6D 65 84 44 61 76 65" + lineSeparator() +
                         "92 B3 72 22 8A 4D 41 52 52 49 45 44 5F 54 4F A0" + lineSeparator() +
                         "B3 72 2C 89 57 4F 52 4B 53 5F 46 4F 52 A0 94 01" + lineSeparator() +
                         "01 02 01") );
    }

    private String serialized( Object object ) throws IOException
    {
        RecordMessage message =
                new RecordMessage( record( object ) );
        return HexPrinter.hex( serialize( message ), 4, " " );
    }

    private void assertSerializes( Message msg ) throws IOException
    {
        assertThat( serializeAndDeserialize( msg ), equalTo( msg ) );
    }

    private <T extends Message> T serializeAndDeserialize( T msg ) throws IOException
    {
        RecordingByteChannel channel = new RecordingByteChannel();
        MessageFormat.Reader reader = new PackStreamMessageFormatV1.Reader(
                new Neo4jPack.Unpacker( new BufferedChannelInput( 16 ).reset( channel ) ) );
        MessageFormat.Writer writer = new PackStreamMessageFormatV1.Writer(
                new Neo4jPack.Packer( new BufferedChannelOutput( channel ) ), NO_OP );

        writer.write( msg ).flush();

        channel.eof();
        return unpack( reader, channel );
    }

    private <T extends Message> T unpack( MessageFormat.Reader reader, RecordingByteChannel channel )
    {
        // Unpack
        String serialized = HexPrinter.hex( channel.getBytes() );
        RecordingMessageHandler messages = new RecordingMessageHandler();
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

    private void assertSerializesNeoValue( Object val ) throws IOException
    {
        assertSerializes( new RecordMessage( record( val ) ) );
    }

}
