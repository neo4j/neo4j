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
package org.neo4j.ndp.messaging.v1;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Path;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.HexPrinter;
import org.neo4j.ndp.messaging.v1.infrastructure.ValueNode;
import org.neo4j.ndp.messaging.v1.infrastructure.ValueRelationship;
import org.neo4j.ndp.messaging.v1.message.AcknowledgeFailureMessage;
import org.neo4j.ndp.messaging.v1.message.DiscardAllMessage;
import org.neo4j.ndp.messaging.v1.message.FailureMessage;
import org.neo4j.ndp.messaging.v1.message.IgnoredMessage;
import org.neo4j.ndp.messaging.v1.message.InitializeMessage;
import org.neo4j.ndp.messaging.v1.message.Message;
import org.neo4j.ndp.messaging.v1.message.PullAllMessage;
import org.neo4j.ndp.messaging.v1.message.RecordMessage;
import org.neo4j.ndp.messaging.v1.message.RunMessage;
import org.neo4j.ndp.messaging.v1.message.SuccessMessage;
import org.neo4j.packstream.BufferedChannelInput;
import org.neo4j.packstream.BufferedChannelOutput;

import static java.util.Arrays.asList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.ndp.messaging.v1.PackStreamMessageFormatV1.Writer.NO_OP;
import static org.neo4j.ndp.messaging.v1.example.Paths.ALL_PATHS;
import static org.neo4j.ndp.runtime.spi.Records.record;

public class MessageFormatTest
{
    @Test
    public void shouldHandleCommonMessages() throws Throwable
    {
        assertSerializes( new RunMessage( "CREATE (n) RETURN åäö" ) );
        assertSerializes( new DiscardAllMessage() );
        assertSerializes( new PullAllMessage() );
        assertSerializes( new RecordMessage( record( 1l, "b", 2l ) ) );
        assertSerializes( new SuccessMessage( new HashMap<String,Object>() ) );
        assertSerializes( new FailureMessage( Status.General.UnknownFailure, "Err" ) );
        assertSerializes( new IgnoredMessage() );
        assertSerializes( new AcknowledgeFailureMessage() );
        assertSerializes( new InitializeMessage( "MyClient/1.0" ) );
    }

    @Test
    public void shouldHandleParameterizedStatements() throws Throwable
    {
        // Given
        Map<String,Object> expected = map( "n", 12l );

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
        assertSerializesNeoValue( 1337l );
        assertSerializesNeoValue( Long.MIN_VALUE );

        assertSerializesNeoValue( Double.MIN_VALUE );
        assertSerializesNeoValue( 13.37d );
        assertSerializesNeoValue( Double.MAX_VALUE );

        assertSerializesNeoValue( "" );
        assertSerializesNeoValue( "A basic piece of text" );
        assertSerializesNeoValue( new String( new byte[16000], StandardCharsets.UTF_8 ) );

        assertSerializesNeoValue( asList() );
        assertSerializesNeoValue( asList( null, null ) );
        assertSerializesNeoValue( asList( true, false ) );
        assertSerializesNeoValue( asList( "one", "", "three" ) );
        assertSerializesNeoValue( asList( 12.4d, 0.0d ) );

        assertSerializesNeoValue( map( "k", null ) );
        assertSerializesNeoValue( map( "k", true ) );
        assertSerializesNeoValue( map( "k", false ) );
        assertSerializesNeoValue( map( "k", 1337l ) );
        assertSerializesNeoValue( map( "k", 133.7d ) );
        assertSerializesNeoValue( map( "k", "Hello" ) );
        assertSerializesNeoValue( map( "k", asList( "one", "", "three" ) ) );
    }

    @Test
    public void shouldSerializeNode() throws Throwable
    {
        assertSerializesNeoValue( new ValueNode( 12l, asList( label( "User" ), label( "Banana" ) ),
                map( "name", "Bob", "age", 14 ) ) );
    }

    @Test
    public void shouldSerializeRelationship() throws Throwable
    {
        assertSerializesNeoValue( new ValueRelationship( 12l, 1l, 2l, DynamicRelationshipType.withName( "KNOWS" ),
                map( "name", "Bob", "age", 14 ) ) );
    }

    @Test
    public void shouldSerializePaths() throws Throwable
    {
        for ( Path path : ALL_PATHS )
        {
            assertSerializesNeoValue( path );
        }
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
