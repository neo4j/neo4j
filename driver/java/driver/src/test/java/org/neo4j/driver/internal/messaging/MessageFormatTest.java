/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.driver.internal.messaging;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.internal.SimpleNode;
import org.neo4j.driver.internal.SimplePath;
import org.neo4j.driver.internal.SimpleRelationship;
import org.neo4j.driver.util.BytePrinter;
import org.neo4j.packstream.PackStream;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.driver.Values.properties;
import static org.neo4j.driver.Values.value;

public class MessageFormatTest
{
    public MessageFormat format = new PackStreamMessageFormatV1();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldPackAllRequests() throws Throwable
    {
        assertSerializes( new RunMessage( "Hello", properties() ) );
        assertSerializes( new RunMessage( "Hello", properties( "a", 12 ) ) );
        assertSerializes( new PullAllMessage() );
        assertSerializes( new DiscardAllMessage() );
        assertSerializes( new IgnoredMessage() );
        assertSerializes( new FailureMessage( "Neo.Banana.Bork.Birk", "Hello, world!" ) );
    }

    @Test
    public void shouldUnpackAllResponses() throws Throwable
    {
        assertSerializes( new RecordMessage( new Value[]{value( 1337l )} ) );
        assertSerializes( new SuccessMessage( new HashMap<String,Value>() ) );
    }

    @Test
    public void shouldUnpackAllValues() throws Throwable
    {
        assertSerializesValue( value( properties( "k", 12, "a", "banana" ) ) );
        assertSerializesValue( value( asList( "k", 12, "a", "banana" ) ) );
        assertSerializesValue( value(
                new SimpleNode( "node/1", asList( "User" ), properties( "name", "Bob", "age", 45 ) ) ) );
        assertSerializesValue( value(
                new SimpleRelationship( "rel/1", "node/1", "node/1",
                        "KNOWS",
                        properties( "name", "Bob", "age", 45 ) ) ) );
        assertSerializesValue( value(
                new SimplePath(
                        new SimpleNode( "node/1" ),
                        new SimpleRelationship( "relationship/1", "node/1", "node/1",
                                "KNOWS", properties() ),
                        new SimpleNode( "node/1" ),
                        new SimpleRelationship( "relationship/2", "node/1", "node/1",
                                "LIKES", properties() ),
                        new SimpleNode( "node/1" )
                ) ) );
    }

    @Test
    public void shouldGiveHelpfulErrorOnMalformedNodeStruct() throws Throwable
    {
        // Given
        ByteArrayOutputStream out = new ByteArrayOutputStream( 128 );
        PackStream.Packer packer = new PackStream.Packer( 8192 );
        packer.reset( Channels.newChannel( out ) );

        packer.packStructHeader( 1, (char) PackStreamMessageFormatV1.MSG_RECORD );
        packer.packListHeader( 1 );
        packer.packStructHeader( 0, PackStreamMessageFormatV1.NODE );
        packer.flush();

        // Expect
        exception.expect( ClientException.class );
        exception.expectMessage( "Invalid message received, serialized NODE structures should have 3 fields, " +
                                 "received NODE structure has 0 fields." );

        // When
        unpack( out.toByteArray() );
    }

    private void assertSerializesValue( Value value ) throws IOException
    {
        assertSerializes( new RecordMessage( new Value[]{value} ) );
    }

    private void assertSerializes( Message... messages ) throws IOException
    {
        MessageFormat.Writer writer = format.newWriter();

        // Pack
        final ByteArrayOutputStream out = new ByteArrayOutputStream( 128 );
        writer.reset( out );
        for ( Message message : messages )
        {
            writer.write( message );
        }
        writer.flush();

        // Unpack
        assertThat( unpack( out.toByteArray() ), equalTo( asList( messages ) ) );
    }

    private ArrayList<Message> unpack( byte[] data )
    {
        final ArrayList<Message> outcome = new ArrayList<>();
        try
        {
            MessageFormat.Reader reader = format.newReader();
            reader.reset( new ByteArrayInputStream( data ) );
            reader.read( new MessageHandler()
            {
                @Override
                public void handlePullAllMessage()
                {
                    outcome.add( new PullAllMessage() );
                }

                @Override
                public void handleRunMessage( String statement, Map<String,Value> parameters )
                {
                    outcome.add( new RunMessage( statement, parameters ) );
                }

                @Override
                public void handleDiscardAllMessage()
                {
                    outcome.add( new DiscardAllMessage() );
                }

                @Override
                public void handleAckFailureMessage()
                {
                    outcome.add( new AckFailureMessage() );
                }

                @Override
                public void handleSuccessMessage( Map<String,Value> meta )
                {
                    outcome.add( new SuccessMessage( meta ) );
                }

                @Override
                public void handleRecordMessage( Value[] fields )
                {
                    outcome.add( new RecordMessage( fields ) );
                }

                @Override
                public void handleFailureMessage( String code, String message )
                {
                    outcome.add( new FailureMessage( code, message ) );
                }

                @Override
                public void handleIgnoredMessage()
                {
                    outcome.add( new IgnoredMessage() );
                }
            } );
        }
        catch ( Neo4jException e )
        {
            throw e;
        }
        catch ( Throwable e )
        {
            throw new RuntimeException( "Failed to deserialize message. Raw data was:\n" + BytePrinter.hex( data ), e );
        }
        return outcome;
    }
}
