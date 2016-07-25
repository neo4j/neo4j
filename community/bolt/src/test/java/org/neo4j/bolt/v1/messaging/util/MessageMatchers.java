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
package org.neo4j.bolt.v1.messaging.util;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.bolt.v1.messaging.MessageFormat;
import org.neo4j.bolt.v1.messaging.Neo4jPack;
import org.neo4j.bolt.v1.messaging.PackStreamMessageFormatV1;
import org.neo4j.bolt.v1.messaging.RecordingByteChannel;
import org.neo4j.bolt.v1.messaging.RecordingMessageHandler;
import org.neo4j.bolt.v1.messaging.message.FailureMessage;
import org.neo4j.bolt.v1.messaging.message.IgnoredMessage;
import org.neo4j.bolt.v1.messaging.message.Message;
import org.neo4j.bolt.v1.messaging.message.RecordMessage;
import org.neo4j.bolt.v1.messaging.message.SuccessMessage;
import org.neo4j.bolt.v1.packstream.BufferedChannelInput;
import org.neo4j.bolt.v1.packstream.BufferedChannelOutput;
import org.neo4j.bolt.v1.runtime.spi.Record;
import org.neo4j.bolt.v1.transport.integration.TestNotification;
import org.neo4j.graphdb.Notification;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.HexPrinter;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.neo4j.bolt.v1.messaging.PackStreamMessageFormatV1.Writer.NO_OP;

public class MessageMatchers
{

    public static Matcher<List<Message>> equalsMessages( final Matcher<Message>... messageMatchers )
    {
        return new TypeSafeMatcher<List<Message>>()
        {
            @Override
            protected boolean matchesSafely( List<Message> messages )
            {
                if ( messageMatchers.length != messages.size() )
                {
                    return false;
                }
                for ( int i = 0; i < messageMatchers.length; i++ )
                {
                    if ( !messageMatchers[i].matches( messages.get( i ) ) ) { return false; }
                }
                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendList( "MessageList[", ", ", "]", Arrays.asList( messageMatchers
                ) );
            }
        };
    }

    public static Matcher<Message> hasNotification( Notification notification)
    {
        return new TypeSafeMatcher<Message>()
        {
            @Override
            protected boolean matchesSafely( Message t )
            {
                assertThat( t, instanceOf( SuccessMessage.class ) );
                Map<String,Object> meta = ((SuccessMessage) t).meta();
                assertThat(meta, Matchers.hasKey("notifications"));
                Set<Notification> notifications =
                        ((List<Map<String, Object>>)meta.get( "notifications" ))
                                .stream()
                                .map( TestNotification::fromMap )
                                .collect( Collectors.toSet() );

                assertThat( notifications, Matchers.contains( notification ));
                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "SUCCESS" );
            }
        };
    }

    public static Matcher<Message> msgSuccess( final Map<String,Object> metadata )
    {
        return new TypeSafeMatcher<Message>()
        {
            @Override
            protected boolean matchesSafely( Message t )
            {
                assertThat( t, instanceOf( SuccessMessage.class ) );
                assertThat( ((SuccessMessage) t).meta(), equalTo( metadata ) );
                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "SUCCESS" );
            }
        };
    }

    public static Matcher<Message> msgSuccess()
    {
        return new TypeSafeMatcher<Message>()
        {
            @Override
            protected boolean matchesSafely( Message t )
            {
                assertThat( t, instanceOf( SuccessMessage.class ) );
                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "SUCCESS" );
            }
        };
    }

    public static Matcher<Message> msgIgnored()
    {
        return new TypeSafeMatcher<Message>()
        {
            @Override
            protected boolean matchesSafely( Message t )
            {
                assertThat( t, instanceOf( IgnoredMessage.class ) );
                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "IGNORED" );
            }
        };
    }

    public static Matcher<Message> msgFailure( final Status status, final String message )
    {
        return new TypeSafeMatcher<Message>()
        {
            @Override
            protected boolean matchesSafely( Message t )
            {
                assertThat( t, instanceOf( FailureMessage.class ) );
                FailureMessage msg = (FailureMessage) t;
                assertThat( msg.status(), equalTo( status ) );
                assertThat( msg.message(), containsString( message ) );
                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "FAILURE" );
            }
        };
    }

    public static Matcher<Message> msgRecord( final Matcher<Record> matcher )
    {
        return new TypeSafeMatcher<Message>()
        {
            @Override
            protected boolean matchesSafely( Message t )
            {
                assertThat( t, instanceOf( RecordMessage.class ) );

                RecordMessage msg = (RecordMessage) t;
                assertThat( msg.record(), matcher );
                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "RECORD " );
                description.appendDescriptionOf( matcher );
            }
        };
    }

    public static byte[] serialize( Message... messages ) throws IOException
    {
        final RecordingByteChannel rawData = new RecordingByteChannel();
        final MessageFormat.Writer packer = new PackStreamMessageFormatV1.Writer( new Neo4jPack.Packer( new
                BufferedChannelOutput( rawData )), NO_OP );

        for ( Message message : messages )
        {
            packer.write( message );
        }
        packer.flush();

        return rawData.getBytes();
    }

    public static List<Message> messages( byte[] bytes ) throws IOException
    {
        PackStreamMessageFormatV1.Reader unpacker = reader( bytes );
        RecordingMessageHandler consumer = new RecordingMessageHandler();

        try
        {
            while ( unpacker.hasNext() )
            {
                unpacker.read( consumer );
            }

            return consumer.asList();
        }
        catch ( Throwable e )
        {
            throw new IOException( "Failed to deserialize response, '" + e.getMessage() + "'. Messages read so " +
                                   "far: \n" + consumer.asList() + "\n" +
                                   "Raw data: \n" +
                                   HexPrinter.hex( bytes ) );
        }
    }

    public static Message message( byte[] bytes ) throws IOException
    {
        PackStreamMessageFormatV1.Reader unpacker = reader( bytes );
        RecordingMessageHandler consumer = new RecordingMessageHandler();

        try
        {
            if ( unpacker.hasNext() )
            {
                unpacker.read( consumer );
                return consumer.asList().get( 0 );
            }

            throw new IllegalArgumentException( "Expected a message in `" + HexPrinter.hex( bytes ) + "`" );
        }
        catch ( Throwable e )
        {
            throw new IOException( "Failed to deserialize response, '" + e.getMessage() + "'.\n" +
                                   "Raw data: \n" + HexPrinter.hex( bytes ), e );
        }
    }

    private static PackStreamMessageFormatV1.Reader reader( byte[] bytes )
    {
        return new PackStreamMessageFormatV1.Reader(
                    new Neo4jPack.Unpacker( new BufferedChannelInput( 128 ).reset( new ArrayByteChannel( bytes ) ) ) );
    }

}
