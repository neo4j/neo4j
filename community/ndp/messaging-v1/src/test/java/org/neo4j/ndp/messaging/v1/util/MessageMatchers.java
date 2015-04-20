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
package org.neo4j.ndp.messaging.v1.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import org.neo4j.kernel.impl.util.HexPrinter;
import org.neo4j.ndp.messaging.v1.PackStreamMessageFormatV1;
import org.neo4j.ndp.messaging.v1.RecordingByteChannel;
import org.neo4j.ndp.messaging.v1.RecordingMessageHandler;
import org.neo4j.ndp.messaging.v1.message.FailureMessage;
import org.neo4j.ndp.messaging.v1.message.IgnoredMessage;
import org.neo4j.ndp.messaging.v1.message.Message;
import org.neo4j.ndp.messaging.v1.message.RecordMessage;
import org.neo4j.ndp.messaging.v1.message.SuccessMessage;
import org.neo4j.ndp.runtime.internal.Neo4jError;
import org.neo4j.stream.Record;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

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
                description.appendText( "SUCCESS" );
            }
        };
    }

    public static Matcher<Message> msgFailure( final Neo4jError error )
    {
        return new TypeSafeMatcher<Message>()
        {
            @Override
            protected boolean matchesSafely( Message t )
            {
                assertThat( t, instanceOf( FailureMessage.class ) );
                FailureMessage msg = (FailureMessage) t;
                assertThat( msg.cause(), equalTo( error ) );
                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "SUCCESS" );
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
        final PackStreamMessageFormatV1.Writer packer = new PackStreamMessageFormatV1.Writer().reset(
                rawData );

        for ( Message message : messages )
        {
            packer.write( message );
        }
        packer.flush();

        return rawData.getBytes();
    }

    public static List<Message> messages( byte[] bytes ) throws IOException
    {
        PackStreamMessageFormatV1.Reader unpacker = new PackStreamMessageFormatV1.Reader();
        RecordingMessageHandler consumer = new RecordingMessageHandler();

        try
        {
            unpacker.reset( new ArrayByteChannel( bytes ) );

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
        PackStreamMessageFormatV1.Reader unpacker = new PackStreamMessageFormatV1.Reader();
        RecordingMessageHandler consumer = new RecordingMessageHandler();

        try
        {
            unpacker.reset( new ArrayByteChannel( bytes ) );

            if ( unpacker.hasNext() )
            {
                unpacker.read( consumer );
                return consumer.asList().get( 0 );
            }

            throw new IllegalArgumentException( "Expected a message in " + HexPrinter.hex( bytes ) );
        }
        catch ( Throwable e )
        {
            throw new IOException( "Failed to deserialize response, '" + e.getMessage() + "'. Messages read so " +
                                   "far: \n" + consumer.asList() + "\n" +
                                   "Raw data: \n" +
                                   HexPrinter.hex( bytes ), e );
        }
    }

}