/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.bolt.logging.NullBoltMessageLogger;
import org.neo4j.bolt.v1.messaging.BoltRequestMessageWriter;
import org.neo4j.bolt.v1.messaging.BoltResponseMessageReader;
import org.neo4j.bolt.v1.messaging.BoltResponseMessageRecorder;
import org.neo4j.bolt.v1.messaging.BoltResponseMessageWriter;
import org.neo4j.bolt.v1.messaging.Neo4jPack;
import org.neo4j.bolt.v1.messaging.RecordingByteChannel;
import org.neo4j.bolt.v1.messaging.message.FailureMessage;
import org.neo4j.bolt.v1.messaging.message.IgnoredMessage;
import org.neo4j.bolt.v1.messaging.message.RecordMessage;
import org.neo4j.bolt.v1.messaging.message.RequestMessage;
import org.neo4j.bolt.v1.messaging.message.ResponseMessage;
import org.neo4j.bolt.v1.messaging.message.SuccessMessage;
import org.neo4j.bolt.v1.packstream.BufferedChannelInput;
import org.neo4j.bolt.v1.packstream.BufferedChannelOutput;
import org.neo4j.bolt.v1.transport.integration.TestNotification;
import org.neo4j.cypher.result.QueryResult;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.BaseToObjectValueWriter;
import org.neo4j.kernel.impl.util.HexPrinter;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.virtual.MapValue;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.v1.messaging.BoltResponseMessageWriter.NO_BOUNDARY_HOOK;

@SuppressWarnings( "unchecked" )
public class MessageMatchers
{
    private MessageMatchers()
    {
    }

    private static Map<String,Object> toRawMap( MapValue mapValue )
    {
        Deserializer deserializer = new Deserializer();
        HashMap<String,Object> map = new HashMap<>( mapValue.size() );
        for ( Map.Entry<String,AnyValue> entry : mapValue.entrySet() )
        {
            entry.getValue().writeTo( deserializer );
            map.put( entry.getKey(), deserializer.value() );
        }
        return map;
    }

    private static class Deserializer extends BaseToObjectValueWriter<RuntimeException>
    {

        @Override
        protected Node newNodeProxyById( long id )
        {
            return null;
        }

        @Override
        protected Relationship newRelationshipProxyById( long id )
        {
            return null;
        }

        @Override
        protected Point newPoint( CoordinateReferenceSystem crs, double[] coordinate )
        {
            return null;
        }
    }

    public static Matcher<List<ResponseMessage>> equalsMessages( final Matcher<ResponseMessage>... messageMatchers )
    {
        return new TypeSafeMatcher<List<ResponseMessage>>()
        {
            @Override
            protected boolean matchesSafely( List<ResponseMessage> messages )
            {
                if ( messageMatchers.length != messages.size() )
                {
                    return false;
                }
                for ( int i = 0; i < messageMatchers.length; i++ )
                {
                    if ( !messageMatchers[i].matches( messages.get( i ) ) )
                    {
                        return false;
                    }
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

    public static Matcher<ResponseMessage> hasNotification( Notification notification )
    {
        return new TypeSafeMatcher<ResponseMessage>()
        {
            @Override
            protected boolean matchesSafely( ResponseMessage t )
            {
                assertThat( t, instanceOf( SuccessMessage.class ) );
                Map<String,Object> meta = toRawMap( ((SuccessMessage) t).meta() );

                assertThat( meta.containsKey( "notifications" ), is( true ) );
                Set<Notification> notifications =
                        ((List<Map<String,Object>>) meta.get( "notifications" ))
                                .stream()
                                .map( TestNotification::fromMap )
                                .collect( Collectors.toSet() );

                assertThat( notifications, Matchers.contains( notification ) );
                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "SUCCESS" );
            }
        };
    }

    public static Matcher<ResponseMessage> msgSuccess( final Map<String,Object> metadata )
    {
        return new TypeSafeMatcher<ResponseMessage>()
        {
            @Override
            protected boolean matchesSafely( ResponseMessage t )
            {
                assertThat( t, instanceOf( SuccessMessage.class ) );
                assertThat( toRawMap( ((SuccessMessage) t).meta() ), equalTo( metadata ) );
                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "SUCCESS" );
            }
        };
    }

    public static Matcher<ResponseMessage> msgSuccess( final Matcher<Map<? extends String,?>> matcher )
    {
        return new TypeSafeMatcher<ResponseMessage>()
        {
            @Override
            protected boolean matchesSafely( ResponseMessage t )
            {
                assertThat( t, instanceOf( SuccessMessage.class ) );
                Map<String,Object> actual = toRawMap( ((SuccessMessage) t).meta() );
                assertThat( actual, matcher );
                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "SUCCESS" );
            }
        };
    }

    public static Matcher<ResponseMessage> msgSuccess()
    {
        return new TypeSafeMatcher<ResponseMessage>()
        {
            @Override
            protected boolean matchesSafely( ResponseMessage t )
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

    public static Matcher<ResponseMessage> msgIgnored()
    {
        return new TypeSafeMatcher<ResponseMessage>()
        {
            @Override
            protected boolean matchesSafely( ResponseMessage t )
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

    public static Matcher<ResponseMessage> msgFailure( final Status status, final String message )
    {
        return new TypeSafeMatcher<ResponseMessage>()
        {
            @Override
            protected boolean matchesSafely( ResponseMessage t )
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

    public static Matcher<ResponseMessage> msgRecord( final Matcher<QueryResult.Record> matcher )
    {
        return new TypeSafeMatcher<ResponseMessage>()
        {
            @Override
            protected boolean matchesSafely( ResponseMessage t )
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

    public static byte[] serialize( Neo4jPack neo4jPack, RequestMessage... messages ) throws IOException
    {
        RecordingByteChannel rawData = new RecordingByteChannel();
        Neo4jPack.Packer packer = neo4jPack.newPacker( new BufferedChannelOutput( rawData ) );
        BoltRequestMessageWriter writer = new BoltRequestMessageWriter( packer, NO_BOUNDARY_HOOK );

        for ( RequestMessage message : messages )
        {
            writer.write( message );
        }
        writer.flush();

        return rawData.getBytes();
    }

    public static byte[] serialize( Neo4jPack neo4jPack, ResponseMessage... messages ) throws IOException
    {
        RecordingByteChannel rawData = new RecordingByteChannel();
        Neo4jPack.Packer packer = neo4jPack.newPacker( new BufferedChannelOutput( rawData ) );
        BoltResponseMessageWriter writer = new BoltResponseMessageWriter( packer, NO_BOUNDARY_HOOK,
                NullBoltMessageLogger.getInstance() );

        for ( ResponseMessage message : messages )
        {
            message.dispatch( writer );
        }
        writer.flush();

        return rawData.getBytes();
    }

    public static ResponseMessage responseMessage( Neo4jPack neo4jPack, byte[] bytes ) throws IOException
    {
        BoltResponseMessageReader unpacker = responseReader( neo4jPack, bytes );
        BoltResponseMessageRecorder consumer = new BoltResponseMessageRecorder();

        try
        {
            unpacker.read( consumer );
            return consumer.asList().get( 0 );
        }
        catch ( Throwable e )
        {
            throw new IOException( "Failed to deserialize response, '" + e.getMessage() + "'.\n" +
                                   "Raw data: \n" + HexPrinter.hex( bytes ), e );
        }
    }

    private static BoltResponseMessageReader responseReader( Neo4jPack neo4jPack, byte[] bytes )
    {
        BufferedChannelInput input = new BufferedChannelInput( 128 );
        input.reset( new ArrayByteChannel( bytes ) );
        return new BoltResponseMessageReader( neo4jPack.newUnpacker( input ) );
    }

}
