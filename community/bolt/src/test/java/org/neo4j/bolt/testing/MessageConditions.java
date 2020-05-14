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
package org.neo4j.bolt.testing;

import org.assertj.core.api.Condition;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.neo4j.bolt.messaging.BoltRequestMessageWriter;
import org.neo4j.bolt.messaging.BoltResponseMessageReader;
import org.neo4j.bolt.messaging.BoltResponseMessageRecorder;
import org.neo4j.bolt.messaging.BoltResponseMessageWriter;
import org.neo4j.bolt.messaging.RecordingByteChannel;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.messaging.ResponseMessage;
import org.neo4j.bolt.packstream.BufferedChannelInput;
import org.neo4j.bolt.packstream.BufferedChannelOutput;
import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.v3.messaging.BoltResponseMessageWriterV3;
import org.neo4j.bolt.v3.messaging.response.FailureMessage;
import org.neo4j.bolt.v3.messaging.response.IgnoredMessage;
import org.neo4j.bolt.v3.messaging.response.RecordMessage;
import org.neo4j.bolt.v3.messaging.response.SuccessMessage;
import org.neo4j.bolt.v4.BoltRequestMessageWriterV4;
import org.neo4j.common.HexPrinter;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.BaseToObjectValueWriter;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.virtual.MapValue;

import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings( "unchecked" )
public class MessageConditions
{
    private MessageConditions()
    {
    }

    private static Map<String,Object> toRawMap( MapValue mapValue )
    {
        Deserializer deserializer = new Deserializer();
        Map<String,Object> map = new HashMap<>( mapValue.size() );
        mapValue.foreach( ( key, value ) ->
        {
            value.writeTo( deserializer );
            map.put( key, deserializer.value() );
        } );

        return map;
    }

    private static class Deserializer extends BaseToObjectValueWriter<RuntimeException>
    {

        @Override
        protected Node newNodeEntityById( long id )
        {
            return null;
        }

        @Override
        protected Relationship newRelationshipEntityById( long id )
        {
            return null;
        }

        @Override
        protected Point newPoint( CoordinateReferenceSystem crs, double[] coordinate )
        {
            return null;
        }
    }

    public static Condition<List<ResponseMessage>> equalsMessages( final Condition<ResponseMessage>... conditions )
    {
        return new Condition<>( messages ->
        {
            if ( conditions.length != messages.size() )
            {
                return false;
            }
            for ( int i = 0; i < conditions.length; i++ )
            {
                if ( !conditions[i].matches( messages.get( i ) ) )
                {
                    return false;
                }
            }
            return true;
        }, "MessageList" + Arrays.asList( conditions ) + "]" );
    }

    public static Consumer<ResponseMessage> hasNotification( Notification notification )
    {
        return message ->
        {
            assertThat( message ).isInstanceOf( SuccessMessage.class );
            Map<String,Object> meta = toRawMap( ((SuccessMessage) message).meta() );

            assertThat( meta ).containsKey( "notifications" );
            Set<Notification> notifications =
                    ((List<Map<String,Object>>) meta.get( "notifications" )).stream().map( TestNotification::fromMap ).collect( toSet() );

            assertThat( notifications ).contains( notification );
        };
    }

    public static <T extends ResponseMessage> Consumer<T> msgSuccess( final Map<String,Object> metadata )
    {
        return message ->
        {
            assertThat( message ).isInstanceOf( SuccessMessage.class );
            assertThat( toRawMap( ((SuccessMessage) message).meta() ) ).isEqualTo( metadata );
        };
    }

    public static <T extends ResponseMessage> Consumer<T> msgSuccess( final Consumer<Map<String,Object>> condition )
    {
        return message ->
        {
            assertThat( message ).isInstanceOf( SuccessMessage.class );
            Map<String,Object> actual = toRawMap( ((SuccessMessage) message).meta() );
            assertThat( actual ).satisfies( condition );
        };
    }

    public static Consumer<ResponseMessage> msgSuccess()
    {
        return message -> assertThat( message ).isInstanceOf( SuccessMessage.class );
    }

    public static Consumer<ResponseMessage> msgIgnored()
    {
        return message -> assertThat( message ).isInstanceOf( IgnoredMessage.class );
    }

    public static Consumer<ResponseMessage> msgFailure()
    {
        return message -> assertThat( message ).isInstanceOf( FailureMessage.class );
    }

    public static Consumer<ResponseMessage> msgFailure( final Status status, final String message )
    {
        return response ->
        {
            assertThat( response ).isInstanceOf( FailureMessage.class );
            FailureMessage msg = (FailureMessage) response;
            assertThat( msg.status() ).isEqualTo( status );
            assertThat( msg.message() ).contains( message );
        };
    }

    public static Consumer<ResponseMessage> msgRecord( final Condition<AnyValue[]> condition )
    {
        return message ->
        {
            assertThat( message ).isInstanceOf( RecordMessage.class );
            RecordMessage msg = (RecordMessage) message;
            assertThat( msg.fields() ).satisfies( condition );
        };
    }

    /**
     * Validates both cases and fails only if neither of them succeed (Used because fabric returns a more specific error status in one case)
     */
    public static Consumer<ResponseMessage> either( final Consumer<ResponseMessage> caseA, final Consumer<ResponseMessage> caseB )
    {
        return message ->
        {
            AssertionError errorA = null;
            AssertionError errorB = null;
            try
            {
                caseA.accept( message );
            }
            catch ( AssertionError e )
            {
                errorA = e;
            }
            try
            {
                caseB.accept( message );
            }
            catch ( AssertionError e )
            {
                errorB = e;
            }
            if ( errorA != null && errorB != null )
            {
                var err = new AssertionError( "Neither case matched" );
                err.addSuppressed( errorA );
                err.addSuppressed( errorB );
                throw err;
            }
        };
    }

    public static byte[] serialize( Neo4jPack neo4jPack, RequestMessage... messages ) throws IOException
    {
        RecordingByteChannel rawData = new RecordingByteChannel();
        Neo4jPack.Packer packer = neo4jPack.newPacker( new BufferedChannelOutput( rawData ) );
        BoltRequestMessageWriter writer = new BoltRequestMessageWriterV4( packer );

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
        BufferedChannelOutput output = new BufferedChannelOutput( rawData );
        BoltResponseMessageWriter writer = new BoltResponseMessageWriterV3( neo4jPack::newPacker, output, NullLogService.getInstance() );

        for ( ResponseMessage message : messages )
        {
            writer.write( message );
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
