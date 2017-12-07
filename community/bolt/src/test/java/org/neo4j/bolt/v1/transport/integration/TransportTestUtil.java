/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.neo4j.bolt.v1.messaging.message.RequestMessage;
import org.neo4j.bolt.v1.messaging.message.ResponseMessage;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.function.Predicates;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.responseMessage;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.serialize;

@SuppressWarnings( "unchecked" )
public class TransportTestUtil
{
    private TransportTestUtil()
    {
    }

    public static byte[] dechunk( byte[] chunked ) throws IOException
    {
        ByteBuffer in = ByteBuffer.wrap( chunked );
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        while ( in.hasRemaining() )
        {
            int chunkSize = in.getShort();
            if ( chunkSize == 0 )
            {
                continue;
            }

            byte[] chunk = new byte[chunkSize];
            in.get( chunk );
            out.write( chunk );
        }
        return out.toByteArray();
    }

    public static byte[] chunk( RequestMessage... messages ) throws IOException
    {
        return chunk( 32, messages );
    }

    public static byte[] chunk( ResponseMessage... messages ) throws IOException
    {
        return chunk( 32, messages );
    }

    public static byte[] chunk( int chunkSize, RequestMessage... messages ) throws IOException
    {
        byte[][] serializedMessages = new byte[messages.length][];
        for ( int i = 0; i < messages.length; i++ )
        {
            serializedMessages[i] = serialize( messages[i] );
        }
        return chunk( chunkSize, serializedMessages );
    }

    public static byte[] chunk( int chunkSize, ResponseMessage... messages ) throws IOException
    {
        byte[][] serializedMessages = new byte[messages.length][];
        for ( int i = 0; i < messages.length; i++ )
        {
            serializedMessages[i] = serialize( messages[i] );
        }
        return chunk( chunkSize, serializedMessages );
    }

    public static byte[] chunk( int chunkSize, byte[] ... messages )
    {
        ByteBuffer output = ByteBuffer.allocate( 10000 ).order( ByteOrder.BIG_ENDIAN );

        for ( byte[] wholeMessage : messages )
        {
            int left = wholeMessage.length;
            while ( left > 0 )
            {
                int size = Math.min( left, chunkSize );
                output.putShort( (short) size );

                int offset = wholeMessage.length - left;
                output.put( wholeMessage, offset, size );

                left -= size;
            }
            output.putShort( (short) 0 );
        }

        output.flip();

        byte[] arrayOutput = new byte[output.limit()];
        output.get( arrayOutput );
        return arrayOutput;
    }

    public static byte[] acceptedVersions( long option1, long option2, long option3, long option4 )
    {
        ByteBuffer bb = ByteBuffer.allocate( 5 * Integer.BYTES ).order( BIG_ENDIAN );
        bb.putInt( 0x6060B017 );
        bb.putInt( (int) option1 );
        bb.putInt( (int) option2 );
        bb.putInt( (int) option3 );
        bb.putInt( (int) option4 );
        return bb.array();
    }

    public static Matcher<TransportConnection> eventuallyReceives( final Matcher<ResponseMessage>... messages )
    {
        return new TypeSafeMatcher<TransportConnection>()
        {
            @Override
            protected boolean matchesSafely( TransportConnection conn )
            {
                try
                {
                    for ( Matcher<ResponseMessage> matchesMessage : messages )
                    {
                        final ResponseMessage message = receiveOneResponseMessage( conn );
                        assertThat( message, matchesMessage );
                    }
                    return true;
                }
                catch ( Exception e )
                {
                    throw new RuntimeException( e );
                }
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendValueList( "Messages[", ",", "]", messages );
            }
        };
    }

    public static ResponseMessage receiveOneResponseMessage( TransportConnection conn ) throws IOException,
            InterruptedException
    {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        while ( true )
        {
            int size = receiveChunkHeader( conn );

            if ( size > 0 )
            {
                byte[] received = conn.recv( size );
                bytes.write( received );
            }
            else
            {
                return responseMessage( bytes.toByteArray() );
            }
        }
    }

    public static int receiveChunkHeader( TransportConnection conn ) throws IOException, InterruptedException
    {
        byte[] raw = conn.recv( 2 );
        return ((raw[0] & 0xff) << 8 | (raw[1] & 0xff)) & 0xffff;
    }

    public static Matcher<TransportConnection> eventuallyReceives( final byte[] expected )
    {
        return new TypeSafeMatcher<TransportConnection>()
        {
            @Override
            protected boolean matchesSafely( TransportConnection item )
            {
                try
                {
                    return Arrays.equals( item.recv( expected.length ), expected );
                }
                catch ( Exception e )
                {
                    throw new RuntimeException( e );
                }
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendValueList( "RawBytes[", ",", "]", expected );
            }
        };
    }

    public static Matcher<TransportConnection> eventuallyDisconnects()
    {
        return new TypeSafeMatcher<TransportConnection>()
        {
            @Override
            protected boolean matchesSafely( TransportConnection connection )
            {
                BooleanSupplier condition = () ->
                {
                    try
                    {
                        connection.send( new byte[]{0,0});
                        connection.recv( 1 );
                    }
                    catch ( Exception e )
                    {
                        // take an IOException on send/receive as evidence of disconnection
                        return e instanceof IOException;
                    }
                    return false;
                };
                try
                {
                    Predicates.await( condition, 2, TimeUnit.SECONDS );
                    return true;
                }
                catch ( Exception e )
                {
                    return false;
                }
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "Eventually Disconnects" );
            }
        };
    }
}
