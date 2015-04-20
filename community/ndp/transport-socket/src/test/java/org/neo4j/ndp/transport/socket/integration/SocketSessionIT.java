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
package org.neo4j.ndp.transport.socket.integration;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import org.neo4j.ndp.messaging.v1.message.Message;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.ndp.messaging.v1.message.Messages.pullAll;
import static org.neo4j.ndp.messaging.v1.message.Messages.run;
import static org.neo4j.ndp.messaging.v1.util.MessageMatchers.message;
import static org.neo4j.ndp.messaging.v1.util.MessageMatchers.msgRecord;
import static org.neo4j.ndp.messaging.v1.util.MessageMatchers.msgSuccess;
import static org.neo4j.ndp.messaging.v1.util.MessageMatchers.serialize;
import static org.neo4j.runtime.internal.runner.StreamMatchers.eqRecord;

public class SocketSessionIT
{
    @Rule
    public Neo4jWithSocket server = new Neo4jWithSocket();

    private static byte[] chunk( Message... messages ) throws IOException
    {
        return chunk( 32, messages );
    }

    private static byte[] chunk( int chunkSize, Message... messages ) throws IOException
    {
        ByteBuffer output = ByteBuffer.allocate( 1024 ).order( ByteOrder.BIG_ENDIAN );

        for ( Message message : messages )
        {
            byte[] wholeMessage = serialize( message );
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

    @Test
    public void shouldNegotiateProtocolVersion() throws Throwable
    {
        // Given
        NDPConn client = new NDPConn();

        // When
        client.connect( server.address() )
                .send( acceptedVersions( 1, 0, 0, 0 ) );

        // Then
        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
    }

    @Test
    public void shouldReturnNilOnNoApplicableVersion() throws Throwable
    {
        // Given
        NDPConn client = new NDPConn();

        // When
        client.connect( server.address() )
                .send( acceptedVersions( 1337, 0, 0, 0 ) );

        // Then
        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 0} ) );
    }

    @Test
    public void shouldRunSimpleStatement() throws Throwable
    {
        // Given
        NDPConn client = new NDPConn();

        // When
        client.connect( server.address() )
                .send( acceptedVersions( 1, 0, 0, 0 ) )
                .send( chunk(
                        run( "UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared" ),
                        pullAll() ) );

        // Then
        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyRecieves(
                msgSuccess( map( "fields", asList( "a", "a_squared" ) ) ),
                msgRecord( eqRecord( equalTo( 1l ), equalTo( 1l ) ) ),
                msgRecord( eqRecord( equalTo( 2l ), equalTo( 4l ) ) ),
                msgRecord( eqRecord( equalTo( 3l ), equalTo( 9l ) ) ),
                msgSuccess() ) );
    }

    private byte[] acceptedVersions( long option1, long option2, long option3, long option4 )
    {
        ByteBuffer bb = ByteBuffer.allocate( 4 * 4 ).order( BIG_ENDIAN );
        bb.putInt( (int) option1 );
        bb.putInt( (int) option2 );
        bb.putInt( (int) option3 );
        bb.putInt( (int) option4 );
        return bb.array();
    }

    private Matcher<NDPConn> eventuallyRecieves( final Matcher<Message>... messages )
    {
        return new TypeSafeMatcher<NDPConn>()
        {
            @Override
            protected boolean matchesSafely( NDPConn conn )
            {
                try
                {
                    int messageNo = 0;
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    while ( messageNo < messages.length )
                    {
                        int size = recvChunkHeader( conn );

                        if ( size > 0 )
                        {
                            baos.write( conn.recv( size ) );
                        }
                        else
                        {
                            assertThat( message( baos.toByteArray() ), messages[messageNo] );
                            baos = new ByteArrayOutputStream();
                            messageNo++;
                        }
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

    private int recvChunkHeader( NDPConn conn ) throws IOException, InterruptedException
    {
        byte[] raw = conn.recv( 2 );
        return (raw[0] << 8 | raw[1]) & 0xffff;
    }

    private Matcher<NDPConn> eventuallyRecieves( final byte[] expected )
    {
        return new TypeSafeMatcher<NDPConn>()
        {
            @Override
            protected boolean matchesSafely( NDPConn item )
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
}
