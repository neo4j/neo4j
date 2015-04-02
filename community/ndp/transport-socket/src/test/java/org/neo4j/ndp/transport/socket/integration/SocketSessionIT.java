/**
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.neo4j.ndp.messaging.v1.PackStreamMessageFormatV1;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static org.hamcrest.MatcherAssert.assertThat;

public class SocketSessionIT
{
    @Rule public Neo4jWithSocket server = new Neo4jWithSocket();

    @Test
    public void shouldNegotiateProtocolVersion() throws Throwable
    {
        // Given
        NDPConn client = new NDPConn(new PackStreamMessageFormatV1());

        // When
        client.connect( server.address() )
              .send( acceptedVersions( 1, 0, 0, 0 ) );

        // Then
        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ));
    }

    @Test
    public void shouldReturnNilOnNoApplicableVersion() throws Throwable
    {
        // Given
        NDPConn client = new NDPConn(new PackStreamMessageFormatV1());

        // When
        client.connect( server.address() )
              .send( acceptedVersions( 1337, 0, 0, 0 ) );

        // Then
        assertThat( client, eventuallyRecieves( new byte[]{ 0, 0, 0, 0 } ));
    }

    private byte[] acceptedVersions( long option1, long option2, long option3, long option4 )
    {
        ByteBuffer bb = ByteBuffer.allocate( 4 * 4 ).order( BIG_ENDIAN );
        bb.putInt( (int)option1 );
        bb.putInt( (int)option2 );
        bb.putInt( (int)option3 );
        bb.putInt( (int)option4 );
        return bb.array();
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
                    return Arrays.equals( item.recv( expected.length ), expected);
                }
                catch ( IOException e )
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
