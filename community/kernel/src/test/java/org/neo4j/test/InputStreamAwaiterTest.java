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
package org.neo4j.test;

import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;

public class InputStreamAwaiterTest
{
    @Test
    public void shouldWaitForALineWithoutBlocking() throws Exception
    {
        // given
        ArtificialClock clock = new ArtificialClock();
        InputStream inputStream = spy( new MockInputStream( clock.progressor( 5, TimeUnit.MILLISECONDS ),
                                                            lines( "important message" ) ) );
        InputStreamAwaiter awaiter = new InputStreamAwaiter( clock, inputStream );

        // when
        awaiter.awaitLine( "important message", 5, TimeUnit.SECONDS );
    }

    @Test
    public void shouldTimeoutWhenDifferentContentProvided() throws Exception
    {
        // given
        ArtificialClock clock = new ArtificialClock();
        InputStream inputStream = spy( new MockInputStream( clock.progressor( 1, TimeUnit.SECONDS ),
                                                            lines( "different content" ),
                                                            lines( "different message" ) ) );
        InputStreamAwaiter awaiter = new InputStreamAwaiter( clock, inputStream );

        // when
        try
        {
            awaiter.awaitLine( "important message", 5, TimeUnit.SECONDS );
            fail( "should have thrown exception" );
        }
        // then
        catch ( TimeoutException e )
        {
            // ok
        }
    }

    @Test
    public void shouldTimeoutWhenNoContentProvided() throws Exception
    {
        // given
        ArtificialClock clock = new ArtificialClock();
        InputStream inputStream = spy( new MockInputStream( clock.progressor( 1, TimeUnit.SECONDS ) ) );
        InputStreamAwaiter awaiter = new InputStreamAwaiter( clock, inputStream );

        // when
        try
        {
            awaiter.awaitLine( "important message", 5, TimeUnit.SECONDS );
            fail( "should have thrown exception" );
        }
        // then
        catch ( TimeoutException e )
        {
            // ok
        }
    }

    private static String lines( String... lines )
    {
        StringBuilder result = new StringBuilder();
        for ( String line : lines )
        {
            result.append( line ).append( System.lineSeparator() );
        }
        return result.toString();
    }

    private static class MockInputStream extends InputStream
    {
        private final byte[][] chunks;
        private final ArtificialClock.Progressor progressor;
        private int chunk = 0;

        MockInputStream( ArtificialClock.Progressor progressor, String... chunks )
        {
            this.progressor = progressor;
            this.chunks = new byte[chunks.length][];
            for ( int i = 0; i < chunks.length; i++ )
            {
                this.chunks[i] = chunks[i].getBytes();
            }
        }

        @Override
        public int available() throws IOException
        {
            progressor.tick();
            if ( chunk >= chunks.length )
            {
                return 0;
            }
            return chunks[chunk].length;
        }

        @Override
        public int read( byte[] target ) throws IOException
        {
            if ( chunk >= chunks.length )
            {
                return 0;
            }
            byte[] source = chunks[chunk++];
            System.arraycopy( source, 0, target, 0, source.length );
            return source.length;
        }

        @Override
        public int read() throws IOException
        {
            throw new UnsupportedOperationException();
        }
    }
}
