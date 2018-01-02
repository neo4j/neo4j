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
package org.neo4j.csv.reader;

import org.junit.Test;

import java.io.CharArrayReader;
import java.io.IOException;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import static java.util.Arrays.copyOfRange;

public class ThreadAheadReadableTest
{
    @Test
    public void shouldReadAhead() throws Exception
    {
        // GIVEN
        TrackingReader actual = new TrackingReader( 23 );
        int bufferSize = 5;
        CharReadable aheadReader = ThreadAheadReadable.threadAhead( actual, bufferSize );
        SectionedCharBuffer buffer = new SectionedCharBuffer( bufferSize );

        // WHEN starting it up it should read and fill the buffer to the brim
        assertEquals( bufferSize, actual.awaitCompletedReadAttempts( 1 ) );

        // WHEN we read one buffer
        int read = 0;
        buffer = aheadReader.read( buffer, buffer.front() );
        assertBuffer( chars( read, bufferSize ), buffer, 0, bufferSize );
        read += buffer.available();

        // and simulate reading all characters, i.e. back section will be empty in the new buffer
        buffer = aheadReader.read( buffer, buffer.front() );
        assertBuffer( chars( read, bufferSize ), buffer, 0, bufferSize );
        read += buffer.available();

        // then simulate reading some characters, i.e. back section will contain some characters
        int keep = 2;
        buffer = aheadReader.read( buffer, buffer.front()-keep );
        assertBuffer( chars( read-keep, bufferSize+keep ), buffer, keep, bufferSize );
        read += buffer.available();

        keep = 3;
        buffer = aheadReader.read( buffer, buffer.front()-keep );
        assertBuffer( chars( read-keep, bufferSize+keep ), buffer, keep, bufferSize );
        read += buffer.available();

        keep = 1;
        buffer = aheadReader.read( buffer, buffer.front()-keep );
        assertEquals( 3, buffer.available() );
        assertBuffer( chars( read-keep, buffer.available()+keep ), buffer, keep, 3 );
        read += buffer.available();
        assertEquals( 23, read );
    }

    @Test
    public void shouldHandleReadAheadEmptyData() throws Exception
    {
        // GIVEN
        TrackingReader actual = new TrackingReader( 0 );
        int bufferSize = 10;
        CharReadable aheadReadable = ThreadAheadReadable.threadAhead( actual, bufferSize );

        // WHEN
        actual.awaitCompletedReadAttempts( 1 );

        // THEN
        SectionedCharBuffer buffer = new SectionedCharBuffer( bufferSize );
        buffer = aheadReadable.read( buffer, buffer.front() );
        assertEquals( buffer.pivot(), buffer.back() );
        assertEquals( buffer.pivot(), buffer.front() );
    }

    private void assertBuffer( char[] expectedChars, SectionedCharBuffer buffer, int charsInBack, int charsInFront )
    {
        assertEquals( buffer.pivot()-charsInBack, buffer.back() );
        assertEquals( buffer.pivot()+charsInFront, buffer.front() );
        assertArrayEquals( expectedChars, copyOfRange( buffer.array(), buffer.back(), buffer.front() ) );
    }

    private static class TrackingReader extends CharReadable.Adapter
    {
        private int bytesRead;
        private volatile int readsCompleted;
        private final CharReadable actual;

        public TrackingReader( int length )
        {
            this.actual = Readables.wrap( new CharArrayReader( chars( 0, length ) ) );
        }

        @Override
        public SectionedCharBuffer read( SectionedCharBuffer buffer, int from ) throws IOException
        {
            try
            {
                return registerBytesRead( actual.read( buffer, from ) );
            }
            finally
            {
                readsCompleted++;
            }
        }

        private SectionedCharBuffer registerBytesRead( SectionedCharBuffer buffer )
        {
            bytesRead += buffer.available();
            return buffer;
        }

        @Override
        public void close() throws IOException
        {   // Nothing to close
        }

        private int awaitCompletedReadAttempts( int ticket )
        {
            while ( readsCompleted < ticket )
            {
                LockSupport.parkNanos( 10_000_000 );
            }
            return bytesRead;
        }

        @Override
        public long position()
        {
            return actual.position();
        }

        @Override
        public String sourceDescription()
        {
            return getClass().getSimpleName();
        }
    }

    private static char[] chars( int start, int length )
    {
        char[] result = new char[length];
        for ( int i = 0; i < length; i++ )
        {
            result[i] = (char) (start+i);
        }
        return result;
    }
}
