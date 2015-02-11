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
package org.neo4j.csv.reader;

import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.assertEquals;

public class ThreadAheadReadableTest
{
    @Test
    public void shouldReadAhead() throws Exception
    {
        // GIVEN
        MockedReader actual = new MockedReader( 20 );
        int bufferSize = 10;
        CharReadable aheadReader = ThreadAheadReadable.threadAhead( actual, bufferSize );

        // WHEN starting it up it should read and fill the buffer to the brim
        assertEquals( bufferSize, actual.awaitCompletedReadAttempts( 1 ) );

        // WHEN we grab a couple of bytes off of the actual reader, the read-ahead reader should immediately
        // fill those bytes up with new data.
        assertEquals( 7, aheadReader.read( new char[7], 0, 7 ) );
        assertEquals( bufferSize+7, actual.awaitCompletedReadAttempts( 2 ) );

        // WHEN reading a bit more
        assertEquals( 10, aheadReader.read( new char[10], 0, 10 ) );
        assertEquals( 20, actual.awaitCompletedReadAttempts( 3 ) );

        // WHEN reaching the end
        assertEquals( 3, aheadReader.read( new char[5], 0, 5 /*anything more than 3*/ ) );
        assertEquals( 20, actual.awaitCompletedReadAttempts( 4 ) );

        // THEN we should have reached the end
        assertEquals( -1, aheadReader.read( new char[2], 0, 2 ) );
    }

    @Test
    public void shouldHandleReadAheadEmptyData() throws Exception
    {
        // GIVEN
        MockedReader actual = new MockedReader( 0 );
        CharReadable aheadReadable = ThreadAheadReadable.threadAhead( actual, 10 );

        // WHEN
        actual.awaitCompletedReadAttempts( 1 );

        // THEN
        assertEquals( -1, aheadReadable.read( new char[10], 0, 10 ) );
    }

    private static class MockedReader extends CharReadable.Adapter
    {
        private int bytesRead;
        private volatile int readsCompleted;
        private final int length;

        public MockedReader( int length )
        {
            this.length = length;
        }

        @Override
        public int read( char[] buffer, int offset, int len ) throws IOException
        {
            try
            {
                if ( bytesRead == length )
                {   // eof
                    return -1;
                }

                int count = 0;
                while ( count < len && bytesRead < length )
                {
                    buffer[offset + count++] = (char) bytesRead++;
                }
                return count;
            }
            finally
            {
                readsCompleted++;
            }
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
            return bytesRead;
        }
    }
}
