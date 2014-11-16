/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.io.Closeable;
import java.io.IOException;
import java.nio.CharBuffer;
import java.util.concurrent.locks.LockSupport;

import static java.lang.Math.min;
import static java.lang.System.arraycopy;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Like an ordinary {@link CharReadable}, it's just that the reading happens in a separate thread, so when
 * a consumer wants to {@link #read(CharBuffer)} more data it's already available, merely a memcopy away.
 */
public class ThreadAheadReadable extends Thread implements CharReadable, Closeable
{
    private static final long PARK_TIME = MILLISECONDS.toNanos( 100 );

    private final CharReadable actual;
    private final Thread owner;
    private final char[] readAheadArray;
    private final CharBuffer readAheadBuffer;
    private volatile boolean hasReadAhead;
    private volatile boolean closed;
    private volatile boolean eof;
    private volatile IOException ioException;

    private ThreadAheadReadable( CharReadable actual, int bufferSize  )
    {
        this.actual = actual;
        this.owner = Thread.currentThread();
        this.readAheadArray = new char[bufferSize];
        this.readAheadBuffer = CharBuffer.wrap( readAheadArray );
        this.readAheadBuffer.position( bufferSize );
        start();
    }

    /**
     * The one calling read doesn't actually read, since reading is up to this guy. Instead the caller just
     * waits for this thread to have fully read the next buffer.
     */
    @Override
    public int read( char[] buffer, int offset, int length ) throws IOException
    {
        // are we still healthy and all that?
        assertHealthy();

        // wait until thread has made data available
        while ( !hasReadAhead )
        {
            parkAWhile();
            assertHealthy();
        }

        if ( eof )
        {
            return -1;
        }

        // copy data from the read ahead buffer into the target buffer
        int bytesToCopy = min( readAheadBuffer.remaining(), length );
        arraycopy( readAheadArray, readAheadBuffer.position(), buffer, offset, bytesToCopy );
        readAheadBuffer.position( readAheadBuffer.position() + bytesToCopy );

        // wake up the reader... there's stuff to do, data to read
        hasReadAhead = false;
        LockSupport.unpark( this );
        return bytesToCopy == 0 ? -1 : bytesToCopy;
    }

    private void assertHealthy() throws IOException
    {
        if ( ioException != null )
        {
            throw new IOException( "Error occured in read-ahead thread", ioException );
        }
    }

    private void parkAWhile()
    {
        LockSupport.parkNanos( PARK_TIME );
    }

    @Override
    public void close() throws IOException
    {
        closed = true;
        try
        {
            join();
        }
        catch ( InterruptedException e )
        {
            throw new IOException( e );
        }
        finally
        {
            actual.close();
        }
    }

    @Override
    public void run()
    {
        while ( !closed )
        {
            if ( hasReadAhead || eof )
            {   // We have already read ahead, sleep a little
                parkAWhile();
            }
            else
            {   // We haven't read ahead, or the data we read ahead have been consumed
                try
                {
                    readAheadBuffer.compact();
                    int read = actual.read( readAheadArray, readAheadBuffer.position(), readAheadBuffer.remaining() );
                    if ( read == -1 )
                    {
                        eof = true;
                        read = 0;
                    }
                    readAheadBuffer.limit( readAheadBuffer.position() + read );
                    readAheadBuffer.position( 0 );
                    hasReadAhead = true;
                    LockSupport.unpark( owner );
                }
                catch ( IOException e )
                {
                    ioException = e;
                    closed = true;
                }
                catch ( Throwable e )
                {
                    ioException = new IOException( e );
                    closed = true;
                }
            }
        }
    }

    public static CharReadable threadAhead( CharReadable actual, int bufferSize )
    {
        return new ThreadAheadReadable( actual, bufferSize );
    }
}
