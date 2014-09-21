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
package org.neo4j.unsafe.impl.batchimport.input.csv.reader;

import java.io.Closeable;
import java.io.IOException;
import java.nio.CharBuffer;
import java.util.concurrent.locks.LockSupport;

import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Like an ordinary {@link Readable}, it's just that the reading happens in a separate thread, so when
 * a consumer wants to {@link #read(CharBuffer)} more data it's already available, merely a memcopy away.
 */
public class ThreadAheadReadable extends Thread implements Readable, Closeable
{
    private static final long PARK_TIME = NANOSECONDS.toMillis( 100 );

    private final Readable actual;
    private final Thread owner;
    private final CharBuffer readAheadBuffer;
    private volatile boolean hasReadAhead;
    private volatile boolean closed;
    private volatile IOException ioException;

    private ThreadAheadReadable( Readable actual, int bufferSize  )
    {
        this.actual = actual;
        this.owner = Thread.currentThread();
        this.readAheadBuffer = CharBuffer.allocate( bufferSize );
        this.readAheadBuffer.position( bufferSize );
        start();
    }

    /**
     * The one calling read doesn't actually read, since reading is up to this guy. Instead the caller just
     * waits for this thread to have fully read the next buffer.
     */
    @Override
    public int read( CharBuffer target ) throws IOException
    {
        // are we still healthy and all that?
        if ( ioException != null )
        {
            throw new IOException( "IOException occured on read-ahead thread", ioException );
        }

        // wait until thread has made data available
        while ( !hasReadAhead )
        {
            parkAWhile();
        }

        // copy data from the read ahead buffer into the target buffer
        int available = readAheadBuffer.limit();
        int bytesToCopy;
        try
        {
            bytesToCopy = min( available, target.remaining() );
            readAheadBuffer.limit( bytesToCopy );
            target.put( readAheadBuffer );
        }
        finally
        {
            readAheadBuffer.limit( available );
        }

        // Wake up the reader... there's stuff to do, data to read
        hasReadAhead = false;
        LockSupport.unpark( this );
        return bytesToCopy;
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
    }

    @Override
    public void run()
    {
        while ( !closed )
        {
            if ( hasReadAhead )
            {   // We have already read ahead, sleep a little
                parkAWhile();
            }
            else
            {   // We haven't read ahead, or the data we read ahead have been consumed
                try
                {
                    readAheadBuffer.compact();
                    actual.read( readAheadBuffer );
                    readAheadBuffer.flip();
                    hasReadAhead = true;
                    LockSupport.unpark( owner );
                }
                catch ( IOException e )
                {
                    ioException = e;
                    closed = true;
                }
            }
        }
    }

    public static Readable threadAhead( Readable actual, int bufferSize )
    {
        return new ThreadAheadReadable( actual, bufferSize );
    }
}
