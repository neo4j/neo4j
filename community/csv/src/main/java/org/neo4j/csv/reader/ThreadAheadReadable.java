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

import java.io.Closeable;
import java.io.IOException;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.LockSupport;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Like an ordinary {@link CharReadable}, it's just that the reading happens in a separate thread, so when
 * a consumer wants to {@link #read(CharBuffer)} more data it's already available, merely a memcopy away.
 */
public class ThreadAheadReadable extends Thread implements CharReadable, Closeable, SourceMonitor
{
    // A "long" time to wait is OK since these two threads: the owner and the read-ahead thread
    // notifies/unparks each other when it's time to continue on anyways
    private static final long PARK_TIME = MILLISECONDS.toNanos( 100 );

    private final CharReadable actual;
    private final Thread owner;
    private SectionedCharBuffer theOtherBuffer;
    private volatile boolean hasReadAhead;
    private volatile boolean closed;
    private volatile boolean eof;
    private volatile IOException ioException;

    private final List<SourceMonitor> sourceMonitors = new ArrayList<>();
    private String sourceDescription;
    // the variable below is read and changed in both the ahead thread and the caller,
    // but doesn't have to be volatile since it piggy-backs off of hasReadAhead.
    private String newSourceDescription;

    private ThreadAheadReadable( CharReadable actual, int bufferSize )
    {
        super( ThreadAheadReadable.class.getSimpleName() + " for " + actual );

        this.actual = actual;
        this.owner = Thread.currentThread();
        this.theOtherBuffer = new SectionedCharBuffer( bufferSize );
        this.sourceDescription = actual.toString();
        actual.addSourceMonitor( this );
        setDaemon( true );
        start();
    }

    /**
     * The one calling read doesn't actually read, since reading is up to the thread in here.
     * Instead the caller just waits for this thread to have fully read the next buffer and
     * flips over to that buffer, returning it.
     */
    @Override
    public SectionedCharBuffer read( SectionedCharBuffer buffer, int from ) throws IOException
    {
        // are we still healthy and all that?
        assertHealthy();

        // wait until thread has made data available
        while ( !hasReadAhead )
        {
            parkAWhile();
            assertHealthy();
        }

        // flip the buffers
        SectionedCharBuffer resultBuffer = theOtherBuffer;
        buffer.compact( resultBuffer, from );
        theOtherBuffer = buffer;

        // handle source notifications that has happened
        if ( newSourceDescription != null )
        {
            sourceDescription = newSourceDescription;
            // At this point the new source is official, so tell that to our external monitors
            for ( SourceMonitor monitor : sourceMonitors )
            {
                monitor.notify( newSourceDescription );
            }
            newSourceDescription = null;
        }

        // wake up the reader... there's stuff to do, data to read
        hasReadAhead = false;
        LockSupport.unpark( this );
        return resultBuffer;
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
                    theOtherBuffer = actual.read( theOtherBuffer, theOtherBuffer.front() );
                    if ( !theOtherBuffer.hasAvailable() )
                    {
                        eof = true;
                    }
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

    @Override
    public long position()
    {
        return actual.position();
    }

    @Override
    public void notify( String sourceDescription )
    {   // Called when the underlying readable, read by the thread-ahead, changes source
        newSourceDescription = sourceDescription;
    }

    @Override
    public void addSourceMonitor( SourceMonitor sourceMonitor )
    {
        sourceMonitors.add( sourceMonitor );
    }

    @Override
    public String toString()
    {   // Returns the source information of where this reader is perceived to be. The fact that this
        // thing reads ahead should be visible in this description.
        return sourceDescription;
    }

    public static CharReadable threadAhead( CharReadable actual, int bufferSize )
    {
        return new ThreadAheadReadable( actual, bufferSize );
    }
}
