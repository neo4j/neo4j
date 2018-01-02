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

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.locks.LockSupport;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Base functionality for having a companion thread reading ahead, prefetching.
 */
public abstract class ThreadAhead extends Thread implements Closeable
{
    // A "long" time to wait is OK since these two threads: the owner and the read-ahead thread
    // notifies/unparks each other when it's time to continue on anyways
    private static final long PARK_TIME = MILLISECONDS.toNanos( 100 );

    private final Thread owner;
    private volatile boolean hasReadAhead;
    private volatile boolean closed;
    private volatile boolean eof;
    private volatile IOException ioException;
    private final Closeable actual;

    protected ThreadAhead( Closeable actual )
    {
        this.actual = actual;
        setName( getClass().getSimpleName() + " for " + actual );
        this.owner = Thread.currentThread();
        setDaemon( true );
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

    protected void waitUntilReadAhead() throws IOException
    {
        assertHealthy();
        while ( !hasReadAhead )
        {
            parkAWhile();
            assertHealthy();
        }
    }

    protected void assertHealthy() throws IOException
    {
        if ( ioException != null )
        {
            throw new IOException( "Error occured in read-ahead thread", ioException );
        }
    }

    protected void parkAWhile()
    {
        LockSupport.parkNanos( PARK_TIME );
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
                    if ( !readAhead() )
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

    protected abstract boolean readAhead() throws IOException;

    protected void pokeReader()
    {
        // wake up the reader... there's stuff to do, data to read
        hasReadAhead = false;
        LockSupport.unpark( this );
    }
}
