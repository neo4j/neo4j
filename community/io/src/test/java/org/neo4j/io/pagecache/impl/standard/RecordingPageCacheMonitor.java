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
package org.neo4j.io.pagecache.impl.standard;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.hamcrest.Matcher;

import org.neo4j.io.pagecache.PageCacheMonitor;
import org.neo4j.io.pagecache.PageLock;

public class RecordingPageCacheMonitor implements PageCacheMonitor
{
    private final BlockingQueue<Event> record = new LinkedBlockingQueue<>();
    private CountDownLatch trapLatch;
    private Matcher<? extends Event> trap;

    @Override
    public void pageFault( long pageId, PageIO io )
    {
        Fault event = new Fault( io, pageId );
        record.add( event );
        trip( event );
    }

    @Override
    public void evict( long pageId, PageIO io )
    {
        Evict event = new Evict( io, pageId );
        record.add( event );
        trip( event );
    }

    @Override
    public void pin( PageLock lock, long pageId, PageIO io )
    {
        // we currently do not record these
    }

    @Override
    public void unpin( PageLock lock, long pageId, PageIO io )
    {
        // we currently do not record these
    }

    public <T extends Event> T observe( Class<T> type ) throws InterruptedException
    {
        return type.cast( record.take() );
    }

    public <T extends Event> T tryObserve( Class<T> type )
    {
        return type.cast( record.poll() );
    }

    /**
     * Set a trap for the eviction thread, and return a CountDownLatch with a counter set to 1.
     * When the eviction thread performs the given trap-event, it will block on the latch after
     * making the event observable.
     */
    public synchronized CountDownLatch trap( Matcher<? extends Event> trap )
    {
        assert trap != null;
        trapLatch = new CountDownLatch( 1 );
        this.trap = trap;
        return trapLatch;
    }

    private synchronized void trip( Event event )
    {

        if ( trap != null && trap.matches( event ) )
        {
            try
            {
                trapLatch.await();
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                throw new RuntimeException( "Unexpected interrupt in RecordingMonitor", e );
            }
        }
    }

    static abstract class Event
    {
        public final PageIO io;
        public final long pageId;

        Event( PageIO io, long pageId )
        {
            this.io = io;
            this.pageId = pageId;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            Event event = (Event) o;

            return pageId == event.pageId && !(io != null ? !io.equals( event.io ) : event.io != null);

        }

        @Override
        public int hashCode()
        {
            int result = io != null ? io.hashCode() : 0;
            result = 31 * result + (int) (pageId ^ (pageId >>> 32));
            return result;
        }

        @Override
        public String toString()
        {
            return String.format( "%s{io=%s, pageId=%s}",
                    getClass().getSimpleName(), io, pageId );
        }
    }

    static class Fault extends Event
    {
        Fault( PageIO io, long pageId )
        {
            super( io, pageId );
        }
    }

    static class Evict extends Event
    {
        Evict( PageIO io, long pageId )
        {
            super( io, pageId );
        }
    }
}
