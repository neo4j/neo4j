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
package org.neo4j.io.pagecache;

import org.hamcrest.Matcher;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.neo4j.io.pagecache.tracing.EvictionEvent;
import org.neo4j.io.pagecache.tracing.EvictionRunEvent;
import org.neo4j.io.pagecache.tracing.FlushEventOpportunity;
import org.neo4j.io.pagecache.tracing.MajorFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageFaultEvent;
import org.neo4j.io.pagecache.tracing.PinEvent;

public class RecordingPageCacheTracer implements PageCacheTracer
{
    private final BlockingQueue<Event> record = new LinkedBlockingQueue<>();
    private CountDownLatch trapLatch;
    private Matcher<? extends Event> trap;

    public void pageFaulted( long filePageId, PageSwapper swapper )
    {
        Fault event = new Fault( swapper, filePageId );
        record.add( event );
        trip( event );
    }

    public void evicted( long filePageId, PageSwapper swapper )
    {
        Evict event = new Evict( swapper, filePageId );
        record.add( event );
        trip( event );
    }

    @Override
    public void mappedFile( File file )
    {
        // we currently do not record these
    }

    @Override
    public void unmappedFile( File file )
    {
        // we currently do not record these
    }

    @Override
    public EvictionRunEvent beginPageEvictions( int pageCountToEvict )
    {
        return new EvictionRunEvent()
        {
            @Override
            public EvictionEvent beginEviction()
            {
                return new RecordingEvictionEvent();
            }

            @Override
            public void close()
            {
            }
        };
    }

    @Override
    public PinEvent beginPin( boolean exclusiveLock, final long filePageId, final PageSwapper swapper )
    {
        return new PinEvent()
        {
            @Override
            public void setCachePageId( int cachePageId )
            {
            }

            @Override
            public PageFaultEvent beginPageFault()
            {
                return new PageFaultEvent()
                {
                    @Override
                    public void addBytesRead( long bytes )
                    {
                    }

                    @Override
                    public void done()
                    {
                        pageFaulted( filePageId, swapper );
                    }

                    @Override
                    public void done( Throwable throwable )
                    {
                    }

                    @Override
                    public EvictionEvent beginEviction()
                    {
                        return new RecordingEvictionEvent();
                    }

                    @Override
                    public void setCachePageId( int cachePageId )
                    {
                    }
                };
            }

            @Override
            public void done()
            {
            }
        };
    }

    @Override
    public MajorFlushEvent beginFileFlush( PageSwapper swapper )
    {
        return MajorFlushEvent.NULL;
    }

    @Override
    public MajorFlushEvent beginCacheFlush()
    {
        return MajorFlushEvent.NULL;
    }

    @Override
    public long countFaults()
    {
        return 0;
    }

    @Override
    public long countEvictions()
    {
        return 0;
    }

    @Override
    public long countPins()
    {
        return 0;
    }

    @Override
    public long countUnpins()
    {
        return 0;
    }

    @Override
    public long countFlushes()
    {
        return 0;
    }

    @Override
    public long countBytesRead()
    {
        return 0;
    }

    @Override
    public long countBytesWritten()
    {
        return 0;
    }

    @Override
    public long countFilesMapped()
    {
        return 0;
    }

    @Override
    public long countFilesUnmapped()
    {
        return 0;
    }

    @Override
    public long countEvictionExceptions()
    {
        return 0;
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

    private void trip( Event event )
    {
        Matcher<? extends Event> theTrap;
        CountDownLatch theTrapLatch;

        // The synchronized block is in here, so we don't risk calling await on
        // the trapLatch while holding the monitor lock.
        synchronized ( this )
        {
            theTrap = trap;
            theTrapLatch = trapLatch;
        }

        if ( theTrap != null && theTrap.matches( event ) )
        {
            try
            {
                theTrapLatch.await();
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                throw new RuntimeException( "Unexpected interrupt in RecordingMonitor", e );
            }
        }
    }

    public static abstract class Event
    {
        public final PageSwapper io;
        public final long pageId;

        public Event( PageSwapper io, long pageId )
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

    public static class Fault extends Event
    {
        public Fault( PageSwapper io, long pageId )
        {
            super( io, pageId );
        }
    }

    public static class Evict extends Event
    {
        public Evict( PageSwapper io, long pageId )
        {
            super( io, pageId );
        }
    }

    private class RecordingEvictionEvent implements EvictionEvent
    {
        private long filePageId;
        private PageSwapper swapper;

        @Override
        public void setFilePageId( long filePageId )
        {

            this.filePageId = filePageId;
        }

        @Override
        public void setSwapper( PageSwapper swapper )
        {
            this.swapper = swapper;
        }

        @Override
        public FlushEventOpportunity flushEventOpportunity()
        {
            return FlushEventOpportunity.NULL;
        }

        @Override
        public void threwException( IOException exception )
        {
        }

        @Override
        public void setCachePageId( int cachePageId )
        {
        }

        @Override
        public void close()
        {
            evicted( filePageId, swapper );
        }
    }
}
