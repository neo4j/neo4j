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
package org.neo4j.io.pagecache.monitoring;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.pagecache.PageSwapper;

/**
 * A PageCacheMonitor receives a steady stream of events and data about what
 * the page cache is doing. Implementations of this interface should be as
 * efficient as possible, lest they severely slow down the page cache.
 */
public interface PageCacheMonitor
{
    /**
     * A FlushEvent implementation that does nothing.
     */
    public static final FlushEvent NULL_FLUSH_EVENT = new FlushEvent()
    {
        @Override
        public void addBytesWritten( int bytes )
        {
        }

        @Override
        public void done()
        {
        }

        @Override
        public void done( IOException exception )
        {
        }
    };

    /**
     * A FlushEventOpportunity that only returns the NULL_FLUSH_EVENT.
     */
    public static final FlushEventOpportunity NULL_FLUSH_EVENT_OPPORTUNITY = new FlushEventOpportunity()
    {
        @Override
        public FlushEvent beginFlush( long filePageId, int cachePageId, PageSwapper swapper )
        {
            return NULL_FLUSH_EVENT;
        }
    };

    /**
     * A MajorFlushEvent that only returns the NULL_FLUSH_EVENT_OPPORTUNITY.
     */
    public static final MajorFlushEvent NULL_MAJOR_FLUSH_EVENT = new MajorFlushEvent()
    {
        @Override
        public FlushEventOpportunity flushEventOpportunity()
        {
            return NULL_FLUSH_EVENT_OPPORTUNITY;
        }

        @Override
        public void close()
        {
        }
    };

    /**
     * An EvictionEvent that does nothing other than return the NULL_FLUSH_EVENT_OPPORTUNITY.
     */
    public static final EvictionEvent NULL_EVICTION_EVENT = new EvictionEvent()
    {
        @Override
        public void setFilePageId( long filePageId )
        {
        }

        @Override
        public void setSwapper( PageSwapper swapper )
        {
        }

        @Override
        public FlushEventOpportunity flushEventOpportunity()
        {
            return NULL_FLUSH_EVENT_OPPORTUNITY;
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
        }
    };

    /**
     * A PageFaultEvent that does nothing.
     */
    public static final PageFaultEvent NULL_PAGE_FAULT_EVENT = new PageFaultEvent()
    {
        @Override
        public void addBytesRead( int bytes )
        {
        }

        @Override
        public void done()
        {
        }

        @Override
        public void done( Throwable throwable )
        {
        }

        @Override
        public void setCachePageId( int cachePageId )
        {
        }

        @Override
        public void setParked( boolean parked )
        {
        }
    };

    /**
     * A PinEvent that does nothing other than return the NULL_PAGE_FAULT_EVENT.
     */
    public static final PinEvent NULL_PIN_EVENT = new PinEvent()
    {
        @Override
        public void setCachePageId( int cachePageId )
        {
        }

        @Override
        public PageFaultEvent beginPageFault()
        {
            return NULL_PAGE_FAULT_EVENT;
        }

        @Override
        public void done()
        {
        }
    };

    /**
     * An EvictionRunEvent that does nothing other than return the NULL_EVICTION_EVENT.
     */
    public static final EvictionRunEvent NULL_EVICTION_RUN_EVENT = new EvictionRunEvent()
    {

        @Override
        public EvictionEvent beginEviction()
        {
            return NULL_EVICTION_EVENT;
        }

        @Override
        public void close()
        {
        }
    };

    /**
     * A PageCacheMonitor that does nothing other than return the NULL variants of the companion interfaces.
     */
    public static final PageCacheMonitor NULL = new PageCacheMonitor()
    {
        @Override
        public void mappedFile( File file )
        {
        }

        @Override
        public void unmappedFile( File file )
        {
        }

        @Override
        public EvictionRunEvent beginPageEvictions( int pageCountToEvict )
        {
            return NULL_EVICTION_RUN_EVENT;
        }

        @Override
        public PinEvent beginPin( boolean exclusiveLock, long filePageId, PageSwapper swapper )
        {
            return NULL_PIN_EVENT;
        }

        @Override
        public MajorFlushEvent beginFileFlush( PageSwapper swapper )
        {
            return NULL_MAJOR_FLUSH_EVENT;
        }

        @Override
        public MajorFlushEvent beginCacheFlush()
        {
            return NULL_MAJOR_FLUSH_EVENT;
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
    };

    /**
     * The given file has been mapped, where no existing mapping for that file existed.
     */
    public void mappedFile( File file );

    /**
     * The last reference to the given file has been unmapped.
     */
    public void unmappedFile( File file );

    /**
     * A background eviction has begun. Called from the background eviction thread.
     * 
     * This call will be paired with a following PageCacheMonitor#endPageEviction call.
     *
     * The method returns an EvictionRunEvent to represent the event of this eviction run.
     **/
    public EvictionRunEvent beginPageEvictions( int pageCountToEvict );

    /**
     * A page is to be pinned.
     */
    public PinEvent beginPin( boolean exclusiveLock, long filePageId, PageSwapper swapper );

    /**
     * A PagedFile wants to flush all its bound pages.
     */
    public MajorFlushEvent beginFileFlush( PageSwapper swapper );

    /**
     * The PageCache wants to flush all its bound pages.
     */
    public MajorFlushEvent beginCacheFlush();

    /**
     * @return The number of page faults observed thus far.
     */
    public long countFaults();

    /**
     * @return The number of page evictions observed thus far.
     */
    public long countEvictions();

    /**
     * @return The number of page pins observed thus far.
     */
    public long countPins();

    /**
     * @return The number of page unpins observed thus far.
     */
    public long countUnpins();

    /**
     * @return The number of page flushes observed thus far.
     */
    public long countFlushes();

    /**
     * @return The sum total of bytes read in through page faults thus far.
     */
    public long countBytesRead();

    /**
     * @return The sum total of bytes written through flushes thus far.
     */
    public long countBytesWritten();

    /**
     * @return The number of file mappings observed thus far.
     */
    public long countFilesMapped();

    /**
     * @return The number of file unmappings observed thus far.
     */
    public long countFilesUnmapped();

    /**
     * @return The number of page evictions that have thrown exceptions thus far.
     */
    public long countEvictionExceptions();
}
