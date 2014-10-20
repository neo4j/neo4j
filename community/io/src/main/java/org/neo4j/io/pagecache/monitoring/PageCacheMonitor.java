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

public interface PageCacheMonitor
{
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

    public static final FlushEventOpportunity NULL_FLUSH_EVENT_OPPORTUNITY = new FlushEventOpportunity()
    {
        @Override
        public FlushEvent beginFlush( long filePageId, int cachePageId, PageSwapper swapper )
        {
            return NULL_FLUSH_EVENT;
        }
    };

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
    };

    public void mappedFile( File file );

    public void unmappedFile( File file );

    /**
     * A background eviction has begun. Called from the background eviction thread.
     * 
     * This call will be paired with a following PageCacheMonitor#endPageEviction call.
     *
     * The method returns an EvictionRunEvent to represent the event of this eviction run.
     **/
    public EvictionRunEvent beginPageEvictions( int pageCountToEvict );

    public PinEvent beginPin( boolean exclusiveLock, long filePageId, PageSwapper swapper );

    public MajorFlushEvent beginFileFlush( PageSwapper swapper );

    public MajorFlushEvent beginCacheFlush();
}
