/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.io.pagecache.tracing;

import java.io.File;

import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.monitoring.PageCacheCounters;

/**
 * A PageCacheTracer receives a steady stream of events and data about what
 * the page cache is doing. Implementations of this interface should be as
 * efficient as possible, lest they severely slow down the page cache.
 */
public interface PageCacheTracer extends PageCacheCounters
{
    /**
     * A PageCacheTracer that does nothing other than return the NULL variants of the companion interfaces.
     */
    PageCacheTracer NULL = new PageCacheTracer()
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
            return EvictionRunEvent.NULL;
        }

        @Override
        public PinEvent beginPin( boolean writeLock, long filePageId, PageSwapper swapper )
        {
            return PinEvent.NULL;
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
        public long faults()
        {
            return 0;
        }

        @Override
        public long evictions()
        {
            return 0;
        }

        @Override
        public long pins()
        {
            return 0;
        }

        @Override
        public long unpins()
        {
            return 0;
        }

        @Override
        public long flushes()
        {
            return 0;
        }

        @Override
        public long bytesRead()
        {
            return 0;
        }

        @Override
        public long bytesWritten()
        {
            return 0;
        }

        @Override
        public long filesMapped()
        {
            return 0;
        }

        @Override
        public long filesUnmapped()
        {
            return 0;
        }

        @Override
        public long evictionExceptions()
        {
            return 0;
        }

        @Override
        public String toString()
        {
            return PageCacheTracer.class.getName() + ".NULL";
        }
    };

    /**
     * The given file has been mapped, where no existing mapping for that file existed.
     */
    void mappedFile( File file );

    /**
     * The last reference to the given file has been unmapped.
     */
    void unmappedFile( File file );

    /**
     * A background eviction has begun. Called from the background eviction thread.
     *
     * This call will be paired with a following PageCacheTracer#endPageEviction call.
     *
     * The method returns an EvictionRunEvent to represent the event of this eviction run.
     **/
    EvictionRunEvent beginPageEvictions( int pageCountToEvict );

    /**
     * A page is to be pinned.
     */
    PinEvent beginPin( boolean writeLock, long filePageId, PageSwapper swapper );

    /**
     * A PagedFile wants to flush all its bound pages.
     */
    MajorFlushEvent beginFileFlush( PageSwapper swapper );

    /**
     * The PageCache wants to flush all its bound pages.
     */
    MajorFlushEvent beginCacheFlush();
}
