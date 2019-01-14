/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * whole page cache is doing. Implementations of this interface should be as
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
        public long hits()
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
        public double hitRatio()
        {
            return 0d;
        }

        @Override
        public double usageRatio()
        {
            return 0d;
        }

        @Override
        public void pins( long pins )
        {
        }

        @Override
        public void unpins( long unpins )
        {
        }

        @Override
        public void hits( long hits )
        {
        }

        @Override
        public void faults( long faults )
        {
        }

        @Override
        public void bytesRead( long bytesRead )
        {
        }

        @Override
        public void evictions( long evictions )
        {
        }

        @Override
        public void evictionExceptions( long evictionExceptions )
        {
        }

        @Override
        public void bytesWritten( long bytesWritten )
        {
        }

        @Override
        public void flushes( long flushes )
        {
        }

        @Override
        public void maxPages( long maxPages )
        {
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
     * A PagedFile wants to flush all its bound pages.
     */
    MajorFlushEvent beginFileFlush( PageSwapper swapper );

    /**
     * The PageCache wants to flush all its bound pages.
     */
    MajorFlushEvent beginCacheFlush();

    /**
     * Report number of observed pins
     * @param pins number of pins
     */
    void pins( long pins );

    /**
     * Report number of observed unpins
     * @param unpins number of unpins
     */
    void unpins( long unpins );

    /**
     * Report number of observer hits
     * @param hits number of hits
     */
    void hits( long hits );

    /**
     * Report number of observed faults
     * @param faults number of faults
     */
    void faults( long faults );

    /**
     * Report number of bytes read
     * @param bytesRead number of read bytes
     */
    void bytesRead( long bytesRead );

    /**
     * Report number of observed evictions
     * @param evictions number of evictions
     */
    void evictions( long evictions );

    /**
     * Report number of eviction exceptions
     * @param evictionExceptions number of eviction exceptions
     */
    void evictionExceptions( long evictionExceptions );

    /**
     * Report number of bytes written
     * @param bytesWritten number of written bytes
     */
    void bytesWritten( long bytesWritten );

    /**
     * Report number of flushes
     * @param flushes number of flushes
     */
    void flushes( long flushes );

    /**
     * Sets the number of available pages.
     * @param maxPages the total number of available pages.
     */
    void maxPages( long maxPages );
}
