/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.io.pagecache.tracing.cursor;

import java.io.IOException;

import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.tracing.EvictionEvent;
import org.neo4j.io.pagecache.tracing.FlushEvent;
import org.neo4j.io.pagecache.tracing.FlushEventOpportunity;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageFaultEvent;
import org.neo4j.io.pagecache.tracing.PinEvent;

public class DefaultPageCursorTracer implements PageCursorTracer
{
    private long totalPins = 0L;
    private long totalUnpins = 0L;
    private long totalFaults = 0L;
    private long totalBytesRead = 0L;
    private long totalBytesWritten = 0L;
    private long totalEvictions = 0L;
    private long totalEvictionExceptions = 0L;
    private long totalFlushes = 0L;

    private long pins;
    private long unpins;
    private long faults;
    private long bytesRead;
    private long bytesWritten;
    private long evictions;
    private long evictionExceptions;
    private long flushes;

    private PageCacheTracer pageCacheTracer = PageCacheTracer.NULL;

    public void init( PageCacheTracer pageCacheTracer )
    {
        this.pageCacheTracer = pageCacheTracer;
    }

    public void reportEvents()
    {
        if (pins > 0)
        {
            pageCacheTracer.pins( pins );
        }
        if (unpins > 0)
        {
            pageCacheTracer.unpins( unpins );
        }
        if ( faults > 0 )
        {
            pageCacheTracer.faults( faults );
        }
        if ( bytesRead > 0 )
        {
            pageCacheTracer.bytesRead( bytesRead );
        }
        if ( evictions > 0 )
        {
            pageCacheTracer.evictions( evictions );
        }
        if ( evictionExceptions > 0 )
        {
            pageCacheTracer.evictionExceptions( evictionExceptions );
        }
        if ( bytesWritten > 0 )
        {
            pageCacheTracer.bytesWritten( bytesWritten );
        }
        if ( flushes > 0 )
        {
            pageCacheTracer.flushes( flushes );
        }
        updateTotals();
    }

    private void updateTotals()
    {
        this.totalPins += pins;
        this.totalUnpins += unpins;
        this.totalFaults += faults;
        this.totalBytesRead += bytesRead;
        this.totalBytesWritten += bytesWritten;
        this.totalEvictions += evictions;
        this.totalEvictionExceptions += evictionExceptions;
        this.totalFlushes += flushes;
        pins = 0;
        unpins = 0;
        faults = 0;
        bytesRead = 0;
        bytesWritten = 0;
        evictions = 0;
        evictionExceptions = 0;
        flushes = 0;
    }

    @Override
    public long faults()
    {
        return totalFaults + faults;
    }

    @Override
    public long pins()
    {
        return totalPins + pins;
    }

    @Override
    public long unpins()
    {
        return totalUnpins + unpins;
    }

    @Override
    public long bytesRead()
    {
        return totalBytesRead + bytesRead;
    }

    @Override
    public long evictions()
    {
        return totalEvictions + evictions;
    }

    @Override
    public long evictionExceptions()
    {
        return totalEvictionExceptions + evictionExceptions;
    }

    @Override
    public long bytesWritten()
    {
        return totalBytesWritten + bytesWritten;
    }

    @Override
    public long flushes()
    {
        return totalFlushes + flushes;
    }

    @Override
    public PinEvent beginPin( boolean writeLock, long filePageId, PageSwapper swapper )
    {
        pins++;
        return pinTracingEvent;
    }

    private final PinEvent pinTracingEvent = new PinEvent()
    {
        @Override
        public void setCachePageId( int cachePageId )
        {
        }

        @Override
        public PageFaultEvent beginPageFault()
        {
            return pageFaultEvent;
        }

        @Override
        public void done()
        {
            unpins++;
        }
    };

    private final EvictionEvent evictionEvent = new EvictionEvent()
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
            return flushEventOpportunity;
        }

        @Override
        public void threwException( IOException exception )
        {
            evictionExceptions++;
        }

        @Override
        public void setCachePageId( int cachePageId )
        {
        }

        @Override
        public void close()
        {
            evictions++;
        }
    };

    private final PageFaultEvent pageFaultEvent = new PageFaultEvent()
    {
        @Override
        public void addBytesRead( long bytes )
        {
            bytesRead += bytes;
        }

        @Override
        public void done()
        {
            faults++;
        }

        @Override
        public void done( Throwable throwable )
        {
            done();
        }

        @Override
        public EvictionEvent beginEviction()
        {
            return evictionEvent;
        }

        @Override
        public void setCachePageId( int cachePageId )
        {
        }
    };

    private final FlushEventOpportunity flushEventOpportunity = new FlushEventOpportunity()
    {
        @Override
        public FlushEvent beginFlush( long filePageId, int cachePageId, PageSwapper swapper )
        {
            return flushEvent;
        }
    };

    private final FlushEvent flushEvent = new FlushEvent()
    {
        @Override
        public void addBytesWritten( long bytes )
        {
            bytesWritten += bytes;
        }

        @Override
        public void done()
        {
            flushes++;
        }

        @Override
        public void done( IOException exception )
        {
            done();
        }

        @Override
        public void addPagesFlushed( int pageCount )
        {
        }
    };
}
