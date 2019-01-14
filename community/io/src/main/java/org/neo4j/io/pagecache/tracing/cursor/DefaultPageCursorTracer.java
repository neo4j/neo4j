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
package org.neo4j.io.pagecache.tracing.cursor;

import java.io.IOException;

import org.neo4j.helpers.MathUtil;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.tracing.EvictionEvent;
import org.neo4j.io.pagecache.tracing.FlushEvent;
import org.neo4j.io.pagecache.tracing.FlushEventOpportunity;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageFaultEvent;
import org.neo4j.io.pagecache.tracing.PinEvent;

public class DefaultPageCursorTracer implements PageCursorTracer
{
    private long pins;
    private long unpins;
    private long hits;
    private long historicalHits;
    private long faults;
    private long historicalFaults;
    private long bytesRead;
    private long bytesWritten;
    private long evictions;
    private long evictionExceptions;
    private long flushes;

    private PageCacheTracer pageCacheTracer = PageCacheTracer.NULL;
    private DefaultPinEvent pinTracingEvent = new DefaultPinEvent();

    @Override
    public void init( PageCacheTracer pageCacheTracer )
    {
        this.pageCacheTracer = pageCacheTracer;
    }

    @Override
    public void reportEvents()
    {
        if ( pins > 0 )
        {
            pageCacheTracer.pins( pins );
        }
        if ( unpins > 0 )
        {
            pageCacheTracer.unpins( unpins );
        }
        if ( hits > 0 )
        {
            pageCacheTracer.hits( hits );
            historicalHits = historicalHits + hits;
        }
        if ( faults > 0 )
        {
            pageCacheTracer.faults( faults );
            historicalFaults = historicalFaults + faults;
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
        reset();
    }

    @Override
    public long accumulatedHits()
    {
        return historicalHits + hits;
    }

    @Override
    public long accumulatedFaults()
    {
        return historicalFaults + faults;
    }

    private void reset()
    {
        pins = 0;
        unpins = 0;
        hits = 0;
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
        return faults;
    }

    @Override
    public long pins()
    {
        return pins;
    }

    @Override
    public long unpins()
    {
        return unpins;
    }

    @Override
    public long hits()
    {
        return hits;
    }

    @Override
    public long bytesRead()
    {
        return bytesRead;
    }

    @Override
    public long evictions()
    {
        return evictions;
    }

    @Override
    public long evictionExceptions()
    {
        return evictionExceptions;
    }

    @Override
    public long bytesWritten()
    {
        return bytesWritten;
    }

    @Override
    public long flushes()
    {
        return flushes;
    }

    @Override
    public double hitRatio()
    {
        return MathUtil.portion( hits(), faults() );
    }

    @Override
    public PinEvent beginPin( boolean writeLock, long filePageId, PageSwapper swapper )
    {
        pins++;
        pinTracingEvent.eventHits = 1;
        return pinTracingEvent;
    }

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
        public void setCachePageId( long cachePageId )
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
        public void setCachePageId( long cachePageId )
        {
        }
    };

    private final FlushEventOpportunity flushEventOpportunity = new FlushEventOpportunity()
    {
        @Override
        public FlushEvent beginFlush( long filePageId, long cachePageId, PageSwapper swapper )
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

    private class DefaultPinEvent implements PinEvent
    {
        int eventHits = 1;

        @Override
        public void setCachePageId( long cachePageId )
        {
        }

        @Override
        public PageFaultEvent beginPageFault()
        {
            eventHits = 0;
            return pageFaultEvent;
        }

        @Override
        public void hit()
        {
            hits += eventHits;
        }

        @Override
        public void done()
        {
            unpins++;
        }
    }
}
