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
import java.util.Objects;

import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.tracing.EvictionEvent;
import org.neo4j.io.pagecache.tracing.FlushEvent;
import org.neo4j.io.pagecache.tracing.FlushEventOpportunity;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageFaultEvent;
import org.neo4j.io.pagecache.tracing.PinEvent;

public class DefaultPageCursorTracer implements PageCursorTracer
{
    private long pins = 0L;
    private long unpins = 0L;
    private long faults = 0L;
    private long bytesRead = 0L;
    private long bytesWritten = 0L;
    private long evictions = 0L;
    private long evictionExceptions = 0L;
    private long flushes = 0L;

    private long cyclePinsStart;
    private long cycleUnpinsStart;
    private long cycleFaultsStart;
    private long cycleBytesReadStart;
    private long cycleBytesWrittenStart;
    private long cycleEvictionsStart;
    private long cycleEvictionExceptionsStart;
    private long cycleFlushesStart;

    private PageCacheTracer pageCacheTracer;

    public void init( PageCacheTracer pageCacheTracer )
    {
        this.pageCacheTracer = pageCacheTracer;
    }

    public void reportEvents()
    {
        Objects.nonNull( pageCacheTracer );
        pageCacheTracer.pins( Math.abs( pins - cyclePinsStart ) );
        pageCacheTracer.unpins( Math.abs( unpins - cycleUnpinsStart ) );
        pageCacheTracer.faults( Math.abs( faults - cycleFaultsStart ) );
        pageCacheTracer.bytesRead( Math.abs( bytesRead - cycleBytesReadStart ) );
        pageCacheTracer.evictions( Math.abs( evictions - cycleEvictionsStart ) );
        pageCacheTracer.evictionExceptions( Math.abs( evictionExceptions - cycleEvictionExceptionsStart ) );
        pageCacheTracer.bytesWritten( Math.abs( bytesWritten - cycleBytesWrittenStart ) );
        pageCacheTracer.flushes( Math.abs( flushes - cycleFlushesStart ) );
        rememberCycleStartValues();
    }

    private void rememberCycleStartValues()
    {
        this.cyclePinsStart = pins;
        this.cycleUnpinsStart = unpins;
        this.cycleFaultsStart = faults;
        this.cycleBytesReadStart = bytesRead;
        this.cycleBytesWrittenStart = bytesWritten;
        this.cycleEvictionsStart = evictions;
        this.cycleEvictionExceptionsStart = evictionExceptions;
        this.cycleFlushesStart = flushes;
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
