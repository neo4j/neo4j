/*
 * Copyright (c) "Neo4j"
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.neo4j.internal.helpers.MathUtil;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.tracing.EvictionEvent;
import org.neo4j.io.pagecache.tracing.FlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageFaultEvent;
import org.neo4j.io.pagecache.tracing.PageReferenceTranslator;
import org.neo4j.io.pagecache.tracing.PinEvent;

import static org.neo4j.util.FeatureToggles.flag;

public class DefaultPageCursorTracer implements PageCursorTracer
{
    /**
     * On encountering a mismatching counts check error in a test this debugging is useful to trace down which exact pin it's about.
     * Just flip DEBUG_PINS = true.
     */
    private static final boolean DEBUG_PINS = false;
    private static final ConcurrentMap<PinEvent,Exception> PIN_DEBUG_MAP = DEBUG_PINS ? new ConcurrentHashMap<>() : null;

    private static final boolean CHECK_REPORTED_COUNTERS = flag( DefaultPageCursorTracer.class, "CHECK_REPORTED_COUNTERS", false );

    private long pins;
    private long unpins;
    private long hits;
    private long faults;
    private long bytesRead;
    private long bytesWritten;
    private long evictions;
    private long evictionExceptions;
    private long flushes;
    private long merges;

    private final DefaultPinEvent pinTracingEvent = new DefaultPinEvent();
    private final PageCacheTracer pageCacheTracer;
    private final String tag;
    private boolean ignoreCounterCheck;

    public DefaultPageCursorTracer( PageCacheTracer pageCacheTracer, String tag )
    {
        this.pageCacheTracer = pageCacheTracer;
        this.tag = tag;
    }

    @Override
    public String getTag()
    {
        return tag;
    }

    @Override
    public void closeCursor()
    {
        pageCacheTracer.closeCursor();
    }

    @Override
    public void merge( PageCursorTracer cursorTracer )
    {
        this.pins += cursorTracer.pins();
        this.unpins += cursorTracer.unpins();
        this.hits += cursorTracer.hits();
        this.faults += cursorTracer.faults();
        this.bytesRead += cursorTracer.bytesRead();
        this.bytesWritten += cursorTracer.bytesWritten();
        this.evictions += cursorTracer.evictions();
        this.evictionExceptions += cursorTracer.evictionExceptions();
        this.flushes += cursorTracer.flushes();
        this.merges += cursorTracer.merges();
    }

    // When updating reporting here please check if that affects any reporting on additional available tracers
    @Override
    public void reportEvents()
    {
        if ( CHECK_REPORTED_COUNTERS && !ignoreCounterCheck )
        {
            checkCounters();
        }
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
        if ( merges > 0 )
        {
            pageCacheTracer.merges( merges );
        }
        reset();
    }

    private void checkCounters()
    {
        boolean pinsMismatch = pins != unpins;
        if ( pinsMismatch )
        {
            throw new RuntimeException( "Mismatch cursor counters. " + this );
        }
    }

    @Override
    public String toString()
    {
        return "PageCursorTracer{" + "pins=" + pins + ", unpins=" + unpins + ", hits=" + hits + ", faults=" + faults + ", bytesRead=" + bytesRead +
                ", bytesWritten=" + bytesWritten + ", evictions=" + evictions + ", evictionExceptions=" + evictionExceptions + ", flushes=" + flushes +
                ", merges=" + merges + ", tag='" + tag + '\'' + (DEBUG_PINS ? ", current (yet unpinned) pins:" + currentPins() : "" ) + '}';
    }

    private String currentPins()
    {
        assert DEBUG_PINS;
        ByteArrayOutputStream byteArrayOut = new ByteArrayOutputStream();
        try ( PrintStream out = new PrintStream( byteArrayOut ) )
        {
            PIN_DEBUG_MAP.forEach( ( pin, stackTrace ) ->
            {
                out.println();
                stackTrace.printStackTrace( out );
            } );
        }
        return byteArrayOut.toString();
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
        merges = 0;
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
    public long merges()
    {
        return merges;
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
        if ( DEBUG_PINS )
        {
            DefaultPinEvent event = new DefaultPinEvent();
            event.eventHits = 1;
            PIN_DEBUG_MAP.put( event, new Exception() );
            return event;
        }
        else
        {
            pinTracingEvent.eventHits = 1;
            return pinTracingEvent;
        }
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
        public FlushEvent beginFlush( long pageRef, PageSwapper swapper, PageReferenceTranslator pageReferenceTranslator )
        {
            return flushEvent;
        }

        @Override
        public void threwException( IOException exception )
        {
            evictionExceptions++;
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
        public void fail( Throwable throwable )
        {
            done();
        }

        @Override
        public void freeListSize( int listSize )
        {
        }

        @Override
        public EvictionEvent beginEviction( long cachePageId )
        {
            return evictionEvent;
        }

        @Override
        public void setCachePageId( long cachePageId )
        {
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
        }

        @Override
        public void done( IOException exception )
        {
            done();
        }

        @Override
        public void addPagesFlushed( int pageCount )
        {
            flushes += pageCount;
        }

        @Override
        public void addPagesMerged( int pagesMerged )
        {
            merges += pagesMerged;
        }
    };

    public void setIgnoreCounterCheck( boolean ignoreCounterCheck )
    {
        this.ignoreCounterCheck = ignoreCounterCheck;
    }

    private class DefaultPinEvent implements PinEvent
    {
        private int eventHits = 1;

        @Override
        public void setCachePageId( long cachePageId )
        {
        }

        @Override
        public PageFaultEvent beginPageFault( long filePageId, int swapperId )
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
            if ( DEBUG_PINS )
            {
                PIN_DEBUG_MAP.remove( this );
            }
        }
    }
}
