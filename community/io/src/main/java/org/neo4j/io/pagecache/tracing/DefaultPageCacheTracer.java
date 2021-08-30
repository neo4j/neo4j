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
package org.neo4j.io.pagecache.tracing;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.neo4j.internal.helpers.MathUtil;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

/**
 * The default PageCacheTracer implementation, that just increments counters.
 */
public class DefaultPageCacheTracer implements PageCacheTracer
{
    protected final LongAdder faults = new LongAdder();
    protected final LongAdder evictions = new LongAdder();
    protected final LongAdder pins = new LongAdder();
    protected final LongAdder unpins = new LongAdder();
    protected final LongAdder hits = new LongAdder();
    protected final LongAdder flushes = new LongAdder();
    protected final LongAdder merges = new LongAdder();
    protected final LongAdder bytesRead = new LongAdder();
    protected final LongAdder bytesWritten = new LongAdder();

    protected final LongAdder filesMapped = new LongAdder();
    protected final LongAdder filesUnmapped = new LongAdder();

    protected final LongAdder evictionExceptions = new LongAdder();
    protected final LongAdder iopqPerformed = new LongAdder();
    protected final LongAdder ioLimitedTimes = new LongAdder();
    protected final LongAdder ioLimitedMillis = new LongAdder();
    protected final LongAdder openedCursors = new LongAdder();
    protected final LongAdder closedCursors = new LongAdder();
    protected final AtomicLong maxPages = new AtomicLong();

    private final boolean tracePageFileIndividually;

    private final PageCacheFlushEvent flushEvent = new PageCacheFlushEvent();

    private final EvictionEvent evictionEvent = new EvictionEvent()
    {
        private PageFileSwapperTracer swapperTracer;

        @Override
        public void setFilePageId( long filePageId )
        {
        }

        @Override
        public void setSwapper( PageSwapper swapper )
        {
            this.swapperTracer = swapper.fileSwapperTracer();
        }

        @Override
        public FlushEvent beginFlush( long pageRef, PageSwapper swapper, PageReferenceTranslator pageReferenceTranslator )
        {
            flushEvent.swapperTracer = swapper.fileSwapperTracer();
            return flushEvent;
        }

        @Override
        public void threwException( IOException exception )
        {
            evictionExceptions.increment();
            swapperTracer.evictionExceptions( 1 );
        }

        @Override
        public void close()
        {
            evictions.increment();
            if ( swapperTracer != null )
            {
                swapperTracer.evictions( 1 );
            }
        }
    };

    private final EvictionRunEvent evictionRunEvent = new EvictionRunEvent()
    {
        @Override
        public void freeListSize( int size )
        {

        }

        @Override
        public EvictionEvent beginEviction( long cachePageId )
        {
            return evictionEvent;
        }

        @Override
        public void close()
        {
        }
    };

    private final MajorFlushEvent majorFlushEvent = new MajorFlushEvent()
    {

        @Override
        public FlushEvent beginFlush( long[] pageRefs, PageSwapper swapper, PageReferenceTranslator pageReferenceTranslator, int pagesToFlush,
                int mergedPages )
        {
            flushEvent.swapperTracer = swapper.fileSwapperTracer();
            return flushEvent;
        }

        @Override
        public FlushEvent beginFlush( long pageRef, PageSwapper swapper, PageReferenceTranslator pageReferenceTranslator )
        {
            flushEvent.swapperTracer = swapper.fileSwapperTracer();
            return flushEvent;
        }

        @Override
        public void startFlush( int[][] translationTable )
        {

        }

        @Override
        public ChunkEvent startChunk( int[] chunk )
        {
            return ChunkEvent.NULL;
        }

        @Override
        public void throttle( long millis )
        {
            ioLimitedTimes.increment();
            ioLimitedMillis.add( millis );
        }

        @Override
        public void reportIO( int completedIOs )
        {
            iopqPerformed.add( completedIOs );
        }

        @Override
        public void close()
        {
        }
    };

    public DefaultPageCacheTracer()
    {
        this( false );
    }

    public DefaultPageCacheTracer( boolean tracePageFileIndividually )
    {
        this.tracePageFileIndividually = tracePageFileIndividually;
    }

    @Override
    public PageFileSwapperTracer createFileSwapperTracer()
    {
        return tracePageFileIndividually ? new DefaultPageFileSwapperTracer() : PageFileSwapperTracer.NULL;
    }

    @Override
    public PageCursorTracer createPageCursorTracer( String tag )
    {
        return new DefaultPageCursorTracer( this, tag );
    }

    @Override
    public void mappedFile( int swapperId, PagedFile mappedFile )
    {
        filesMapped.increment();
    }

    @Override
    public void unmappedFile( int swapperId, PagedFile mappedFile )
    {
        filesUnmapped.increment();
    }

    @Override
    public EvictionRunEvent beginPageEvictions( int pageCountToEvict )
    {
        return evictionRunEvent;
    }

    @Override
    public EvictionRunEvent beginEviction()
    {
        return evictionRunEvent;
    }

    @Override
    public MajorFlushEvent beginFileFlush( PageSwapper swapper )
    {
        return majorFlushEvent;
    }

    @Override
    public MajorFlushEvent beginCacheFlush()
    {
        return majorFlushEvent;
    }

    @Override
    public long faults()
    {
        return faults.sum();
    }

    @Override
    public long evictions()
    {
        return evictions.sum();
    }

    @Override
    public long pins()
    {
        return pins.sum();
    }

    @Override
    public long unpins()
    {
        return unpins.sum();
    }

    @Override
    public long hits()
    {
        return hits.sum();
    }

    @Override
    public long flushes()
    {
        return flushes.sum();
    }

    @Override
    public long merges()
    {
        return merges.sum();
    }

    @Override
    public long bytesRead()
    {
        return bytesRead.sum();
    }

    @Override
    public long bytesWritten()
    {
        return bytesWritten.sum();
    }

    @Override
    public long filesMapped()
    {
        return filesMapped.sum();
    }

    @Override
    public long filesUnmapped()
    {
        return filesUnmapped.sum();
    }

    @Override
    public long evictionExceptions()
    {
        return evictionExceptions.sum();
    }

    @Override
    public double hitRatio()
    {
        return MathUtil.portion( hits(), faults() );
    }

    @Override
    public double usageRatio()
    {
        long pages = maxPages.get();
        if ( pages == 0 )
        {
            return 0;
        }
        return Math.max( 0, (faults.sum() - evictions.sum()) / (double) pages );
    }

    @Override
    public long iopqPerformed()
    {
        return iopqPerformed.sum();
    }

    @Override
    public long ioLimitedTimes()
    {
        return ioLimitedTimes.sum();
    }

    @Override
    public long ioLimitedMillis()
    {
        return ioLimitedMillis.sum();
    }

    @Override
    public long openedCursors()
    {
        return openedCursors.sum();
    }

    @Override
    public long closedCursors()
    {
        return closedCursors.sum();
    }

    @Override
    public void iopq( long iopq )
    {
        iopqPerformed.add( iopq );
    }

    @Override
    public void limitIO( long millis )
    {
        ioLimitedTimes.increment();
        ioLimitedMillis.add( millis );
    }

    @Override
    public void closeCursor()
    {
        closedCursors.increment();
    }

    @Override
    public void openCursor()
    {
        openedCursors.increment();
    }

    @Override
    public void pins( long pins )
    {
        this.pins.add( pins );
    }

    @Override
    public void unpins( long unpins )
    {
        this.unpins.add( unpins );
    }

    @Override
    public void hits( long hits )
    {
        this.hits.add( hits );
    }

    @Override
    public void faults( long faults )
    {
        this.faults.add( faults );
    }

    @Override
    public void bytesRead( long bytesRead )
    {
        this.bytesRead.add( bytesRead );
    }

    @Override
    public void evictions( long evictions )
    {
        this.evictions.add( evictions );
    }

    @Override
    public void evictionExceptions( long evictionExceptions )
    {
        this.evictionExceptions.add( evictionExceptions );
    }

    @Override
    public void bytesWritten( long bytesWritten )
    {
        this.bytesWritten.add( bytesWritten );
    }

    @Override
    public void flushes( long flushes )
    {
        this.flushes.add( flushes );
    }

    @Override
    public void merges( long merges )
    {
        this.merges.add( merges );
    }

    @Override
    public void maxPages( long maxPages, long pageSize )
    {
        this.maxPages.set( maxPages );
    }

    private class PageCacheFlushEvent implements FlushEvent
    {
        private PageFileSwapperTracer swapperTracer;

        @Override
        public void addBytesWritten( long bytes )
        {
            bytesWritten.add( bytes );
            swapperTracer.bytesWritten( bytes );
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
            flushes.add( pageCount );
            swapperTracer.flushes( pageCount );
        }

        @Override
        public void addPagesMerged( int pagesMerged )
        {
            merges.add( pagesMerged );
            swapperTracer.merges( pagesMerged );
        }
    }
}
