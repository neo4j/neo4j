/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.io.pagecache.impl.muninn;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.scheduler.CancelListener;
import org.neo4j.time.SystemNanoClock;

import static org.neo4j.io.pagecache.PageCursor.UNBOUND_PAGE_ID;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;

/**
 * An adaptive page pre-fetcher for sequential scans, for either forwards (increasing page id order) or backwards (decreasing page id order) scans.
 *
 * The given page cursor is being "weakly" observed from a background pre-fetcher thread, as it is progressing through its scan, and the pre-fetcher tries
 * to touch pages ahead of the scanning cursor in order to move page fault overhead from the scanning thread to the pre-fetching thread.
 *
 * The pre-fetcher relies on {@link UnsafeUtil#putOrderedLong(Object, long, long) ordered stores} of the "current page id" from the scanner thread,
 * and on {@link UnsafeUtil#getLongVolatile(long) volatile loads} in the pre-fetcher thread, in order to observe the progress of the scanner without placing
 * too much synchronisation overhead on the scanner. Because this does not form a "synchronises-with" edge in Java Memory Model palace, we say that the
 * scanning cursor is being "weakly" observed. Ordered stores have compiler barriers, but no CPU or cache coherence barriers beyond plain stores.
 *
 * The pre-fetcher is adaptive because the number of pages the pre-fetcher will move ahead of the scanning cursor, and the length of time the pre-fetcher
 * will wait in between checking on the progress of the scanner, are dynamically computed and updated based on how fast the scanner appears to be.
 * The pre-fetcher also automatically figures out if the scanner is scanning the file in a forward or backwards direction.
 */
class PreFetcher implements Runnable, CancelListener
{
    private static final String TRACER_PRE_FETCHER_TAG = "Pre-fetcher";
    private final MuninnPageCursor observedCursor;
    private final CursorFactory cursorFactory;
    private final PageCacheTracer tracer;
    private final SystemNanoClock clock;
    private volatile boolean cancelled;
    private long startTime;
    private long deadline;
    private long tripCount;
    private long pauseNanos = TimeUnit.MILLISECONDS.toNanos( 10 );

    PreFetcher( MuninnPageCursor observedCursor, CursorFactory cursorFactory, PageCacheTracer tracer, SystemNanoClock clock )
    {
        this.observedCursor = observedCursor;
        this.cursorFactory = cursorFactory;
        this.tracer = tracer;
        this.clock = clock;
    }

    @Override
    public void run()
    {
        // Phase 1: Wait for observed cursor to start moving.
        setDeadline( 150, TimeUnit.MILLISECONDS ); // Give up if nothing happens for 150 milliseconds.
        long initialPageId;
        while ( ( initialPageId = getCurrentObservedPageId() ) == UNBOUND_PAGE_ID )
        {
            pause();
            if ( pastDeadline() )
            {
                return; // Give up. Looks like this cursor is either already finished, or never started.
            }
        }

        // Phase 2: Wait for the cursor to move either forwards or backwards, to determine the prefetching direction.
        setDeadline( 200, TimeUnit.MILLISECONDS ); // We will wait up to 200 milliseconds for this phase to complete.
        long secondPageId;
        while ( ( secondPageId = getCurrentObservedPageId() ) == initialPageId )
        {
            pause();
            if ( pastDeadline() )
            {
                return; // Okay, this is going too slow. Give up.
            }
        }
        if ( secondPageId == UNBOUND_PAGE_ID )
        {
            return; // We're done. The observed cursor was closed.
        }

        // Phase 3: We now know what direction to prefetch in.
        // Just keep loading pages on the right side of the cursor until its closed.
        boolean forward = initialPageId < secondPageId;
        long currentPageId;
        long cp;
        long nextPageId;
        long fromPage;
        long toPage;

        // Offset is a fixed adjustment of the observed cursor position.
        // This moves the start of the pre-fetch range forward for forward pre-fetching,
        // or the end position backward for backward pre-fetching.
        long offset = forward ? 1 : -1;

        // Jump is the dynamically adjusted size of the prefetch range,
        // with a sign component to indicate forwards or backwards pre-fetching.
        // That is, jump is negative if we are pre-fetching backwards.
        // This way, observed position + jump is the end or start of the pre-fetch range,
        // for forwards or backwards pre-fetch respectively.
        // The initial value don't matter so much. Just same as offset, so we initially fetch one page.
        long jump = offset;

        try ( PageCursorTracer cursorTracer = tracer.createPageCursorTracer( TRACER_PRE_FETCHER_TAG );
              PageCursor prefetchCursor = cursorFactory.takeReadCursor( 0, PF_SHARED_READ_LOCK, cursorTracer ) )
        {
            currentPageId = getCurrentObservedPageId();
            while ( currentPageId != UNBOUND_PAGE_ID )
            {
                cp = currentPageId + offset;
                if ( forward )
                {
                    fromPage = cp;
                    toPage = cp + jump;
                }
                else
                {
                    fromPage = Math.max( 0, cp + jump );
                    toPage = cp;
                }
                while ( fromPage < toPage )
                {
                    if ( !prefetchCursor.next( fromPage ) || cancelled )
                    {
                        return; // Reached the end of the file. Or got cancelled.
                    }
                    fromPage++;
                }

                // Phase 3.5: After each prefetch round, we wait for the cursor to move again.
                // If it just stops somewhere for more than a second, then we quit.
                nextPageId = getCurrentObservedPageId();
                if ( nextPageId == currentPageId )
                {
                    setDeadline( 10, TimeUnit.SECONDS );
                    while ( nextPageId == currentPageId )
                    {
                        pause();
                        if ( pastDeadline() )
                        {
                            return; // The cursor hasn't made any progress for a whole second. Leave it alone.
                        }
                        nextPageId = getCurrentObservedPageId();
                    }
                    madeProgress();
                }
                if ( nextPageId != UNBOUND_PAGE_ID )
                {
                    jump = (nextPageId - currentPageId) * 2;
                }
                currentPageId = nextPageId;
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private void setDeadline( long timeout, TimeUnit unit )
    {
        startTime = clock.nanos();
        deadline = unit.toNanos( timeout ) + startTime;
        if ( tripCount != 0 )
        {
            tripCount = 0;
        }
    }

    private void pause()
    {
        if ( tripCount < 10 )
        {
            Thread.onSpinWait();
        }
        else
        {
            LockSupport.parkNanos( this, pauseNanos );
        }
        tripCount++;
    }

    private boolean pastDeadline()
    {
        boolean past = clock.nanos() > deadline;
        if ( past )
        {
            if ( tripCount != 0 )
            {
                tripCount = 0;
            }
        }
        return past || cancelled;
    }

    private void madeProgress()
    {
        // Let our best guess of how long is good to pause, asymptotically approach how long we actually paused (this time).
        long timeToProgressNanos = clock.nanos() - startTime;
        long pause = (pauseNanos * 3 + timeToProgressNanos * 5) / 8;
        pauseNanos = Math.min( pause, TimeUnit.MILLISECONDS.toNanos( 10 ) );
    }

    private long getCurrentObservedPageId()
    {
        // Read as volatile even though the field isn't volatile.
        // We rely on the ordered-store of all writes to the current page id field, in order to weakly observe this value.
        return observedCursor.loadVolatileCurrentPageId();
    }

    @Override
    public void cancelled()
    {
        cancelled = true;
    }
}
