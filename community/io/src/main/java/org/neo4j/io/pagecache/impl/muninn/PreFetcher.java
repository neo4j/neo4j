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
package org.neo4j.io.pagecache.impl.muninn;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import static org.neo4j.io.pagecache.PageCursor.UNBOUND_PAGE_ID;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;

public class PreFetcher implements Runnable
{
    private static final String TRACER_PRE_FETCHER_TAG = "Pre-fetcher";
    private final MuninnPageCursor observedCursor;
    private final CursorFactory cursorFactory;
    private final PageCacheTracer tracer;
    private long startTime;
    private long deadline;
    private long tripCount;
    private long pauseNanos = TimeUnit.MILLISECONDS.toNanos( 10 );

    public PreFetcher( MuninnPageCursor observedCursor, CursorFactory cursorFactory, PageCacheTracer tracer )
    {
        this.observedCursor = observedCursor;
        this.cursorFactory = cursorFactory;
        this.tracer = tracer;
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
        long jump = forward ? -1 : 1;
        int offset = 1;
        try ( PageCursorTracer cursorTracer = tracer.createPageCursorTracer( TRACER_PRE_FETCHER_TAG );
              PageCursor prefetchCursor = cursorFactory.takeReadCursor( 0, PF_SHARED_READ_LOCK, cursorTracer ) )
        {
            currentPageId = getCurrentObservedPageId();
            while ( currentPageId != UNBOUND_PAGE_ID )
            {
                cp = forward ? currentPageId + offset : currentPageId - offset;
                long fromPage = Math.min( cp, cp + jump );
                long toPage = Math.max( cp, cp + jump );
                while ( fromPage < toPage )
                {
                    if ( !prefetchCursor.next( fromPage ) )
                    {
                        return; // Reached end of file.
                    }
                    fromPage++;
                }

                // Phase 3.5: After each prefetch round, we wait for the cursor to move again.
                // If it just stops somewhere for more than a second, then we quit.
                nextPageId = getCurrentObservedPageId();
                if ( nextPageId == currentPageId )
                {
                    setDeadline( 1, TimeUnit.SECONDS );
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
        startTime = System.nanoTime();
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
        boolean past = System.nanoTime() > deadline;
        if ( past )
        {
            if ( tripCount != 0 )
            {
                tripCount = 0;
            }
        }
        return past;
    }

    private void madeProgress()
    {
        // Let our best guess of how long is good to pause, asymptotically approach how long we actually paused (this time).
        long timeToProgressNanos = System.nanoTime() - startTime;
        long pause = (pauseNanos * 3 + timeToProgressNanos * 5) / 8;
        pauseNanos = Math.min( pause, TimeUnit.MILLISECONDS.toNanos( 10 ) );
    }

    private long getCurrentObservedPageId()
    {
        // Read as volatile even though the field isn't volatile.
        // We rely on the ordered-store of all writes to the current page id field, in order to weakly observe this value.
        return observedCursor.loadVolatileCurrentPageId();
    }
}
