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
import java.util.concurrent.TimeUnit;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import static org.neo4j.io.pagecache.PageCursor.UNBOUND_PAGE_ID;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;

public class PreFetcher implements Runnable
{
    private final MuninnPageCursor observedCursor;
    private final CursorFactory cursorFactory;
    private final long prefetchDistance;
    private final PageCursorTracer tracer;
    private long deadline;

    public PreFetcher( MuninnPageCursor observedCursor, CursorFactory cursorFactory, long pageCount, PageCursorTracer tracer )
    {
        this.observedCursor = observedCursor;
        this.cursorFactory = cursorFactory;
        this.prefetchDistance = Math.min( 1000, Math.max( pageCount / 5, 1 ) );
        this.tracer = tracer;
    }

    @Override
    public void run()
    {
        // Phase 1: Wait for observed cursor to start moving.
        setDeadline( 150, TimeUnit.MILLISECONDS ); // Give up if nothing happens for 150 milliseconds.
        long initialPageId;
        while ( ( initialPageId = getCurrentObservedPageId()) == UNBOUND_PAGE_ID )
        {
            Thread.yield();
            if ( pastDeadline() )
            {
                return; // Give up. Looks like this cursor is either already finished, or never started.
            }
        }

        // Phase 2: Wait for the cursor to move either forwards or backwards, to determine the prefetching direction.
        setDeadline( 200, TimeUnit.MILLISECONDS ); // We will wait up to 200 milliseconds for this phase to complete.
        long secondPageId;
        while ( ( secondPageId = getCurrentObservedPageId()) == initialPageId )
        {
            Thread.yield();
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
        // We load up to 1.000 pages ahead of the cursor. That's 8 MiBs of slack.
        long distance = initialPageId < secondPageId ? prefetchDistance : -prefetchDistance;
        long currentPageId;
        long nextPageId;
        try ( PageCursorTracer cursorTracer = tracer.fork();
              PageCursor prefetchCursor = cursorFactory.takeReadCursor( 0, PF_SHARED_READ_LOCK, cursorTracer ) )
        {
            currentPageId = getCurrentObservedPageId();
            while ( currentPageId != UNBOUND_PAGE_ID )
            {
                long fromPage = Math.min( currentPageId, currentPageId + distance );
                long toPage = Math.max( currentPageId, currentPageId + distance );
                while ( fromPage < toPage )
                {
                    prefetchCursor.next( fromPage++ );
                }
                // Phase 3.5: After each prefetch round, we wait for the cursor to move again.
                // If it just stops somewhere for more than a second, then we quit.
                setDeadline( 1, TimeUnit.SECONDS );
                nextPageId = getCurrentObservedPageId();
                while ( nextPageId == currentPageId )
                {
                    Thread.yield();
                    if ( pastDeadline() )
                    {
                        return; // The cursor hasn't made any progress for a whole second. Leave it alone.
                    }
                    nextPageId = getCurrentObservedPageId();
                }
                currentPageId = nextPageId;
            }
        }
        catch ( IOException e )
        {
            e.printStackTrace(); // todo what can we do about this?
        }
    }

    private void setDeadline( long timeout, TimeUnit unit )
    {
        deadline = unit.toNanos( timeout ) + System.nanoTime();
    }

    private boolean pastDeadline()
    {
        return System.nanoTime() > deadline;
    }

    private long getCurrentObservedPageId()
    {
        // Read as volatile even though the field isn't volatile.
        // We rely on the ordered-store of all writes to the current page id field, in order to weakly observe this value.
        return observedCursor.loadVolatileCurrentPageId();
    }
}
