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
package org.neo4j.consistency.newchecker;

import java.io.IOException;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.CommonAbstractStore;

import static java.lang.Math.max;

/**
 * Page prefetch logic that paves the way for a reader so that it won't have to spend time page faulting. The other big benefit is for the backwards
 * prefetching where the pages are fetched in batches forwards because of much better that is mechanically, like so:
 *
 * <pre>
 *   Backwards prefetching:
 *   pages   0                                                                                N
 *          |----------------------------------------------------------------------------------|
 *       (1)                                                              -------------------->   jump back and load pages forwards
 *       (2)                                                                      <------------   as reader advances backwards...
 *       (3)                                         -------------------->                        jump one more batch backwards and load pages forwards
 *       (4)                          a.s.o.                                                      ad infinitum, nah until page 0 basically
 * </pre>
 */
class StorePagePrefetcher
{
    public interface Monitor
    {
        void awaitingReader();
    }

    public static final Monitor NO_MONITOR = () ->
    {
    };

    private final CommonAbstractStore<?,?> store;
    private final int readAheadSize;
    private final BooleanSupplier cancellation;
    private final Monitor monitor;

    StorePagePrefetcher( CommonAbstractStore<?,?> store, int readAheadSize, BooleanSupplier cancellation, Monitor monitor )
    {
        this.store = store;
        this.readAheadSize = readAheadSize;
        this.cancellation = cancellation;
        this.monitor = monitor;
    }

    /**
     * Runs the prefetch of pages on the calling thread and will return when prefetch is completed, is cancelled or throws exception.
     *
     * @param currentReadingPage coordination with where the reader is, as to not prefetch too far ahead causing unnecessary eviction of important pages
     * @param forwards {@code true} if prefetch forwards, {@code false} for backwards.
     * @throws IOException on {@link PageCursor} exception thrown.
     */
    void prefetch( LongSupplier currentReadingPage, boolean forwards ) throws IOException
    {
        if ( forwards )
        {
            forwards( currentReadingPage );
        }
        else
        {
            backwards( currentReadingPage );
        }
    }

    private void forwards( LongSupplier currentReadingPage ) throws IOException
    {
        try ( PageCursor cursor = store.openPageCursorForReading( 0 ) )
        {
            // Simply read ahead
            long currentPageId;
            while ( cursor.next() && !cancellation.getAsBoolean() )
            {
                currentPageId = cursor.getCurrentPageId();
                while ( currentPageId - currentReadingPage.getAsLong() >= readAheadSize && !cancellation.getAsBoolean() )
                {
                    monitor.awaitingReader();
                    if ( sleepAWhile() )
                    {
                        break;
                    }
                }
            }
        }
    }

    private void backwards( LongSupplier currentReadingPage ) throws IOException
    {
        // Jump backwards and read batch sequentially forwards
        try ( PageCursor cursor = store.openPageCursorForReading( 0 ) )
        {
            long endPageId = store.getHighId() / store.getRecordsPerPage(); // exclusive
            long startPageId = max( 0, endPageId - readAheadSize ); // inclusive
            while ( (startPageId > 0 || endPageId > 0) && !cancellation.getAsBoolean() )
            {
                cursor.next( startPageId );
                while ( cursor.getCurrentPageId() < endPageId && !cancellation.getAsBoolean() )
                {
                    if ( !cursor.next() )
                    {
                        break;
                    }
                }
                while ( currentReadingPage.getAsLong() - startPageId > readAheadSize / 2 && !cancellation.getAsBoolean() )
                {
                    monitor.awaitingReader();
                    if ( sleepAWhile() )
                    {
                        break;
                    }
                }
                endPageId = max( 0, startPageId );
                startPageId = max( 0, endPageId - readAheadSize );
            }
        }
    }

    private boolean sleepAWhile()
    {
        try
        {
            Thread.sleep( 10 );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            return true;
        }
        return false;
    }
}
