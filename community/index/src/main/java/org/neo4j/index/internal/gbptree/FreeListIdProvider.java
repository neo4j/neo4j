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
package org.neo4j.index.internal.gbptree;

import java.io.IOException;
import java.util.function.LongConsumer;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

import static org.neo4j.index.internal.gbptree.PageCursorUtil.checkOutOfBounds;
import static org.neo4j.index.internal.gbptree.PageCursorUtil.goTo;

class FreeListIdProvider implements IdProvider
{
    interface Monitor
    {
        /**
         * Called when a page id was acquired for storing released ids into.
         *
         * @param freelistPageId page id of the acquired page.
         */
        default void acquiredFreelistPageId( long freelistPageId )
        {   // Empty by default
        }

        /**
         * Called when a free-list page was released due to all its ids being acquired.
         * A released free-list page ends up in the free-list itself.
         *
         * @param freelistPageId page if of the released page.
         */
        default void releasedFreelistPageId( long freelistPageId )
        {   // Empty by default
        }
    }

    static final Monitor NO_MONITOR = new Monitor()
    {   // Empty
    };

    private final PagedFile pagedFile;

    /**
     * {@link FreelistNode} governs physical layout of a free-list.
     */
    private final FreelistNode freelistNode;

    /**
     * There's one free-list which both stable and unstable state (the state pages A/B) shares.
     * Each free list page links to a potential next free-list page, by using the last entry containing
     * page id to the next.
     * <p>
     * Each entry in the the free list consist of a page id and the generation in which it was freed.
     * <p>
     * Read pointer cannot go beyond entries belonging to stable generation.
     * About the free-list id/offset variables below:
     * <pre>
     * Every cell in picture contains generation, page id is omitted for briefness.
     * StableGeneration   = 1
     * UnstableGeneration = 2
     *
     *        {@link #readPos}                         {@link #writePos}
     *        v                               v
     *  ┌───┬───┬───┬───┬───┬───┐   ┌───┬───┬───┬───┬───┬───┐
     *  │ 1 │ 1 │ 1 │ 2 │ 2 │ 2 │-->│ 2 │ 2 │   │   │   │   │
     *  └───┴───┴───┴───┴───┴───┘   └───┴───┴───┴───┴───┴───┘
     *  ^                           ^
     *  {@link #readPageId}                  {@link #writePageId}
     * </pre>
     */
    private volatile long writePageId;
    private volatile long readPageId;
    private volatile int writePos;
    private volatile int readPos;

    /**
     * Last allocated page id, used for allocating new ids as more data gets inserted into the tree.
     */
    private volatile long lastId;

    /**
     * For monitoring internal free-list activity.
     */
    private final Monitor monitor;

    FreeListIdProvider( PagedFile pagedFile, int pageSize, long lastId, Monitor monitor )
    {
        this.pagedFile = pagedFile;
        this.monitor = monitor;
        this.freelistNode = new FreelistNode( pageSize );
        this.lastId = lastId;
    }

    void initialize( long lastId, long writePageId, long readPageId, int writePos, int readPos )
    {
        this.lastId = lastId;
        this.writePageId = writePageId;
        this.readPageId = readPageId;
        this.writePos = writePos;
        this.readPos = readPos;
    }

    void initializeAfterCreation() throws IOException
    {
        // Allocate a new free-list page id and set both write/read free-list page id to it.
        writePageId = nextLastId();
        readPageId = writePageId;

        try ( PageCursor cursor = pagedFile.io( writePageId, PagedFile.PF_SHARED_WRITE_LOCK ) )
        {
            goTo( cursor, "free-list", writePageId );
            FreelistNode.initialize( cursor );
            checkOutOfBounds( cursor );
        }
    }

    @Override
    public long acquireNewId( long stableGeneration, long unstableGeneration ) throws IOException
    {
        return acquireNewId( stableGeneration, unstableGeneration, true );
    }

    private long acquireNewId( long stableGeneration, long unstableGeneration, boolean allowTakeLastFromPage )
            throws IOException
    {
        // Acquire id from free-list or end of store file
        long acquiredId = acquireNewIdFromFreelistOrEnd( stableGeneration, unstableGeneration, allowTakeLastFromPage );

        // Zap the page, i.e. set all bytes to zero
        try ( PageCursor cursor = pagedFile.io( acquiredId, PagedFile.PF_SHARED_WRITE_LOCK ) )
        {
            PageCursorUtil.goTo( cursor, "newly allocated free-list page", acquiredId );
            cursor.zapPage();
            // don't initialize node here since this acquisition can be used both for tree nodes
            // as well as free-list nodes.
        }
        return acquiredId;
    }

    private long acquireNewIdFromFreelistOrEnd( long stableGeneration, long unstableGeneration,
            boolean allowTakeLastFromPage ) throws IOException
    {
        if ( (readPageId != writePageId || readPos < writePos) &&
                (allowTakeLastFromPage || readPos < freelistNode.maxEntries() - 1) )
        {
            // It looks like reader isn't even caught up to the writer page-wise,
            // or the read pos is < write pos so check if we can grab the next id (generation could still mismatch).
            try ( PageCursor cursor = pagedFile.io( readPageId, PagedFile.PF_SHARED_READ_LOCK ) )
            {
                if ( !cursor.next() )
                {
                    throw new IOException( "Couldn't go to free-list read page " + readPageId );
                }

                long resultPageId;
                do
                {
                    resultPageId = freelistNode.read( cursor, stableGeneration, readPos );
                }
                while ( cursor.shouldRetry() );

                if ( resultPageId != FreelistNode.NO_PAGE_ID )
                {
                    // FreelistNode compares generation and so this means that we have an available
                    // id in the free list which we can acquire from a stable generation. Increment readPos
                    readPos++;
                    if ( readPos >= freelistNode.maxEntries() )
                    {
                        // The current reader page is exhausted, go to the next free-list page.
                        readPos = 0;
                        do
                        {
                            readPageId = FreelistNode.next( cursor );
                        }
                        while ( cursor.shouldRetry() );

                        // Put the exhausted free-list page id itself on the free-list
                        long exhaustedFreelistPageId = cursor.getCurrentPageId();
                        releaseId( stableGeneration, unstableGeneration, exhaustedFreelistPageId );
                        monitor.releasedFreelistPageId( exhaustedFreelistPageId );
                    }
                    return resultPageId;
                }
            }
        }

        // Fall-back to acquiring at the end of the file
        return nextLastId();
    }

    private long nextLastId()
    {
        return ++lastId;
    }

    @Override
    public void releaseId( long stableGeneration, long unstableGeneration, long id ) throws IOException
    {
        try ( PageCursor cursor = pagedFile.io( writePageId, PagedFile.PF_SHARED_WRITE_LOCK ) )
        {
            PageCursorUtil.goTo( cursor, "free-list write page", writePageId );
            freelistNode.write( cursor, unstableGeneration, id, writePos );
            writePos++;
        }

        if ( writePos >= freelistNode.maxEntries() )
        {
            // Current free-list write page is full, allocate a new one.
            long nextFreelistPage = acquireNewId( stableGeneration, unstableGeneration, false );
            try ( PageCursor cursor = pagedFile.io( writePageId, PagedFile.PF_SHARED_WRITE_LOCK ) )
            {
                PageCursorUtil.goTo( cursor, "free-list write page", writePageId );
                FreelistNode.initialize( cursor );
                // Link previous --> new writer page
                FreelistNode.setNext( cursor, nextFreelistPage );
            }
            writePageId = nextFreelistPage;
            writePos = 0;
            monitor.acquiredFreelistPageId( nextFreelistPage );
        }
    }

    /**
     * Visits all page ids currently in use as free-list pages.
     *
     * @param visitor {@link LongConsumer} getting calls about free-list page ids.
     * @throws IOException on {@link PageCursor} error.
     */
    void visitFreelistPageIds( LongConsumer visitor ) throws IOException
    {
        if ( readPageId == FreelistNode.NO_PAGE_ID )
        {
            return;
        }

        long pageId = readPageId;
        visitor.accept( pageId );
        try ( PageCursor cursor = pagedFile.io( pageId, PagedFile.PF_SHARED_READ_LOCK ) )
        {
            while ( pageId != writePageId )
            {
                PageCursorUtil.goTo( cursor, "free-list", pageId );
                long nextFreelistPageId;
                do
                {
                    nextFreelistPageId = FreelistNode.next( cursor );
                }
                while ( cursor.shouldRetry() );
                visitor.accept( nextFreelistPageId );
                pageId = nextFreelistPageId;
            }
        }
    }

    /**
     * Visits all unacquired ids, i.e. ids that have been {@link #releaseId(long, long, long) released},
     * but not yet {@link #acquireNewId(long, long) acquired}.
     * Calling this method will not change the free-list.
     * All released ids will be visited, even ones released in unstable generation.
     *
     * @param visitor {@link LongConsumer} getting calls about unacquired ids.
     * @throws IOException on {@link PageCursor} error.
     */
    void visitUnacquiredIds( LongConsumer visitor, long stableGeneration ) throws IOException
    {
        if ( readPageId == FreelistNode.NO_PAGE_ID )
        {
            return;
        }

        long pageId = readPageId;
        int pos = readPos;
        try ( PageCursor cursor = pagedFile.io( pageId, PagedFile.PF_SHARED_READ_LOCK ) )
        {
            while ( pageId != writePageId || pos < writePos )
            {
                PageCursorUtil.goTo( cursor, "free-list", pageId );

                // Read next un-acquired id
                long unacquiredId;
                long nextFreelistPageId;
                do
                {
                    unacquiredId = freelistNode.read( cursor, stableGeneration, pos );
                    nextFreelistPageId = FreelistNode.next( cursor );
                }
                while ( cursor.shouldRetry() );

                // Tell visitor
                visitor.accept( unacquiredId );

                // Go to next id
                if ( ++pos == freelistNode.maxEntries() )
                {
                    // Go to next free-list page
                    pos = 0;
                    pageId = nextFreelistPageId;
                }
            }
        }
    }

    long lastId()
    {
        return lastId;
    }

    long writePageId()
    {
        return writePageId;
    }

    long readPageId()
    {
        return readPageId;
    }

    int writePos()
    {
        return writePos;
    }

    int readPos()
    {
        return readPos;
    }

    // test-access method
    int entriesPerPage()
    {
        return freelistNode.maxEntries();
    }
}
