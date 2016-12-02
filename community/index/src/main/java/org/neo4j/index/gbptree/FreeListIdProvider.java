/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.index.gbptree;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

class FreeListIdProvider implements IdProvider
{
    private final byte[] zeroPage;

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
     * StableGen   = 1
     * UnstableGen = 2
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

    FreeListIdProvider( PagedFile pagedFile, int pageSize, long lastId )
    {
        this.pagedFile = pagedFile;
        this.freelistNode = new FreelistNode( pageSize );
        this.lastId = lastId;
        this.zeroPage = new byte[pageSize];
    }

    void initialize( long lastId, long writePageId, long readPageId, int writePos, int readPos )
    {
        this.lastId = lastId;
        this.writePageId = writePageId;
        this.readPageId = readPageId;
        this.writePos = writePos;
        this.readPos = readPos;
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

        // Zero the page
        try ( PageCursor cursor = pagedFile.io( acquiredId, PagedFile.PF_SHARED_WRITE_LOCK ) )
        {
            if ( !cursor.next() )
            {
                throw new IllegalStateException( "Could not go to newly allocated page " + acquiredId );
            }
            // TODO use cursor.clear() when available
            cursor.putBytes( zeroPage );
        }
        return acquiredId;
    }

    private long acquireNewIdFromFreelistOrEnd( long stableGeneration, long unstableGeneration,
            boolean allowTakeLastFromPage ) throws IOException
    {
        if ( (readPageId != writePageId || readPos < writePos) &&
                (allowTakeLastFromPage || readPos < freelistNode.maxEntries() - 1) )
        {
            // It looks like read pos is < write pos so check if we can grab the next id
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
                    // id in the free list which we can acquire from a stable generation
                    if ( ++readPos >= freelistNode.maxEntries() )
                    {
                        readPos = 0;
                        do
                        {
                            readPageId = freelistNode.next( cursor );
                        } while ( cursor.shouldRetry() );

                        // Put the exhausted free-list page id itself on the free-list
                        long exhaustedFreelistPageId = cursor.getCurrentPageId();
                        releaseId( stableGeneration, unstableGeneration, exhaustedFreelistPageId );
                    }
                    return resultPageId;
                }
            }
        }

        // Fall-back to acquiring at the end of the file
        return ++lastId;
    }

    @Override
    public void releaseId( long stableGeneration, long unstableGeneration, long id ) throws IOException
    {
        try ( PageCursor cursor = pagedFile.io( writePageId, PagedFile.PF_SHARED_WRITE_LOCK ) )
        {
            if ( !cursor.next() )
            {
                throw new IllegalStateException( "Couldn't go to free-list write page " + writePageId );
            }
            freelistNode.write( cursor, unstableGeneration, id, writePos );
            writePos++;
        }

        if ( writePos >= freelistNode.maxEntries() )
        {
            // Current free-list write page is full, allocate a new one.
            long nextFreelistPage = acquireNewId( stableGeneration, unstableGeneration, false );
            try ( PageCursor cursor = pagedFile.io( writePageId, PagedFile.PF_SHARED_WRITE_LOCK ) )
            {
                if ( !cursor.next() )
                {
                    throw new IllegalStateException( "Couldn't go to free-list write page " + writePageId );
                }
                freelistNode.initialize( cursor );
                freelistNode.setNext( cursor, nextFreelistPage );
            }
            writePageId = nextFreelistPage;
            writePos = 0;
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
