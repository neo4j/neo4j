/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.io.pagecache.impl.muninn;

import java.io.IOException;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongIntMap;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

class MuninnPagedFile implements PagedFile
{
    private static final long referenceCounterOffset =
            UnsafeUtil.getFieldOffset( MuninnPagedFile.class, "referenceCounter" );

    // these are the globally shared pages of the cache:
    private final MuninnPage[] cachePages;
    // this is the table where we translate file-page-ids to cache-page-ids:
    private final PrimitiveLongIntMap translationTable;
    private final int pageSize;
    private final MuninnCursorFreelist readCursors;
    private final MuninnCursorFreelist writeCursors;

    // accessed via Unsafe
    private volatile int referenceCounter;

    MuninnPagedFile( MuninnPage[] pages, int pageSize )
    {
        this.cachePages = pages;
        this.pageSize = pageSize;
        // the initial capacity of our translation table is one quarter
        // the number of pages in the cache in total, because we're most
        // likely not going to be the only mapped file.
        translationTable = Primitive.longIntMap( pages.length / 4 );
        readCursors = new MuninnCursorFreelist()
        {
            @Override
            protected MuninnPageCursor createNewCursor()
            {
                return new MuninnReadPageCursor( this );
            }
        };
        writeCursors = new MuninnCursorFreelist()
        {
            @Override
            protected MuninnPageCursor createNewCursor()
            {
                return new MuninnWritePageCursor( this );
            }
        };
    }

    @Override
    public PageCursor io( long pageId, int pf_flags ) throws IOException
    {
        int lockMask = PF_EXCLUSIVE_LOCK | PF_SHARED_LOCK;
        if ( (pf_flags & lockMask) == 0 )
        {
            throw new IllegalArgumentException(
                    "Must specify either PF_EXCLUSIVE_LOCK or PF_SHARED_LOCK" );
        }
        if ( (pf_flags & lockMask) == lockMask )
        {
            throw new IllegalArgumentException(
                    "Cannot specify both PF_EXCLUSIVE_LOCK and PF_SHARED_LOCK" );
        }
        MuninnPageCursor cursor = (pf_flags & PF_SHARED_LOCK) == 0?
                writeCursors.takeCursor() : readCursors.takeCursor();
        cursor.initialise( this, pageId, pf_flags );
        cursor.rewind();
        return cursor;
    }

    @Override
    public int pageSize()
    {
        return pageSize;
    }

    @Override
    public void close() throws IOException
    {

    }

    @Override
    public int numberOfCachedPages()
    {
        return translationTable.size();
    }

    @Override
    public void flush() throws IOException
    {

    }

    @Override
    public void force() throws IOException
    {

    }

    @Override
    public long getLastPageId() throws IOException
    {
        return 0;
    }

    /**
     * Atomically increment the reference count for this mapped file.
     */
    void incrementReferences()
    {
        UnsafeUtil.getAndAddInt( this, referenceCounterOffset, 1 );
    }

    /**
     * Atomically decrement the reference count. Returns true if this was the
     * last reference.
     */
    boolean decrementReferences()
    {
        // compares with 1 because getAndAdd returns the old value, and a 1
        // means the value is now 0.
        return UnsafeUtil.getAndAddInt( this, referenceCounterOffset, -1 ) == 1;
    }
}
