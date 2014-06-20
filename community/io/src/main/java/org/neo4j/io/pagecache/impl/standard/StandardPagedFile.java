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
package org.neo4j.io.pagecache.impl.standard;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCacheMonitor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageLock;
import org.neo4j.io.pagecache.PagedFile;

public class StandardPagedFile implements PagedFile
{
    static final Object NULL = new Object();

    private final PageTable table;
    private final int filePageSize;
    private final PageCacheMonitor monitor;
    /** Currently active pages in the file this object manages. */
    private final  ConcurrentMap<Long, Object> filePages;
    private final StandardPageSwapper swapper;
    private final AtomicInteger references;
    private final AtomicLong lastPageId;
    private final CursorFreelist cursorFreelist;

    /**
     * @param table
     * @param file
     * @param channel
     * @param filePageSize is the page size used by this file, NOT the page size used by
     *                     the cache. This value is always smaller than the page size used
     *                     by the cache. The remaining space in the page cache buffers is
     *                     left unused.
     * @param monitor
     */
    public StandardPagedFile(
            PageTable table,
            File file,
            StoreChannel channel,
            int filePageSize,
            PageCacheMonitor monitor ) throws IOException
    {
        this.table = table;
        this.filePageSize = filePageSize;
        this.monitor = monitor;
        this.filePages = new ConcurrentHashMap<>();
        this.swapper = new StandardPageSwapper( file, channel, filePageSize, new RemoveEvictedPage( filePages ) );
        this.references = new AtomicInteger( 1 );
        this.lastPageId = new AtomicLong( swapper.getLastPageId() );
        this.cursorFreelist = new CursorFreelist();
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
        // Taking shared locks implies an inability to grow the file
        pf_flags |= (pf_flags & PF_SHARED_LOCK) != 0? PF_NO_GROW : 0;
        StandardPageCursor cursor = cursorFreelist.takeCursor();
        cursor.initialise( this, pageId, pf_flags );
        cursor.rewind();
        return cursor;
    }

    @Override
    public void pin( PageCursor pinToCursor, PageLock lock, long pageId ) throws IOException
    {
        StandardPageCursor cursor = (StandardPageCursor) pinToCursor;
        pin( cursor, lock, pageId );
    }

    void pin( StandardPageCursor cursor, PageLock lock, long pageId ) throws IOException
    {
        cursor.assertNotInUse();
        for (;;)
        {
            Object pageRef = filePages.get( pageId );
            if ( pageRef == null )
            {
                filePages.putIfAbsent( pageId, NULL );
            }
            else if ( pageRef == NULL )
            {
                CountDownLatch latch = new CountDownLatch( 1 );
                if ( filePages.replace( pageId, pageRef, latch ) )
                {
                    PinnablePage page = table.load( swapper, pageId, lock );
                    cursor.reset( page, lock );
                    filePages.put( pageId, page );
                    latch.countDown();

                    monitor.pin( lock, pageId, swapper );
                    return; // yay!
                }
            }
            else if ( pageRef instanceof CountDownLatch )
            {
                try
                {
                    ((CountDownLatch) pageRef).await();
                }
                catch ( InterruptedException e )
                {
                    Thread.interrupted();
                    throw new IOException( "Interrupted while waiting for page load.", e );
                }
            }
            else
            {
                // happy case where we have a page id
                PinnablePage page = (PinnablePage) pageRef;
                if ( page.pin( swapper, pageId, lock ) )
                {
                    cursor.reset( page, lock );
                    monitor.pin( lock, pageId, swapper );
                    return; // yay!
                }
                filePages.replace( pageId, page, NULL );
            }
        }
    }

    @Override
    public void unpin( PageCursor cursor )
    {
        StandardPageCursor standardCursor = (StandardPageCursor) cursor;
        PageLock lock = standardCursor.lockType();
        PinnablePage page = standardCursor.page();
        long pageId = page.pageId();
        page.unpin( lock );
        monitor.unpin( lock, pageId, swapper );
        standardCursor.reset( null, null );
    }

    @Override
    public int pageSize()
    {
        return filePageSize;
    }

    @Override
    public int numberOfCachedPages()
    {
        return filePages.size();
    }

    /**
     * @return true if this file is still open and we managed to claim a reference to it.
     */
    boolean claimReference()
    {
        int refs;
        do
        {
            refs = references.get();
            if(refs <= 0)
            {
                return false;
            }
        } while( !references.compareAndSet( refs, refs + 1 ));
        return true;
    }

    /**
     * @return true if this was the last reference to the file.
     */
    boolean releaseReference()
    {
        return references.decrementAndGet() == 0;
    }

    // TODO why do we have both this close method, and unmap on the PageCache?
    @Override
    public void close() throws IOException
    {
        force();
        swapper.close();
    }

    @Override
    public void flush() throws IOException
    {
        table.flush( swapper );
        force();
    }

    @Override
    public void force() throws IOException
    {
        swapper.force();
    }

    public long getLastPageId() throws IOException
    {
        return lastPageId.get();
    }

    /**
     * Make sure that the lastPageId is at least the given pageId.
     * @param newLastPageId
     * Make sure that the lastPageId is equal to or greater than this number.
     */
    public long increaseLastPageIdTo( long newLastPageId )
    {
       long current;
       do
       {
           current = lastPageId.get();
       }
       while ( current < newLastPageId
               && !lastPageId.compareAndSet( current, newLastPageId ) );
        return lastPageId.get();
    }
}
