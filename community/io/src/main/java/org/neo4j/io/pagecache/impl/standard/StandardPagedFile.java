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

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageLock;
import org.neo4j.io.pagecache.PagedFile;

import static org.neo4j.io.pagecache.impl.standard.PageTable.PinnablePage;

public class StandardPagedFile implements PagedFile
{
    static final Object NULL = new Object();

    private final PageTable table;
    private final StoreChannel channel;
    private final int filePageSize;
    private final PageTable.PageIO pageIO;

    private final AtomicInteger references = new AtomicInteger( 1 );

    /** Currently active pages in the file this object manages. */
    private ConcurrentMap<Long, Object> filePages = new ConcurrentHashMap<>();

    /**
     * @param table
     * @param channel
     * @param filePageSize is the page size used by this file, NOT the page size used by
     *                     the cache. This value is always smaller than the page size used
     *                     by the cache. The remaining space in the page cache buffers are
     *                     unused.
     */
    public StandardPagedFile( PageTable table, StoreChannel channel, int filePageSize )
    {
        this.table = table;
        this.channel = channel;
        this.filePageSize = filePageSize;
        this.pageIO = new StandardPageIO(channel, filePageSize);
    }

    @Override
    public void pin( PageCursor cursor, PageLock lock, long pageId ) throws IOException
    {
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
                    PinnablePage page = table.load( pageIO, pageId, lock );
                    filePages.put( pageId, page );
                    latch.countDown();

                    ((StandardPageCursor)cursor).reset( page, lock );

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
                if ( page.pin( pageIO, pageId, lock ) )
                {
                    ((StandardPageCursor)cursor).reset( page, lock );
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
        standardCursor.page().unpin( standardCursor.lockType() );
        standardCursor.reset( null, null );
    }

    @Override
    public int pageSize()
    {
        return filePageSize;
    }

    /**
     * @return true if this file is still open and we managed to claim a reference to it.
     */
    public boolean claimReference()
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
    public boolean releaseReference()
    {
        return references.decrementAndGet() == 0;
    }

    public void close() throws IOException
    {
        channel.close();
    }
}
