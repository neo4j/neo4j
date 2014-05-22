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
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.io.pagecache.PageLock;

public class StandardPageTable implements PageTable, Runnable
{
    private final AtomicReference<StandardPinnablePage> freeList;
    private final StandardPinnablePage[] pages;
    private volatile Thread sweeperThread;

    public StandardPageTable( int maxPages, int pageSize )
    {
        freeList = new AtomicReference<>();
        pages = new StandardPinnablePage[maxPages];

        for ( int i = 0; i < maxPages; i++ )
        {
            StandardPinnablePage page = new StandardPinnablePage(
                    ByteBuffer.allocateDirect( pageSize ) );
            pages[i] = page;
            page.next = freeList.get();
            freeList.compareAndSet( page.next, page );
        }
    }

    @Override
    public PinnablePage load( PageIO io, long pageId, PageLock lock ) throws IOException
    {
        StandardPinnablePage page = nextFreePage();
        page.reset( io, pageId );
        page.pin( io, pageId, lock );
        page.load();
        return page;
    }

    @Override
    public void flush() throws IOException
    {
        for ( StandardPinnablePage page : pages )
        {
            page.lock( PageLock.SHARED );
            try
            {
                page.flush();
            }
            finally
            {
                page.unlock(PageLock.SHARED);
            }
        }
    }

    private StandardPinnablePage nextFreePage()
    {
        StandardPinnablePage page;
        do {
            page = freeList.get();
            if ( page == null )
            {
                LockSupport.unpark( sweeperThread );
            }
        } while ( page == null || !freeList.compareAndSet( page, page.next ));
        return page;
    }

    /**
     * This is the continuous background page replacement and flushing job.
     * This method runs concurrently with the page loading.
     */
    @Override
    public void run()
    {
        sweeperThread = Thread.currentThread();
        continuouslySweepPages();
        shutDownPageCache();
    }

    private void continuouslySweepPages()
    {
        /**
         * This is the minimum amount of pages to keep around, we will stop
         * evicting pages once we reach this threshold.
         */
        final int minLoadedPages = (int) Math.round(pages.length * 0.96);
        int maxPagesToEvict = Math.max( pages.length - minLoadedPages, 1 );
        int clockHand = 0;

        while ( !Thread.interrupted() )
        {
            try
            {
                int loadedPages = 0;
                for ( ; clockHand < pages.length; clockHand++ )
                {
                    StandardPinnablePage page = pages[clockHand];
                    if ( page.loaded )
                    {
                        loadedPages++;

                        if( page.tryExclusiveLock() )
                        {
                            try
                            {
                                byte stamp = page.usageStamp;
                                if ( stamp == 0 )
                                {
                                    evict( page );
                                    maxPagesToEvict--;
                                    loadedPages--;

                                    // Stop the clock if we're down to a sensible
                                    // number of free pages.
                                    if( maxPagesToEvict <= 0  )
                                    {
                                        break;
                                    }
                                }
                                else
                                {
                                    page.usageStamp = (byte) (stamp - 1);
                                }
                            }
                            finally
                            {
                                page.releaseExclusiveLock();
                            }
                        }
                    }
                }

                // Reset the clock hand if we reached the end of the list of pages.
                if( clockHand >= pages.length )
                {
                    clockHand = 0;
                }

                if( loadedPages <= minLoadedPages )
                {
                    parkUntilEvictionRequired( minLoadedPages );
                }
                maxPagesToEvict = loadedPages - minLoadedPages;
            }
            catch(Exception e)
            {
                // Aviod having this thread crash at all cost.
                // TODO: If we get IOEXception here, we may be failing to flush pages
                // to disk. Need to shut database down if that happens. Perhaps with
                // some retries.
                e.printStackTrace();
            }
        }
    }

    private int parkUntilEvictionRequired( int minLoadedPages )
    {
        int loadedPages;
        outerLoop: do
        {
            LockSupport.parkNanos( TimeUnit.MILLISECONDS.toNanos( 5 ) );

            loadedPages = 0;
            for ( StandardPinnablePage page : pages )
            {
                if(page.loaded)
                {
                    loadedPages++;
                    if( freeList.get() == null )
                    {
                        // Bail out immediately if the free list is empty.
                        break outerLoop;
                    }
                }
            }
        } while( loadedPages <= minLoadedPages );
        return loadedPages;
    }

    private void evict( StandardPinnablePage page ) throws IOException
    {
        page.flush();
        page.evicted();
        page.reset( null, 0 );
        page.loaded = false;
        do {
            page.next = freeList.get();
        } while ( !freeList.compareAndSet( page.next, page ) );
    }

    private void shutDownPageCache()
    {

    }
}
