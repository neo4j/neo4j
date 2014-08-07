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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.io.pagecache.PageCacheMonitor;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.PagedFile;

import static org.neo4j.io.pagecache.PageCursor.UNBOUND_PAGE_ID;

/**
 * An implementation of {@link org.neo4j.io.pagecache.impl.standard.PageTable} which uses
 * a variant of the ClockSweep algorithm to attempt to keep popular pages in RAM based on
 * recency and frequency of use.
 *
 * It uses a background thread to run eviction, trying to optimize for allowing new page-ins
 * to immediately have free pages available to be populated, while still keeping as many pages
 * as possible live in RAM.
 */
public class ClockSweepPageTable implements PageTable, Runnable
{
    /**
     * Only evict pages if more than this percentage is used.
     * For instance, if the ratio is 0.96, then the eviction thread will only do work
     * if more than 96% of the pages in the cache are in use.
     */
    static final double PAGE_UTILISATION_RATIO = getDouble(
            "org.neo4j.io.pagecache.impl.standard.ClockSweepPageTable.pageUtilisationRatio", 0.96 );

    private static double getDouble( String propertyName, double defaultValue )
    {
        String property = System.getProperty( propertyName );
        if ( property == null )
        {
            return defaultValue;
        }
        return Double.parseDouble( property );
    }

    private final AtomicReference<StandardPinnablePage> freeList;
    private final StandardPinnablePage[] pages;
    private final int pageSize;
    private final PageCacheMonitor monitor;

    private volatile Thread sweeperThread;
    private volatile IOException sweeperException;

    public ClockSweepPageTable( int maxPages, int pageSize, PageCacheMonitor monitor )
    {
        this.pageSize = pageSize;
        this.monitor = monitor;
        freeList = new AtomicReference<>();
        pages = new StandardPinnablePage[maxPages];

        for ( int i = 0; i < maxPages; i++ )
        {
            StandardPinnablePage page = new StandardPinnablePage( pageSize );
            pages[i] = page;
            page.next = freeList.get();
            freeList.compareAndSet( page.next, page );
        }
    }

    @Override
    public PinnablePage load( PageSwapper io, long pageId, int pf_flags ) throws IOException
    {
        StandardPinnablePage page = nextFreePage();
        if ( page.pin( null, UNBOUND_PAGE_ID, pf_flags ) )
        {
            page.reset( io, pageId );
            page.load();
            monitor.pageFault( pageId, io );
        }
        else
        {
            throw new IOException(
                    "Tried to load a page (page fault) but the page in the free-list " +
                    "was already bound to a file and a pageId: " + page );
        }
        return page;
    }

    private StandardPinnablePage nextFreePage() throws IOException
    {
        StandardPinnablePage page;
        do {
            page = freeList.get();
            if ( page == null )
            {
                if ( sweeperException != null )
                {
                    throw new IOException( sweeperException );
                }
                LockSupport.unpark( sweeperThread );
            }
        } while ( page == null || !freeList.compareAndSet( page, page.next ));
        return page;
    }

    @Override
    public void flush() throws IOException
    {
        assertNoSweeperException();
        for ( StandardPinnablePage page : pages )
        {
            page.lock(PagedFile.PF_SHARED_LOCK);
            try
            {
                page.flush( monitor );
            }
            finally
            {
                page.unlock( PagedFile.PF_SHARED_LOCK);
            }
        }
    }

    @Override
    public void flush( PageSwapper io ) throws IOException
    {
        assertNoSweeperException();
        for ( StandardPinnablePage page : pages )
        {
            page.lock( PagedFile.PF_SHARED_LOCK );
            try
            {
                if( page.isBackedBy( io ) )
                {
                    assertNoSweeperException();
                    page.flush( monitor );
                }
            }
            finally
            {
                page.unlock( PagedFile.PF_SHARED_LOCK );
            }
        }
    }

    private void assertNoSweeperException() throws IOException
    {
        if ( sweeperException != null )
        {
            throw new IOException( "Cannot safely flush, page eviction thread has hit IO problems.", sweeperException );
        }
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
    }

    /**
     * This is the eviction algorithm, run in a background thread.
     */
    private void continuouslySweepPages()
    {
        /**
         * This is the minimum amount of pages to keep around, we will stop
         * evicting pages once we reach this threshold.
         */
        final int minLoadedPages = (int) Math.round( pages.length * PAGE_UTILISATION_RATIO );
        int maxPagesToEvict = Math.max( pages.length - minLoadedPages, 1 );
        int clockHand = 0;

        while ( !Thread.interrupted() )
        {
            try
            {
                // A rather strange loop, it is done like this to allow us to stop in the middle of the movement
                // of the "clock hand", and then come back to this for loop and pick up where we left off.
                int loadedPages = 0;
                for ( ; clockHand < pages.length; clockHand++ )
                {
                    StandardPinnablePage page = pages[clockHand];
                    if ( page.loaded )
                    {
                        loadedPages++;

                        // If we can't lock the page, someone is using it, and there is no reason for us to bother.
                        if( page.tryExclusiveLock() )
                        {
                            try
                            {
                                // If this pages usageStamp has reached 0, it's time for it to leave the party.
                                byte stamp = page.usageStamp;
                                if ( stamp == 0 )
                                {
                                    evict( page );

                                    maxPagesToEvict--;
                                    loadedPages--;

                                    // Stop the clock if we're down to a sensible number of free pages.
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
            catch ( IOException e )
            {
                // Failed to write to disk, this is fatal, shut down.
                sweeperException = e;
                return;
            }
            catch ( Exception e )
            {
                // Other than IO Exception, avoid having this thread die at all cost.
                // TODO: Report this via a monitor rather than like this
                e.printStackTrace();
            }
        }
    }

    private void evict( StandardPinnablePage page ) throws IOException
    {
        long pageId = page.pageId();
        PageSwapper swapper = page.swapper();

        page.flush( monitor );
        page.setAllBytesToZero();
        page.evicted();
        page.reset( null, UNBOUND_PAGE_ID );
        page.loaded = false;
        do {
            page.next = freeList.get();
        } while ( !freeList.compareAndSet( page.next, page ) );
        monitor.evict( pageId, swapper );
    }

    private void parkUntilEvictionRequired( int minLoadedPages )
    {
        int loadedPages;
        outerLoop: do
        {
            LockSupport.parkNanos( TimeUnit.MILLISECONDS.toNanos( 10 ) );
            if(Thread.currentThread().isInterrupted() || freeList.get() == null)
            {
                return;
            }

            loadedPages = 0;
            for ( StandardPinnablePage page : pages )
            {
                if ( page.loaded )
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
    }

    @Override
    public int pageSize()
    {
        return pageSize;
    }

    @Override
    public int maxCachedPages()
    {
        return pages.length;
    }
}
