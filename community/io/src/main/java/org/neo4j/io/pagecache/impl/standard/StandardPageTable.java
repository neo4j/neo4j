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

    public StandardPageTable( long memoryPoolSizeInBytes )
    {
        freeList = new AtomicReference<>();
        int pageSize = 1024;
        int pageCount = (int) (memoryPoolSizeInBytes / pageSize);
        pages = new StandardPinnablePage[pageCount];

        for ( int i = 0; i < pageCount; i++ )
        {
            StandardPinnablePage page = new StandardPinnablePage(
                    ByteBuffer.allocateDirect( pageSize ) );
            pages[i] = page;
            page.next = freeList.get();
            freeList.compareAndSet( page.next, page );
        }
    }

    @Override
    public PinnablePage load( PageIO io, long pageId, PageLock lock )
    {
        StandardPinnablePage page = nextFreePage();
        page.reset( io, pageId );
        page.pin( io, pageId, lock );
        io.read( pageId, page.buffer() );
        return page;
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
        while ( !Thread.interrupted() )
        {
            int useSumTotal = 0;
            for ( StandardPinnablePage page : pages )
            {
                if ( page.grabUnpinned() )
                {
                    try
                    {
                        byte stamp = page.usageStamp;
                        useSumTotal += stamp;
                        if ( stamp == 0 )
                        {
                            evict( page );
                        }
                        else
                        {
                            page.usageStamp = (byte) (stamp - 1);
                        }
                    }
                    finally
                    {
                        page.releaseUnpinned();
                    }
                }
            }
            LockSupport.parkNanos( TimeUnit.MILLISECONDS.toNanos( 10 ) );
        }
    }

    private void evict( StandardPinnablePage page )
    {
        page.flush();
        page.reset( null, 0 );
        do {
            page.next = freeList.get();
        } while ( !freeList.compareAndSet( page.next, page ) );
    }

    private void shutDownPageCache()
    {

    }
}
