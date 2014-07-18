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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.io.pagecache.PageCacheMonitor;
import org.neo4j.io.pagecache.PageLock;
import org.neo4j.io.pagecache.PagedFile;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import static org.neo4j.io.pagecache.impl.standard.RecordingPageCacheMonitor.Fault;
import static org.neo4j.io.pagecache.impl.standard.RecordingPageCacheMonitor.Evict;

public class ClockSweepPageTableTest
{
    private static final int TEST_PAGE_SIZE = 1024;
    private final byte[] bytesA = "the A bytes".getBytes();
    private final byte[] bytesB = "the B bytes".getBytes();

    private RecordingPageCacheMonitor monitor;
    private ClockSweepPageTable table;
    private Thread sweeperThread;

    @Before
    public void startPageTable()
    {
        monitor = new RecordingPageCacheMonitor();
        table = new ClockSweepPageTable( 1, TEST_PAGE_SIZE, monitor );
        sweeperThread = new Thread( table );
        sweeperThread.start();
    }

    @After
    public void stopPageTable()
    {
        sweeperThread.interrupt();
    }

    @Test
    public void loading_must_read_file() throws Exception
    {
        // Given
        BufferPageSwapper io = new BufferPageSwapper( bytesA );

        // When
        PinnablePage page = table.load( io, 1, PagedFile.PF_EXCLUSIVE_LOCK );

        // Then
        byte[] actual = new byte[bytesA.length];
        page.getBytes( actual, 0 );

        assertThat( actual, equalTo( bytesA ) );
    }

    @Test
    public void evicting_must_flush_file() throws Exception
    {
        // Given a table with 1 entry, which I've modified
        ByteBuffer storageBuffer = ByteBuffer.allocate( TEST_PAGE_SIZE );
        BufferPageSwapper io = new BufferPageSwapper( storageBuffer );

        PinnablePage page = table.load( io, 12, PagedFile.PF_EXCLUSIVE_LOCK );
        page.putBytes( bytesA, 0 );
        page.unpin( PagedFile.PF_EXCLUSIVE_LOCK );

        // When I perform an operation that will force eviction
        fork(new Runnable()
        {
            @Override
            public void run()
            {
                // This thread will cause the single page in the cache to be replaced
                BufferPageSwapper io = new BufferPageSwapper( ByteBuffer.allocate( 1 ) );
                try
                {
                    table.load( io, 3, PagedFile.PF_SHARED_LOCK );
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }
            }
        }).join();

        // Then my changes should've been forced to disk
        byte[] actual = new byte[bytesA.length];
        storageBuffer.position(0);
        storageBuffer.get( actual );
        assertThat( actual, equalTo( bytesA ) );
    }

    @Test
    public void loading_with_shared_lock_allows_other_shared() throws Exception
    {
        // Given
        BufferPageSwapper io = new BufferPageSwapper( bytesA );

        // When
        PinnablePage page = table.load( io, 12, PagedFile.PF_SHARED_LOCK );

        // Then we should be able to grab another shared lock on it
        assertTrue( page.pin( io, 12, PagedFile.PF_SHARED_LOCK ) );
    }

    @Test(timeout = 1000)
    public void loading_with_shared_lock_stops_exclusive_pinners() throws Exception
    {
        // Given
        final BufferPageSwapper io = new BufferPageSwapper( bytesA );

        // When
        final PinnablePage page = table.load( io, 12, PagedFile.PF_SHARED_LOCK );

        // Then we should have to wait for the page to be unpinned if we want an
        // exclusive lock on it.
        final AtomicBoolean acquiredPage = new AtomicBoolean( false );
        Thread otherThread = fork( new Runnable()
        {
            @Override
            public void run()
            {
                page.pin( io, 12, PagedFile.PF_EXCLUSIVE_LOCK );
                acquiredPage.set( true );
            }
        } );
        awaitThreadState( otherThread, Thread.State.WAITING );

        assertFalse( acquiredPage.get() );

        // And when I unpin mine, the other thread should get it
        page.unpin( PagedFile.PF_SHARED_LOCK );

        otherThread.join();

        assertTrue( acquiredPage.get() );
    }

    @Test(timeout = 1000)
    public void loading_with_exclusive_lock_stops_all_others() throws Exception
    {
        // Given
        ClockSweepPageTable table = new ClockSweepPageTable( 1, TEST_PAGE_SIZE, PageCacheMonitor.NULL );
        final BufferPageSwapper io = new BufferPageSwapper( bytesA );

        // When
        final PinnablePage page = table.load( io, 12, PagedFile.PF_EXCLUSIVE_LOCK );

        // Then we should have to wait for the page to be unpinned if we want an
        // exclusive lock on it.
        final AtomicBoolean acquiredPage = new AtomicBoolean( false );
        Thread otherThread = fork( new Runnable()
        {
            @Override
            public void run()
            {
                page.pin( io, 12, PagedFile.PF_SHARED_LOCK );
                acquiredPage.set( true );
            }
        } );
        awaitThreadState( otherThread, Thread.State.WAITING );

        assertFalse( acquiredPage.get() );

        // And when I unpin mine, the other thread should get it
        page.unpin( PagedFile.PF_EXCLUSIVE_LOCK );

        otherThread.join();

        assertTrue( acquiredPage.get() );
    }

    @Test(timeout = 1000)
    public void pinning_replaced_page_must_fail() throws Exception
    {
        // Given
        BufferPageSwapper io = new BufferPageSwapper( bytesA );

        PinnablePage page = table.load( io, 12, PagedFile.PF_SHARED_LOCK );
        page.unpin( PagedFile.PF_SHARED_LOCK );

        // When
        fork(new Runnable()
        {
            @Override
            public void run()
            {
                // This thread will cause the single page in the cache to be replaced
                BufferPageSwapper io = new BufferPageSwapper( ByteBuffer.wrap( bytesB ) );
                try
                {
                    table.load( io, 3, PagedFile.PF_SHARED_LOCK ).unpin( PagedFile.PF_SHARED_LOCK );
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }
            }
        }).join();

        // Then
        assertFalse( page.pin( io, 12, PagedFile.PF_SHARED_LOCK ) );
    }

    @Test
    public void must_notify_io_object_on_eviction() throws Exception
    {
        // Given
        BufferPageSwapper io = spy(new BufferPageSwapper( bytesA ));

        PinnablePage page = table.load( io, 12, PagedFile.PF_SHARED_LOCK );
        page.unpin( PagedFile.PF_SHARED_LOCK );

        // When
        Thread thread = fork( new Runnable()
        {
            @Override
            public void run()
            {
                // This thread will cause the single page in the cache to be replaced
                BufferPageSwapper io = new BufferPageSwapper( ByteBuffer.allocate( TEST_PAGE_SIZE ) );
                try
                {
                    table.load( io, 3, PagedFile.PF_SHARED_LOCK ).unpin( PagedFile.PF_SHARED_LOCK );
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }
            }
        } );
        thread.join();

        // Then
        verify( io ).evicted( 12 );
    }

    @Test
    public void must_notify_monitor_of_evicted_pages() throws Exception
    {
        // If we load a page ...
        PageSwapper io = new BufferPageSwapper( ByteBuffer.allocate( TEST_PAGE_SIZE ) );
        long pageId = 12;
        PinnablePage page = table.load( io, pageId, PagedFile.PF_EXCLUSIVE_LOCK );
        page.unpin( PagedFile.PF_EXCLUSIVE_LOCK );

        // ... then we should observe its page fault
        assertThat( monitor.observe( Fault.class ), is( new Fault( io, pageId ) ) );

        // ... and when it sits idle for long enough, we should observe its eviction
        assertThat( monitor.observe( Evict.class ), is( new Evict( io, pageId ) ) );
    }

    @Test( timeout = 1000 )
    public void readers_and_writers_must_block_on_evicting_page() throws Exception
    {
        // If we have a loaded page ...
        PageSwapper io = new BufferPageSwapper( ByteBuffer.allocate( TEST_PAGE_SIZE ) );
        long pageId = 12;
        PinnablePage page = table.load( io, pageId, PagedFile.PF_EXCLUSIVE_LOCK );
        monitor.observe( Fault.class );

        // ... a page that will take a long time to evict
        CountDownLatch latch = monitor.trap( is( new Evict( io, pageId ) ) );

        // ... and a page that is soon up for eviction
        page.unpin( PagedFile.PF_EXCLUSIVE_LOCK );

        // ... then when we observe the eviction taking place
        monitor.observe( Evict.class );

        // ... other threads should not be able to pin that page
        Thread pinForShared = fork( $pinUnpin( page, io, pageId, PagedFile.PF_SHARED_LOCK ) );
        Thread pinForExclusive = fork( $pinUnpin( page, io, pageId, PagedFile.PF_EXCLUSIVE_LOCK ) );
        awaitThreadState( pinForShared, Thread.State.WAITING );
        awaitThreadState( pinForExclusive, Thread.State.WAITING );

        // ... until the eviction finishes
        latch.countDown();
        pinForShared.join();
        pinForExclusive.join();
    }

    @Test
    public void flushing_pages_with_specific_pageio_must_not_race_with_eviction() throws Exception
    {
        // The idea is that we repeatedly load a page with an EXCLUSIVE lock, and unpin it so it can
        // be evicted. As soon as we have unpinned, we repeatedly try to flush it with our given
        // PageIO. If this causes an exception to be thrown, then we've raced with the eviction
        // where we shouldn't.
        PageSwapper io = new BufferPageSwapper( ByteBuffer.allocate( TEST_PAGE_SIZE ) );
        long pageId = 12;

        PinnablePage page = table.load( io, pageId, PagedFile.PF_EXCLUSIVE_LOCK );
        monitor.observe( Fault.class );
        page.unpin( PagedFile.PF_EXCLUSIVE_LOCK ); // eviction is now possible
        LockSupport.unpark( sweeperThread );

        while ( monitor.tryObserve( Evict.class ) == null )
        {
            table.flush( io );
        }
    }


    private void awaitThreadState( Thread otherThread, Thread.State state ) throws InterruptedException
    {
        while( otherThread.getState() != state )
        {
            otherThread.join( 1 );
        }
    }

    private Thread fork( Runnable runnable )
    {
        Thread thread = new Thread( runnable );
        thread.start();
        return thread;
    }

    private Runnable $pinUnpin(
            final PinnablePage page,
            final PageSwapper io,
            final long pageId,
            final int pf_flags )
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                if ( page.pin( io, pageId, pf_flags ) )
                {
                    page.unpin( pf_flags );
                }
            }
        };
    }

    private class BufferPageSwapper implements PageSwapper
    {
        private final ByteBuffer buffer;

        private BufferPageSwapper( byte[] bytes )
        {
            this( ByteBuffer.wrap( bytes ) );
        }

        private BufferPageSwapper( ByteBuffer buffer )
        {
            this.buffer = buffer;
        }

        @Override
        public void read( long pageId, ByteBuffer into )
        {
            buffer.position( 0 );
            into.put( buffer );
        }

        @Override
        public void write( long pageId, ByteBuffer from )
        {
            buffer.position( 0 );
            buffer.put(from);
        }

        @Override
        public void evicted( long pageId )
        {

        }

        @Override
        public String fileName()
        {
            return buffer.toString();
        }
    }
}
