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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.io.pagecache.PageLock;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import static org.neo4j.io.pagecache.impl.standard.RecordingMonitor.Fault;
import static org.neo4j.io.pagecache.impl.standard.RecordingMonitor.Evict;

public class ClockSweepPageTableTest
{
    private static final int TEST_PAGE_SIZE = 1024;
    private final byte[] bytesA = "the A bytes".getBytes();
    private final byte[] bytesB = "the B bytes".getBytes();

    private RecordingMonitor monitor;
    private ClockSweepPageTable table;
    private Thread sweeperThread;

    @Before
    public void startPageTable()
    {
        monitor = new RecordingMonitor();
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
        BufferPageIO io = new BufferPageIO( bytesA );

        // When
        PinnablePage page = table.load( io, 1, PageLock.EXCLUSIVE );

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
        BufferPageIO io = new BufferPageIO( storageBuffer );

        PinnablePage page = table.load( io, 12, PageLock.EXCLUSIVE );
        page.putBytes( bytesA, 0 );
        page.unpin( PageLock.EXCLUSIVE );

        // When I perform an operation that will force eviction
        Thread thread = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                // This thread will cause the single page in the cache to be replaced
                BufferPageIO io = new BufferPageIO( ByteBuffer.allocate( 1 ) );
                try
                {
                    table.load( io, 3, PageLock.SHARED );
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }
            }
        });
        thread.start();
        thread.join();

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
        BufferPageIO io = new BufferPageIO( bytesA );

        // When
        PinnablePage page = table.load( io, 12, PageLock.SHARED );

        // Then we should be able to grab another shared lock on it
        assertTrue( page.pin( io, 12, PageLock.SHARED ) );
    }

    @Test(timeout = 1000)
    public void loading_with_shared_lock_stops_exclusive_pinners() throws Exception
    {
        // Given
        final BufferPageIO io = new BufferPageIO( bytesA );

        // When
        final PinnablePage page = table.load( io, 12, PageLock.SHARED );

        // Then we should have to wait for the page to be unpinned if we want an
        // exclusive lock on it.
        final AtomicBoolean acquiredPage = new AtomicBoolean( false );
        Thread otherThread = fork( new Runnable()
        {
            @Override
            public void run()
            {
                page.pin( io, 12, PageLock.EXCLUSIVE );
                acquiredPage.set( true );
            }
        } );
        awaitThreadState( otherThread, Thread.State.WAITING );

        assertFalse( acquiredPage.get() );

        // And when I unpin mine, the other thread should get it
        page.unpin( PageLock.SHARED );

        otherThread.join();

        assertTrue( acquiredPage.get() );
    }

    @Test(timeout = 1000)
    public void loading_with_exclusive_lock_stops_all_others() throws Exception
    {
        // Given
        ClockSweepPageTable table = new ClockSweepPageTable( 1, TEST_PAGE_SIZE, StandardPageCache.NO_MONITOR );
        final BufferPageIO io = new BufferPageIO( bytesA );

        // When
        final PinnablePage page = table.load( io, 12, PageLock.EXCLUSIVE );

        // Then we should have to wait for the page to be unpinned if we want an
        // exclusive lock on it.
        final AtomicBoolean acquiredPage = new AtomicBoolean( false );
        Thread otherThread = fork( new Runnable()
        {
            @Override
            public void run()
            {
                page.pin( io, 12, PageLock.SHARED );
                acquiredPage.set( true );
            }
        } );
        awaitThreadState( otherThread, Thread.State.WAITING );

        assertFalse( acquiredPage.get() );

        // And when I unpin mine, the other thread should get it
        page.unpin( PageLock.EXCLUSIVE );

        otherThread.join();

        assertTrue( acquiredPage.get() );
    }

    @Test(timeout = 1000)
    public void pinning_replaced_page_must_fail() throws Exception
    {
        // Given
        BufferPageIO io = new BufferPageIO( bytesA );

        PinnablePage page = table.load( io, 12, PageLock.SHARED );
        page.unpin( PageLock.SHARED );

        // When
        Thread thread = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                // This thread will cause the single page in the cache to be replaced
                BufferPageIO io = new BufferPageIO( ByteBuffer.wrap( bytesB ) );
                try
                {
                    table.load( io, 3, PageLock.SHARED ).unpin( PageLock.SHARED );
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }
            }
        });
        thread.start();
        thread.join();

        // Then
        assertFalse( page.pin( io, 12, PageLock.SHARED ) );
    }

    @Test
    public void must_notify_io_object_on_eviction() throws Exception
    {
        // Given
        BufferPageIO io = spy(new BufferPageIO( bytesA ));

        PinnablePage page = table.load( io, 12, PageLock.SHARED );
        page.unpin( PageLock.SHARED );

        // When
        Thread thread = fork( new Runnable()
        {
            @Override
            public void run()
            {
                // This thread will cause the single page in the cache to be replaced
                BufferPageIO io = new BufferPageIO( ByteBuffer.allocate( TEST_PAGE_SIZE ) );
                try
                {
                    table.load( io, 3, PageLock.SHARED ).unpin( PageLock.SHARED );
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
        PageIO io = new BufferPageIO( ByteBuffer.allocate( TEST_PAGE_SIZE ) );
        long pageId = 12;
        PinnablePage page = table.load( io, pageId, PageLock.EXCLUSIVE );
        page.unpin( PageLock.EXCLUSIVE );

        // ... then we should observe its page fault
        assertThat( monitor.observeFault(), is( new Fault( io, pageId ) ) );

        // ... and when it sits idle for long enough, we should observe its eviction
        assertThat( monitor.observeEvict(), is( new Evict( io, pageId ) ) );
    }

    @Test( timeout = 1000 )
    public void readers_and_writers_must_block_on_evicting_page() throws Exception
    {
        // If we have a loaded page ...
        PageIO io = new BufferPageIO( ByteBuffer.allocate( TEST_PAGE_SIZE ) );
        long pageId = 12;
        PinnablePage page = table.load( io, pageId, PageLock.EXCLUSIVE );
        monitor.observeFault();

        // ... a page that will take a long time to evict
        CountDownLatch latch = monitor.trap( new Evict( io, pageId ) );

        // ... and a page that is soon up for eviction
        page.unpin( PageLock.EXCLUSIVE );

        // ... then when we observe the eviction taking place
        monitor.observeEvict();

        // ... other threads should not be able to pin that page
        Thread pinForShared = fork( $pinUnpin( page, io, pageId, PageLock.SHARED ) );
        Thread pinForExclusive = fork( $pinUnpin( page, io, pageId, PageLock.EXCLUSIVE ) );
        awaitThreadState( pinForShared, Thread.State.WAITING );
        awaitThreadState( pinForExclusive, Thread.State.WAITING );

        // ... until the eviction finishes
        latch.countDown();
        pinForShared.join();
        pinForExclusive.join();
    }

    // TODO closing a paged file will flush with a particular PageIO - this must not race with eviction!

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
            final PageIO io,
            final long pageId,
            final PageLock pageLock )
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                if ( page.pin( io, pageId, pageLock ) )
                {
                    page.unpin( pageLock );
                }
            }
        };
    }

    private class BufferPageIO implements PageIO
    {
        private final ByteBuffer buffer;

        private BufferPageIO( byte[] bytes )
        {
            this( ByteBuffer.wrap( bytes ) );
        }

        private BufferPageIO( ByteBuffer buffer )
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
