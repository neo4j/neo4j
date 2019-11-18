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
package org.neo4j.consistency.newchecker;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.test.Barrier;
import org.neo4j.test.Race;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.consistency.newchecker.StorePagePrefetcher.NO_MONITOR;
import static org.neo4j.test.Race.throwing;

class StorePagePrefetcherTest
{
    private static final int RECORDS_PER_PAGE = 10;

    @Test
    void shouldForwardPrefetchAllPagesWithinReadAheadSize() throws IOException
    {
        // given
        int numberOfPages = 3;
        PageCursor cursor = mockedTrackingCursor( numberOfPages );
        CommonAbstractStore<?,?> store = mockedStore( cursor, RECORDS_PER_PAGE, numberOfPages );
        int readAheadSize = 100; // pages
        StorePagePrefetcher prefetch = new StorePagePrefetcher( store, readAheadSize, () -> false, NO_MONITOR );

        // when
        prefetch.prefetch( () -> 0, true );

        // then
        InOrder inOrder = inOrder( cursor );
        inOrder.verify( cursor, times( numberOfPages + 1 /*last one says false*/ ) ).next();
    }

    @Test
    void shouldForwardPrefetchAllPagesAwaitingReader() throws IOException, ExecutionException, InterruptedException
    {
        // given
        int numberOfPages = 350;
        PageCursor cursor = mockedTrackingCursor( numberOfPages );
        CommonAbstractStore<?,?> store = mockedStore( cursor, RECORDS_PER_PAGE, numberOfPages );
        int readAheadSize = 100; // pages
        ControlledMonitor monitor = new ControlledMonitor();
        StorePagePrefetcher prefetch = new StorePagePrefetcher( store, readAheadSize, () -> false, monitor );

        // when/then
        AtomicLong readerAtPage = new AtomicLong( -1 );
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> job = executor.submit( throwing( () -> prefetch.prefetch( readerAtPage::get, true ) ) );
        for ( int i = 0; i < 3; i++ )
        {
            monitor.barrier.awaitUninterruptibly();
            verify( cursor, times( (i + 1) * readAheadSize ) ).next();
            readerAtPage.addAndGet( readAheadSize );
            monitor.releaseAndRecreateBarrier();
        }
        job.get();
        verify( cursor, times( numberOfPages + 1 /*last one says false*/ ) ).next();
        executor.shutdown();
    }

    @Test
    void shouldCancelForwardPrefetch() throws IOException
    {
        // given
        int numberOfPages = 350;
        PageCursor cursor = mockedTrackingCursor( numberOfPages );
        CommonAbstractStore<?,?> store = mockedStore( cursor, RECORDS_PER_PAGE, numberOfPages );
        int readAheadSize = 100; // pages
        ControlledMonitor monitor = new ControlledMonitor();
        AtomicBoolean cancelled = new AtomicBoolean( false );
        StorePagePrefetcher prefetch = new StorePagePrefetcher( store, readAheadSize, cancelled::get, monitor );

        // when
        AtomicLong readerAtPage = new AtomicLong( -1 );
        Race race = new Race().withRandomStartDelays();
        race.addContestant( throwing( () -> prefetch.prefetch( readerAtPage::get, true ) ) );
        race.addContestant( () ->
        {
            cancelled.set( true );
            monitor.releaseAndRecreateBarrier();
        } );
        race.goUnchecked();

        // then
        assertTrue( cursor.getCurrentPageId() >= 0 && cursor.getCurrentPageId() <= readAheadSize );
    }

    @Test
    void shouldBackwardPrefetchAllPagesWithinReadAheadSize() throws IOException
    {
        // given
        int numberOfPages = 3;
        PageCursor cursor = mockedTrackingCursor( numberOfPages );
        CommonAbstractStore<?,?> store = mockedStore( cursor, RECORDS_PER_PAGE, numberOfPages );
        int readAheadSize = 100; // pages
        StorePagePrefetcher prefetch = new StorePagePrefetcher( store, readAheadSize, () -> false, NO_MONITOR );

        // when
        prefetch.prefetch( () -> 0, false );

        // then
        InOrder inOrder = inOrder( cursor );
        inOrder.verify( cursor, times( 1 ) ).next( 0 );
        inOrder.verify( cursor, times( numberOfPages ) ).next();
    }

    @Test
    void shouldBackwardPrefetchAllPagesAwaitingReader() throws IOException, ExecutionException, InterruptedException
    {
        // given
        int numberOfPages = 350;
        PageCursor cursor = mockedTrackingCursor( numberOfPages );
        CommonAbstractStore<?,?> store = mockedStore( cursor, RECORDS_PER_PAGE, numberOfPages );
        int readAheadSize = 100; // pages
        ControlledMonitor monitor = new ControlledMonitor();
        StorePagePrefetcher prefetch = new StorePagePrefetcher( store, readAheadSize, () -> false, monitor );

        // when/then
        AtomicLong readerAtPage = new AtomicLong( numberOfPages );
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> job = executor.submit( throwing( () -> prefetch.prefetch( readerAtPage::get, false ) ) );
        for ( int i = 0; i < 3; i++ )
        {
            monitor.barrier.awaitUninterruptibly();
            verify( cursor, times( (i + 1) * readAheadSize ) ).next();
            readerAtPage.addAndGet( -readAheadSize );
            monitor.releaseAndRecreateBarrier();
        }
        job.get();
        verify( cursor, times( 1 ) ).next( 250 );
        verify( cursor, times( 1 ) ).next( 150 );
        verify( cursor, times( 1 ) ).next( 50 );
        verify( cursor, times( 1 ) ).next( 0 );
        verify( cursor, times( numberOfPages ) ).next();
        executor.shutdown();
    }

    @Test
    void shouldCancelBackwardPrefetch() throws IOException
    {
        // given
        int numberOfPages = 350;
        PageCursor cursor = mockedTrackingCursor( numberOfPages );
        CommonAbstractStore<?,?> store = mockedStore( cursor, RECORDS_PER_PAGE, numberOfPages );
        int readAheadSize = 100; // pages
        ControlledMonitor monitor = new ControlledMonitor();
        AtomicBoolean cancelled = new AtomicBoolean( false );
        StorePagePrefetcher prefetch = new StorePagePrefetcher( store, readAheadSize, cancelled::get, monitor );

        // when
        AtomicLong readerAtPage = new AtomicLong( numberOfPages );
        Race race = new Race().withRandomStartDelays();
        race.addContestant( throwing( () -> prefetch.prefetch( readerAtPage::get, false ) ) );
        race.addContestant( () ->
        {
            while ( cursor.getCurrentPageId() == -1 )
            {
                Thread.onSpinWait();
            }
            cancelled.set( true );
            monitor.releaseAndRecreateBarrier();
        } );
        race.goUnchecked();

        // then
        long currentPageId = cursor.getCurrentPageId();
        assertTrue( currentPageId >= numberOfPages - readAheadSize && currentPageId <= numberOfPages );
    }

    private CommonAbstractStore<?,?> mockedStore( PageCursor cursor, int recordsPerPage, int numberOfPages )
    {
        long highId = recordsPerPage * numberOfPages;
        CommonAbstractStore<?,?> store = mock( CommonAbstractStore.class );
        when( store.getRecordsPerPage() ).thenReturn( recordsPerPage );
        when( store.getHighId() ).thenReturn( highId );
        when( store.openPageCursorForReading( anyLong() ) ).thenReturn( cursor );
        return store;
    }

    // Basically wraps an AtomicLong
    private PageCursor mockedTrackingCursor( int numberOfPages ) throws IOException
    {
        long lastPageId = numberOfPages - 1;
        AtomicLong cursorPageId = new AtomicLong( -1 );
        PageCursor cursor = mock( PageCursor.class );
        when( cursor.next( anyLong() ) ).thenAnswer( invocationOnMock ->
        {
            cursorPageId.set( invocationOnMock.getArgument( 0, Long.class ) );
            return cursorPageId.get() >= 0 && cursorPageId.get() <= lastPageId;
        } );
        when( cursor.next() ).thenAnswer( invocationOnMock ->
        {
            cursorPageId.incrementAndGet();
            return cursorPageId.get() >= 0 && cursorPageId.get() <= lastPageId;
        } );
        when( cursor.getCurrentPageId() ).thenAnswer( invocationOnMock -> cursorPageId.get() );
        return cursor;
    }

    private static class ControlledMonitor implements StorePagePrefetcher.Monitor
    {
        private volatile Barrier.Control barrier = new Barrier.Control();

        @Override
        public void awaitingReader()
        {
            barrier.reached();
        }

        void releaseAndRecreateBarrier()
        {
            Barrier.Control oldBarrier = barrier;
            barrier = new Barrier.Control();
            oldBarrier.release();
        }
    }
}
