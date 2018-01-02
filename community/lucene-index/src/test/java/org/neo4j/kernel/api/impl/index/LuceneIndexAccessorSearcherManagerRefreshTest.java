/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.index;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.kernel.api.index.IndexUpdater;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.impl.api.index.IndexUpdateMode.ONLINE;

public class LuceneIndexAccessorSearcherManagerRefreshTest
{
    private final LuceneDocumentStructure structure = mock( LuceneDocumentStructure.class );
    private final ReservingLuceneIndexWriter writer = mock( ReservingLuceneIndexWriter.class );
    private final Directory directory = mock( Directory.class );
    private final File dir = mock( File.class );

    private final AtomicLong count = new AtomicLong( 0 );
    private final LuceneReferenceManagerAdapter<IndexSearcher> manager =
            new LuceneReferenceManagerAdapter<IndexSearcher>()
            {
                private final Semaphore reopenLock = new Semaphore( 1 );

                @Override
                public boolean maybeRefresh() throws IOException
                {
                    boolean lockAcquired = false;
                    try
                    {
                        lockAcquired = reopenLock.tryAcquire();
                        sleep();
                        if ( lockAcquired )
                        {
                            count.incrementAndGet();
                        }
                        return lockAcquired;
                    }
                    finally
                    {
                        if ( lockAcquired )
                        {
                            reopenLock.release();
                        }
                    }
                }

                private void sleep()
                {
                    try
                    {
                        Thread.sleep( 150 );
                    }
                    catch ( InterruptedException e )
                    {
                        e.printStackTrace();
                    }
                }
            };

    @Test
    public void everySingleUpdateShouldTriggerARefresh() throws Throwable
    {
        final LuceneIndexAccessor accessor = createAccessor( manager );

        final CyclicBarrier barrier = new CyclicBarrier( 2 );
        Thread t1 = new CloseIndexUpdaterThread( accessor.newUpdater( ONLINE ), barrier );
        Thread t2 = new CloseIndexUpdaterThread( accessor.newUpdater( ONLINE ), barrier );

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        assertEquals( 2, count.get() );
    }

    @Test
    public void bothForceAndUpdatesShouldTriggerARefresh() throws Throwable
    {
        final LuceneIndexAccessor accessor = createAccessor( manager );

        final CyclicBarrier barrier = new CyclicBarrier( 2 );
        Thread t1 = new CloseIndexUpdaterThread( accessor.newUpdater( ONLINE ), barrier );
        Thread t2 = new ForceIndexAccessorThread( accessor, barrier );

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        assertEquals( 2, count.get() );
    }

    @Test
    public void twoForceShouldTriggerTwoRefreshes() throws Throwable
    {
        final LuceneIndexAccessor accessor = createAccessor( manager );

        final CyclicBarrier barrier = new CyclicBarrier( 2 );
        Thread t1 = new ForceIndexAccessorThread( accessor, barrier );
        Thread t2 = new ForceIndexAccessorThread( accessor, barrier );

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        assertEquals( 2, count.get() );
    }

    private static class CloseIndexUpdaterThread extends Thread
    {
        private final IndexUpdater indexUpdater;
        private final CyclicBarrier barrier;

        public CloseIndexUpdaterThread( IndexUpdater indexUpdater, CyclicBarrier barrier )
        {
            this.indexUpdater = indexUpdater;
            this.barrier = barrier;
        }

        @Override
        public void run()
        {
            try
            {
                barrier.await();
                indexUpdater.close();
            }
            catch ( Throwable t )
            {
                throw new RuntimeException( t );
            }
        }
    }

    private static class ForceIndexAccessorThread extends Thread
    {
        private final LuceneIndexAccessor accessor;
        private final CyclicBarrier barrier;

        public ForceIndexAccessorThread( LuceneIndexAccessor accessor, CyclicBarrier barrier )
        {
            this.accessor = accessor;
            this.barrier = barrier;
        }

        @Override
        public void run()
        {
            try
            {
                barrier.await();
                accessor.force();
            }
            catch ( Throwable t )
            {
                throw new RuntimeException( t );
            }
        }
    }

    private LuceneIndexAccessor createAccessor( LuceneIndexAccessor.LuceneReferenceManager<IndexSearcher> manager )
    {
        return new LuceneIndexAccessor( structure, false, writer, manager, directory, dir, 42 )
        {
        };
    }

    private static class LuceneReferenceManagerAdapter<G> implements LuceneIndexAccessor.LuceneReferenceManager<G>
    {

        @Override
        public G acquire()
        {
            return null;
        }

        @Override
        public boolean maybeRefresh() throws IOException
        {
            return false;
        }

        @Override
        public void release( G reference ) throws IOException
        {
        }

        @Override
        public void close() throws IOException
        {
        }
    }
}
