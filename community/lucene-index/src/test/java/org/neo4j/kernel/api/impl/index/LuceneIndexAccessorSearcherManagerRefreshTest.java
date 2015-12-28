/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.api.index.IndexUpdateMode.ONLINE;
import static org.neo4j.test.TargetDirectory.testDirForTest;

@Ignore("Needs to be rewriten to use new index infrastructure")
public class LuceneIndexAccessorSearcherManagerRefreshTest
{

    @Rule
    public TargetDirectory.TestDirectory testDirectory = testDirForTest( getClass() );

    private final AtomicLong count = new AtomicLong( 0 );

    @Test
    public void everySingleUpdateShouldTriggerARefresh() throws Throwable
    {
        final LuceneIndexAccessor accessor = createAccessor( );

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
        final LuceneIndexAccessor accessor = createAccessor( );

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
        final LuceneIndexAccessor accessor = createAccessor( );

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

    private LuceneIndexAccessor createAccessor()
            throws IOException
    {
        FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();

        PartitionedIndexStorage indexStorage =
                new PartitionedIndexStorage( DirectoryFactory.PERSISTENT, fileSystem,
                        testDirectory.directory( "index" ), 1 );

        LuceneIndex luceneIndex = new LuceneIndex( indexStorage, new IndexConfiguration( false ),
                new IndexSamplingConfig( new Config() ) );
        luceneIndex.prepare();
        luceneIndex.open();

        return new LuceneIndexAccessor( luceneIndex );

    }
}
