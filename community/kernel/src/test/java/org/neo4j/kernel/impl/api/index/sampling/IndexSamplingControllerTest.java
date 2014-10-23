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
package org.neo4j.kernel.impl.api.index.sampling;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexMap;
import org.neo4j.kernel.impl.api.index.IndexMapSnapshotProvider;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.test.DoubleLatch;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import static org.neo4j.kernel.api.index.InternalIndexState.ONLINE;
import static org.neo4j.kernel.api.index.InternalIndexState.POPULATING;
import static org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode.REBUILD_UPDATED;
import static org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode.TRY_REBUILD_UPDATED;

public class IndexSamplingControllerTest
{
    private final IndexSamplingJobFactory jobFactory = mock( IndexSamplingJobFactory.class );
    private final IndexSamplingJobTracker tracker = mock( IndexSamplingJobTracker.class );
    private final IndexMapSnapshotProvider snapshotProvider = mock( IndexMapSnapshotProvider.class );
    private final IndexMap indexMap = new IndexMap();
    private final IndexProxy indexProxy = mock( IndexProxy.class );
    private final IndexProxy anotherIndexProxy = mock( IndexProxy.class );
    private final IndexDescriptor descriptor = new IndexDescriptor( 3, 4 );
    private final IndexDescriptor anotherDescriptor = new IndexDescriptor( 5, 6 );
    private final IndexSamplingJob job = mock( IndexSamplingJob.class );
    private final IndexSamplingJob anotherJob = mock( IndexSamplingJob.class );

    {
        when( indexProxy.getDescriptor() ).thenReturn( descriptor );
        when( anotherIndexProxy.getDescriptor() ).thenReturn( anotherDescriptor );
        when( snapshotProvider.indexMapSnapshot() ).thenReturn( indexMap );
        when( jobFactory.create( indexProxy ) ).thenReturn( job );
        when( jobFactory.create( anotherIndexProxy ) ).thenReturn( anotherJob );
        indexMap.putIndexProxy( 2, indexProxy );
    }

    @Test
    public void shouldStartASamplingJobForEachIndexInTheDB()
    {
        // given
        IndexSamplingController controller = new IndexSamplingController( jobFactory, tracker, snapshotProvider );
        when( tracker.canExecuteMoreSamplingJobs() ).thenReturn( true );
        when( indexProxy.getState() ).thenReturn( ONLINE );

        // when
        controller.sampleIndexes( TRY_REBUILD_UPDATED );

        // then
        verify( jobFactory ).create( indexProxy );
        verify( tracker ).scheduleSamplingJob( job );
        verify( tracker, times( 2 ) ).canExecuteMoreSamplingJobs();
        verifyNoMoreInteractions( jobFactory, tracker );
    }

    @Test
    public void shouldNotStartAJobIfTheIndexIsNotOnline() throws InterruptedException
    {
        // given
        IndexSamplingController controller = new IndexSamplingController( jobFactory, tracker, snapshotProvider );
        when( tracker.canExecuteMoreSamplingJobs() ).thenReturn( true );
        when( indexProxy.getState() ).thenReturn( POPULATING );

        // when
        controller.sampleIndexes( TRY_REBUILD_UPDATED );

        // then
        verify( tracker, times( 2 ) ).canExecuteMoreSamplingJobs();
        verifyNoMoreInteractions( jobFactory, tracker );
    }

    @Test
    public void shouldNotStartAJobIfTheTrackerCannotHandleIt()
    {
        // given
        IndexSamplingController controller = new IndexSamplingController( jobFactory, tracker, snapshotProvider );
        when( tracker.canExecuteMoreSamplingJobs() ).thenReturn( false );
        when( indexProxy.getState() ).thenReturn( ONLINE );

        // when
        controller.sampleIndexes( TRY_REBUILD_UPDATED );

        // then
        verify( tracker, times( 1 ) ).canExecuteMoreSamplingJobs();
        verifyNoMoreInteractions( jobFactory, tracker );
    }

    @Test
    public void shouldNotEmptyQueueConcurrently()
    {
        // given
        final AtomicInteger totalCount = new AtomicInteger( 0 );
        final AtomicInteger concurrentCount = new AtomicInteger( 0 );
        final DoubleLatch jobLatch = new DoubleLatch();
        final DoubleLatch testLatch = new DoubleLatch();

        IndexSamplingJobFactory jobFactory = new IndexSamplingJobFactory()
        {
            @Override
            public IndexSamplingJob create( IndexProxy indexProxy )
            {
                if ( ! concurrentCount.compareAndSet( 0, 1 ) )
                {
                    throw new IllegalStateException( "count !== 0 on create" );
                }
                totalCount.incrementAndGet();
                jobLatch.awaitStart();
                testLatch.start();
                jobLatch.awaitFinish();
                concurrentCount.decrementAndGet();
                testLatch.finish();
                return null;
            }
        };

        final IndexSamplingController controller = new IndexSamplingController( jobFactory, tracker, snapshotProvider );
        when( tracker.canExecuteMoreSamplingJobs() ).thenReturn( true );
        when( indexProxy.getState() ).thenReturn( ONLINE );

        // when running once
        new Thread( runController( controller, TRY_REBUILD_UPDATED  ) ).start();

        jobLatch.start();
        testLatch.awaitStart();

        // then blocking on first job
        assertEquals( 1, concurrentCount.get() );

        // when running a second time
        controller.sampleIndexes( TRY_REBUILD_UPDATED );

        // then no concurrent job execution
        jobLatch.finish();
        testLatch.awaitFinish();

        // and finally exactly one job has run to completion
        assertEquals( 0, concurrentCount.get() );
        assertEquals( 1, totalCount.get() );
    }

    @Test
    public void shouldSampleAllTheIndexes()
    {
        // given
        IndexSamplingController controller = new IndexSamplingController( jobFactory, tracker, snapshotProvider );
        when( tracker.canExecuteMoreSamplingJobs() ).thenReturn( true );
        when( indexProxy.getState() ).thenReturn( ONLINE );
        when( anotherIndexProxy.getState() ).thenReturn( ONLINE );
        indexMap.putIndexProxy( 3, anotherIndexProxy );

        // when
        controller.sampleIndexes( REBUILD_UPDATED );

        // then
        verify( jobFactory ).create( indexProxy );
        verify( tracker ).scheduleSamplingJob( job );
        verify( jobFactory ).create( anotherIndexProxy );
        verify( tracker ).scheduleSamplingJob( anotherJob );

        verify( tracker, times( 2 ) ).waitUntilCanExecuteMoreSamplingJobs();
        verifyNoMoreInteractions( jobFactory, tracker );
    }

    @Test
    public void shouldSampleAllTheOnlineIndexes()
    {
        // given
        IndexSamplingController controller = new IndexSamplingController( jobFactory, tracker, snapshotProvider );
        when( tracker.canExecuteMoreSamplingJobs() ).thenReturn( true );
        when( indexProxy.getState() ).thenReturn( ONLINE );
        when( anotherIndexProxy.getState() ).thenReturn( POPULATING );
        indexMap.putIndexProxy( 3, anotherIndexProxy );

        // when
        controller.sampleIndexes( REBUILD_UPDATED );

        // then
        verify( jobFactory ).create( indexProxy );
        verify( tracker ).scheduleSamplingJob( job );

        verify( tracker, times( 2 ) ).waitUntilCanExecuteMoreSamplingJobs();
        verifyNoMoreInteractions( jobFactory, tracker );
    }

    @Test
    public void shouldNotStartOtherSamplingWhenSamplingAllTheIndexes()
    {
        // given
        final AtomicInteger totalCount = new AtomicInteger( 0 );
        final AtomicInteger concurrentCount = new AtomicInteger( 0 );
        final DoubleLatch jobLatch = new DoubleLatch();
        final DoubleLatch testLatch = new DoubleLatch();

        IndexSamplingJobFactory jobFactory = new IndexSamplingJobFactory()
        {
            @Override
            public IndexSamplingJob create( IndexProxy indexProxy )
            {
                if ( ! concurrentCount.compareAndSet( 0, 1 ) )
                {
                    throw new IllegalStateException( "count !== 0 on create" );
                }
                totalCount.incrementAndGet();
                jobLatch.awaitStart();
                testLatch.start();
                jobLatch.awaitFinish();
                concurrentCount.decrementAndGet();
                testLatch.finish();
                return null;
            }
        };

        final IndexSamplingController controller = new IndexSamplingController( jobFactory, tracker, snapshotProvider );
        when( tracker.canExecuteMoreSamplingJobs() ).thenReturn( true );
        when( indexProxy.getState() ).thenReturn( ONLINE );

        // when running once
        new Thread( runController( controller, REBUILD_UPDATED ) ).start();

        jobLatch.start();
        testLatch.awaitStart();

        // then blocking on first job
        assertEquals( 1, concurrentCount.get() );

        // when running a second time
        controller.sampleIndexes( TRY_REBUILD_UPDATED );

        // then no concurrent job execution
        jobLatch.finish();
        testLatch.awaitFinish();

        // and finally exactly one job has run to completion
        assertEquals( 0, concurrentCount.get() );
        assertEquals( 1, totalCount.get() );
    }

    private Runnable runController( final IndexSamplingController controller, final IndexSamplingMode mode )
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                controller.sampleIndexes( mode );
            }
        };
    }
}
