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
package org.neo4j.kernel.impl.api.index.sampling;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.function.Predicates;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.impl.api.index.IndexMap;
import org.neo4j.kernel.impl.api.index.IndexMapSnapshotProvider;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.DoubleLatch;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.InternalIndexState.FAILED;
import static org.neo4j.internal.kernel.api.InternalIndexState.ONLINE;
import static org.neo4j.internal.kernel.api.InternalIndexState.POPULATING;
import static org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode.BACKGROUND_REBUILD_UPDATED;
import static org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode.TRIGGER_REBUILD_UPDATED;

public class IndexSamplingControllerTest
{
    @Test
    public void shouldStartASamplingJobForEachIndexInTheDB()
    {
        // given
        IndexSamplingController controller = newSamplingController( always( false ) );
        when( tracker.canExecuteMoreSamplingJobs() ).thenReturn( true );
        when( indexProxy.getState() ).thenReturn( ONLINE );

        // when
        controller.sampleIndexes( BACKGROUND_REBUILD_UPDATED );

        // then
        verify( jobFactory ).create( indexId, indexProxy );
        verify( tracker ).scheduleSamplingJob( job );
        verify( tracker, times( 2 ) ).canExecuteMoreSamplingJobs();
        verifyNoMoreInteractions( jobFactory, tracker );
    }

    @Test
    public void shouldNotStartAJobIfTheIndexIsNotOnline()
    {
        // given
        IndexSamplingController controller = newSamplingController( always( false ) );
        when( tracker.canExecuteMoreSamplingJobs() ).thenReturn( true );
        when( indexProxy.getState() ).thenReturn( POPULATING );

        // when
        controller.sampleIndexes( BACKGROUND_REBUILD_UPDATED );

        // then
        verify( tracker, times( 2 ) ).canExecuteMoreSamplingJobs();
        verifyNoMoreInteractions( jobFactory, tracker );
    }

    @Test
    public void shouldNotStartAJobIfTheTrackerCannotHandleIt()
    {
        // given
        IndexSamplingController controller = newSamplingController( always( false ) );
        when( tracker.canExecuteMoreSamplingJobs() ).thenReturn( false );
        when( indexProxy.getState() ).thenReturn( ONLINE );

        // when
        controller.sampleIndexes( BACKGROUND_REBUILD_UPDATED );

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
        final ThreadLocal<Boolean> hasRun = ThreadLocal.withInitial( () -> false );

        IndexSamplingJobFactory jobFactory = ( indexId, proxy ) ->
        {
            // make sure we execute this once per thread
            if ( hasRun.get() )
            {
                return null;
            }
            hasRun.set( true );

            if ( !concurrentCount.compareAndSet( 0, 1 ) )
            {
                throw new IllegalStateException( "count !== 0 on create" );
            }
            totalCount.incrementAndGet();

            jobLatch.waitForAllToStart();
            testLatch.startAndWaitForAllToStart();
            jobLatch.waitForAllToFinish();

            concurrentCount.decrementAndGet();

            testLatch.finish();
            return null;
        };

        final IndexSamplingController controller = new IndexSamplingController(
                samplingConfig, jobFactory, jobQueue, tracker, snapshotProvider, scheduler, always( false )
        );
        when( tracker.canExecuteMoreSamplingJobs() ).thenReturn( true );
        when( indexProxy.getState() ).thenReturn( ONLINE );

        // when running once
        new Thread( runController( controller, BACKGROUND_REBUILD_UPDATED ) ).start();

        jobLatch.startAndWaitForAllToStart();
        testLatch.waitForAllToStart();

        // then blocking on first job
        assertEquals( 1, concurrentCount.get() );
        assertEquals( 1, totalCount.get() );

        // when running a second time
        controller.sampleIndexes( BACKGROUND_REBUILD_UPDATED );

        // then no concurrent job execution
        jobLatch.finish();
        testLatch.waitForAllToFinish();

        // and finally exactly one job has run to completion
        assertEquals( 0, concurrentCount.get() );
        assertEquals( 1, totalCount.get() );
    }

    @Test
    public void shouldSampleAllTheIndexes()
    {
        // given
        IndexSamplingController controller = newSamplingController( always( false ) );
        when( tracker.canExecuteMoreSamplingJobs() ).thenReturn( true );
        when( indexProxy.getState() ).thenReturn( ONLINE );
        when( anotherIndexProxy.getState() ).thenReturn( ONLINE );
        indexMap.putIndexProxy( anotherIndexId, anotherIndexProxy );

        // when
        controller.sampleIndexes( TRIGGER_REBUILD_UPDATED );

        // then
        verify( jobFactory ).create( indexId, indexProxy );
        verify( tracker ).scheduleSamplingJob( job );
        verify( jobFactory ).create( anotherIndexId, anotherIndexProxy );
        verify( tracker ).scheduleSamplingJob( anotherJob );

        verify( tracker, times( 2 ) ).waitUntilCanExecuteMoreSamplingJobs();
        verifyNoMoreInteractions( jobFactory, tracker );
    }

    @Test
    public void shouldSampleAllTheOnlineIndexes()
    {
        // given
        IndexSamplingController controller = newSamplingController( always( false ) );
        when( tracker.canExecuteMoreSamplingJobs() ).thenReturn( true );
        when( indexProxy.getState() ).thenReturn( ONLINE );
        when( anotherIndexProxy.getState() ).thenReturn( POPULATING );
        indexMap.putIndexProxy( anotherIndexId, anotherIndexProxy );

        // when
        controller.sampleIndexes( TRIGGER_REBUILD_UPDATED );

        // then
        verify( jobFactory ).create( indexId, indexProxy );
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

        IndexSamplingJobFactory jobFactory = ( indexId, proxy ) ->
        {
            if ( ! concurrentCount.compareAndSet( 0, 1 ) )
            {
                throw new IllegalStateException( "count !== 0 on create" );
            }
            totalCount.incrementAndGet();
            jobLatch.waitForAllToStart();
            testLatch.startAndWaitForAllToStart();
            jobLatch.waitForAllToFinish();
            concurrentCount.decrementAndGet();
            testLatch.finish();
            return null;
        };

        final IndexSamplingController controller = new IndexSamplingController(
                samplingConfig, jobFactory, jobQueue, tracker, snapshotProvider, scheduler, always( true )
        );
        when( tracker.canExecuteMoreSamplingJobs() ).thenReturn( true );
        when( indexProxy.getState() ).thenReturn( ONLINE );

        // when running once
        new Thread( runController( controller, TRIGGER_REBUILD_UPDATED ) ).start();

        jobLatch.startAndWaitForAllToStart();
        testLatch.waitForAllToStart();

        // then blocking on first job
        assertEquals( 1, concurrentCount.get() );

        // when running a second time
        controller.sampleIndexes( BACKGROUND_REBUILD_UPDATED );

        // then no concurrent job execution
        jobLatch.finish();
        testLatch.waitForAllToFinish();

        // and finally exactly one job has run to completion
        assertEquals( 0, concurrentCount.get() );
        assertEquals( 1, totalCount.get() );
    }

    @Test
    public void shouldRecoverOnlineIndex()
    {
        // given
        IndexSamplingController controller = newSamplingController( always( true ) );
        when( indexProxy.getState() ).thenReturn( ONLINE );

        // when
        controller.recoverIndexSamples();

        // then
        verify( jobFactory ).create( indexId, indexProxy );
        verify( job ).run();
        verifyNoMoreInteractions( jobFactory, job, tracker );
    }

    @Test
    public void shouldNotRecoverOfflineIndex()
    {
        // given
        IndexSamplingController controller = newSamplingController( always( true ) );
        when( indexProxy.getState() ).thenReturn( FAILED );

        // when
        controller.recoverIndexSamples();

        // then
        verifyNoMoreInteractions( jobFactory, job, tracker );
    }

    @Test
    public void shouldNotRecoverOnlineIndexIfNotNeeded()
    {
        // given
        IndexSamplingController controller = newSamplingController( always( false ) );
        when( indexProxy.getState() ).thenReturn( ONLINE );

        // when
        controller.recoverIndexSamples();

        // then
        verifyNoMoreInteractions( jobFactory, job, tracker );
    }

    @Test
    public void shouldSampleIndex()
    {
        // given
        IndexSamplingController controller = newSamplingController( always( false ) );
        when( tracker.canExecuteMoreSamplingJobs() ).thenReturn( true );
        when( indexProxy.getState() ).thenReturn( ONLINE );
        when( anotherIndexProxy.getState() ).thenReturn( ONLINE );
        indexMap.putIndexProxy( anotherIndexId, anotherIndexProxy );

        // when
        controller.sampleIndex( indexId, TRIGGER_REBUILD_UPDATED );

        // then
        verify( jobFactory, times(1) ).create( indexId, indexProxy );
        verify( tracker, times(1) ).scheduleSamplingJob( job );
        verify( jobFactory, never() ).create( anotherIndexId, anotherIndexProxy );
        verify( tracker, never() ).scheduleSamplingJob( anotherJob );

        verify( tracker, times( 1 ) ).waitUntilCanExecuteMoreSamplingJobs();
        verifyNoMoreInteractions( jobFactory, tracker );
    }

    @Test
    public void shouldNotStartForSingleIndexAJobIfTheTrackerCannotHandleIt()
    {
        // given
        IndexSamplingController controller = newSamplingController( always( false ) );
        when( tracker.canExecuteMoreSamplingJobs() ).thenReturn( false );
        when( indexProxy.getState() ).thenReturn( ONLINE );

        // when
        controller.sampleIndex( indexId, BACKGROUND_REBUILD_UPDATED );

        // then
        verify( tracker, times( 1 ) ).canExecuteMoreSamplingJobs();
        verifyNoMoreInteractions( jobFactory, tracker );
    }

    private static class Always implements IndexSamplingController.RecoveryCondition
    {
        private final boolean ans;

        Always( boolean ans )
        {
            this.ans = ans;
        }

        @Override
        public boolean test( long indexId, SchemaIndexDescriptor descriptor )
        {
            return ans;
        }
    }

    private final IndexSamplingConfig samplingConfig = mock( IndexSamplingConfig.class );
    private final IndexSamplingJobFactory jobFactory = mock( IndexSamplingJobFactory.class );
    private final IndexSamplingJobQueue<Long> jobQueue = new IndexSamplingJobQueue<>( Predicates.alwaysTrue() );
    private final IndexSamplingJobTracker tracker = mock( IndexSamplingJobTracker.class );
    private final JobScheduler scheduler = mock( JobScheduler.class );
    private final IndexMapSnapshotProvider snapshotProvider = mock( IndexMapSnapshotProvider.class );
    private final IndexMap indexMap = new IndexMap();
    private final long indexId = 2;
    private final long anotherIndexId = 3;
    private final IndexProxy indexProxy = mock( IndexProxy.class );
    private final IndexProxy anotherIndexProxy = mock( IndexProxy.class );
    private final SchemaIndexDescriptor descriptor = SchemaIndexDescriptorFactory.forLabel( 3, 4 );
    private final SchemaIndexDescriptor anotherDescriptor = SchemaIndexDescriptorFactory.forLabel( 5, 6 );
    private final IndexSamplingJob job = mock( IndexSamplingJob.class );
    private final IndexSamplingJob anotherJob = mock( IndexSamplingJob.class );

    {
        when( samplingConfig.backgroundSampling() ).thenReturn( true );
        when( samplingConfig.jobLimit() ).thenReturn( 1 );
        when( indexProxy.getDescriptor() ).thenReturn( descriptor );
        when( anotherIndexProxy.getDescriptor() ).thenReturn( anotherDescriptor );
        when( snapshotProvider.indexMapSnapshot() ).thenReturn( indexMap );
        when( jobFactory.create( indexId, indexProxy ) ).thenReturn( job );
        when( jobFactory.create( anotherIndexId, anotherIndexProxy ) ).thenReturn( anotherJob );
        indexMap.putIndexProxy( indexId, indexProxy );
    }

    private IndexSamplingController.RecoveryCondition always( boolean ans )
    {
        return new Always( ans );
    }

    private IndexSamplingController newSamplingController( IndexSamplingController.RecoveryCondition recoveryPredicate )
    {
        return new IndexSamplingController(
                samplingConfig, jobFactory, jobQueue, tracker, snapshotProvider, scheduler, recoveryPredicate
        );
    }

    private Runnable runController( final IndexSamplingController controller, final IndexSamplingMode mode )
    {
        return () -> controller.sampleIndexes( mode );
    }
}
