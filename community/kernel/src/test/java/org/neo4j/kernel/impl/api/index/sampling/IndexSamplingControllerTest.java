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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongPredicate;

import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexMap;
import org.neo4j.kernel.impl.api.index.IndexMapSnapshotProvider;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.InternalIndexState.FAILED;
import static org.neo4j.internal.kernel.api.InternalIndexState.ONLINE;
import static org.neo4j.internal.kernel.api.InternalIndexState.POPULATING;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;
import static org.neo4j.kernel.impl.api.index.TestIndexProviderDescriptor.PROVIDER_DESCRIPTOR;
import static org.neo4j.kernel.impl.api.index.sampling.IndexSamplingController.ASYNC_RECOVER_INDEX_SAMPLES_NAME;
import static org.neo4j.kernel.impl.api.index.sampling.IndexSamplingController.ASYNC_RECOVER_INDEX_SAMPLES_WAIT_NAME;
import static org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode.backgroundRebuildUpdated;
import static org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode.foregroundRebuildUpdated;
import static org.neo4j.util.FeatureToggles.clear;
import static org.neo4j.util.FeatureToggles.set;

class IndexSamplingControllerTest
{
    private final IndexSamplingConfig samplingConfig = mock( IndexSamplingConfig.class );
    private final IndexSamplingJobFactory jobFactory = mock( IndexSamplingJobFactory.class );
    private final LongPredicate samplingUpdatePredicate = id -> true;
    private final IndexSamplingJobTracker tracker = mock( IndexSamplingJobTracker.class, RETURNS_MOCKS );
    private final JobScheduler scheduler = mock( JobScheduler.class );
    private final IndexMapSnapshotProvider snapshotProvider = mock( IndexMapSnapshotProvider.class );
    private final IndexMap indexMap = new IndexMap();
    private final long indexId = 2;
    private final long anotherIndexId = 3;
    private final IndexProxy indexProxy = mock( IndexProxy.class );
    private final IndexProxy anotherIndexProxy = mock( IndexProxy.class );
    private final IndexDescriptor descriptor =
            forSchema( forLabel( 3, 4 ), PROVIDER_DESCRIPTOR ).withName( "index_2" ).materialise( indexId );
    private final IndexDescriptor anotherDescriptor =
            forSchema( forLabel( 5, 6 ), PROVIDER_DESCRIPTOR ).withName( "index_3" ).materialise( anotherIndexId );
    private final IndexSamplingJob job = mock( IndexSamplingJob.class );
    private final IndexSamplingJob anotherJob = mock( IndexSamplingJob.class );
    private AssertableLogProvider logProvider;

    @BeforeEach
    void setupLogProvider()
    {
        when( samplingConfig.backgroundSampling() ).thenReturn( true );
        when( indexProxy.getDescriptor() ).thenReturn( descriptor );
        when( anotherIndexProxy.getDescriptor() ).thenReturn( anotherDescriptor );
        when( snapshotProvider.indexMapSnapshot() ).thenReturn( indexMap );
        when( jobFactory.create( indexId, indexProxy ) ).thenReturn( job );
        when( jobFactory.create( anotherIndexId, anotherIndexProxy ) ).thenReturn( anotherJob );

        indexMap.putIndexProxy( indexProxy );
        logProvider = new AssertableLogProvider();
    }

    @Test
    void shouldStartASamplingJobForEachIndexInTheDB()
    {
        // given
        IndexSamplingController controller = newSamplingController( always( false ), logProvider);
        when( indexProxy.getState() ).thenReturn( ONLINE );

        // when
        controller.sampleIndexes( backgroundRebuildUpdated() );

        // then
        verify( jobFactory ).create( indexId, indexProxy );
        verify( tracker ).scheduleSamplingJob( job );
        verifyNoMoreInteractions( jobFactory, tracker );
    }

    @Test
    void shouldNotStartAJobIfTheIndexIsNotOnline()
    {
        // given
        IndexSamplingController controller = newSamplingController( always( false ), logProvider);
        when( indexProxy.getState() ).thenReturn( POPULATING );

        // when
        controller.sampleIndexes( backgroundRebuildUpdated() );

        // then
        verifyNoMoreInteractions( jobFactory, tracker );
    }

    @Test
    void shouldSampleAllTheIndexes()
    {
        // given
        IndexSamplingController controller = newSamplingController( always( false ), logProvider);
        when( indexProxy.getState() ).thenReturn( ONLINE );
        when( anotherIndexProxy.getState() ).thenReturn( ONLINE );
        indexMap.putIndexProxy( anotherIndexProxy );

        // when
        controller.sampleIndexes( backgroundRebuildUpdated() );

        // then
        verify( jobFactory ).create( indexId, indexProxy );
        verify( tracker ).scheduleSamplingJob( job );
        verify( jobFactory ).create( anotherIndexId, anotherIndexProxy );
        verify( tracker ).scheduleSamplingJob( anotherJob );

        verifyNoMoreInteractions( jobFactory, tracker );
    }

    @Test
    void shouldSampleAllTheOnlineIndexes()
    {
        // given
        IndexSamplingController controller = newSamplingController( always( false ), logProvider);
        when( indexProxy.getState() ).thenReturn( ONLINE );
        when( anotherIndexProxy.getState() ).thenReturn( POPULATING );
        indexMap.putIndexProxy( anotherIndexProxy );

        // when
        controller.sampleIndexes( backgroundRebuildUpdated() );

        // then
        verify( jobFactory ).create( indexId, indexProxy );
        verify( tracker ).scheduleSamplingJob( job );

        verifyNoMoreInteractions( jobFactory, tracker );
    }

    @Test
    void shouldForegroundSampleAllTheIndexes() throws InterruptedException, ExecutionException, TimeoutException
    {
        // given
        IndexSamplingController controller = newSamplingController( always( false ), logProvider);
        when( indexProxy.getState() ).thenReturn( ONLINE );
        when( anotherIndexProxy.getState() ).thenReturn( ONLINE );
        indexMap.putIndexProxy( anotherIndexProxy );
        JobHandle jobHandle = mock( JobHandle.class);
        JobHandle anotherJobHandle = mock( JobHandle.class );
        when( tracker.scheduleSamplingJob( job ) ).thenReturn( jobHandle );
        when( tracker.scheduleSamplingJob( anotherJob ) ).thenReturn( anotherJobHandle );

        // when
        IndexSamplingMode mode = foregroundRebuildUpdated( 60 );
        controller.sampleIndexes( mode );

        // then
        verify( jobFactory ).create( indexId, indexProxy );
        verify( tracker ).scheduleSamplingJob( job );
        verify( jobFactory ).create( anotherIndexId, anotherIndexProxy );
        verify( tracker ).scheduleSamplingJob( anotherJob );
        verify( jobHandle ).waitTermination( anyLong(), any( TimeUnit.class ) );
        verify( anotherJobHandle ).waitTermination( anyLong(), any( TimeUnit.class ) );
        verifyNoMoreInteractions( jobFactory, tracker, jobHandle, anotherJobHandle );
    }

    @Test
    void shouldThrowIfJobTimesOut()
    {
        // given
        IndexSamplingController controller = newSamplingController( always( false ), logProvider);
        when( indexProxy.getState() ).thenReturn( ONLINE );
        when( anotherIndexProxy.getState() ).thenReturn( ONLINE );
        indexMap.putIndexProxy( anotherIndexProxy );
        JobHandle<Object> jobHandle = new JobHandle<>()
        {
            @Override
            public void cancel()
            {
            }

            @Override
            public void waitTermination()
            {
                fail( "We should never use this wait for foreground sampling." );
            }

            @Override
            public void waitTermination( long timeout, TimeUnit unit ) throws TimeoutException
            {
                throw new TimeoutException( "I'm sorry, so slow." );
            }

            @Override
            public Object get()
            {
                fail( "We should never use this wait for foreground sampling." );
                return null;
            }
        };
        when( tracker.scheduleSamplingJob( job ) ).thenReturn( jobHandle );

        IndexSamplingMode mode = foregroundRebuildUpdated( 1 );
        RuntimeException e = assertThrows( RuntimeException.class, () -> controller.sampleIndexes( mode ) );
        assertThat( e.getMessage() ).contains( "Could not finish index sampling within the given time limit, 1 milliseconds." );
    }

    @Test
    void shouldRecoverOnlineIndex()
    {
        // given
        IndexSamplingController controller = newSamplingController( always( true ), logProvider);
        when( indexProxy.getState() ).thenReturn( ONLINE );

        // when
        controller.recoverIndexSamples();

        // then
        verify( jobFactory ).create( indexId, indexProxy );
        verify( tracker ).scheduleSamplingJob( job );
        verifyNoMoreInteractions( jobFactory, job, tracker );
    }

    @Test
    void shouldNotRecoverOfflineIndex()
    {
        // given
        IndexSamplingController controller = newSamplingController( always( true ), logProvider);
        when( indexProxy.getState() ).thenReturn( FAILED );

        // when
        controller.recoverIndexSamples();

        // then
        verifyNoMoreInteractions( jobFactory, job, tracker );
    }

    @Test
    void shouldNotRecoverOnlineIndexIfNotNeeded()
    {
        // given
        IndexSamplingController controller = newSamplingController( always( false ), logProvider);
        when( indexProxy.getState() ).thenReturn( ONLINE );

        // when
        controller.recoverIndexSamples();

        // then
        verifyNoMoreInteractions( jobFactory, job, tracker );
    }

    @Test
    void shouldSampleIndex()
    {
        // given
        IndexSamplingController controller = newSamplingController( always( false ), logProvider);
        when( indexProxy.getState() ).thenReturn( ONLINE );
        when( anotherIndexProxy.getState() ).thenReturn( ONLINE );
        indexMap.putIndexProxy( anotherIndexProxy );

        // when
        controller.sampleIndex( indexId, backgroundRebuildUpdated() );

        // then
        verify( jobFactory ).create( indexId, indexProxy );
        verify( tracker ).scheduleSamplingJob( job );
        verify( jobFactory, never() ).create( anotherIndexId, anotherIndexProxy );
        verify( tracker, never() ).scheduleSamplingJob( anotherJob );

        verifyNoMoreInteractions( jobFactory, tracker );
    }

    @Test
    void shouldLogRecoveryIndexSamples()
    {
        set( IndexSamplingController.class, IndexSamplingController.LOG_RECOVER_INDEX_SAMPLES_NAME, true );
        try
        {
            final RecoveryCondition predicate = descriptor -> descriptor.equals( indexProxy.getDescriptor() );
            final IndexSamplingController controller = newSamplingController( predicate, logProvider );

            when( indexProxy.getState() ).thenReturn( ONLINE );
            when( anotherIndexProxy.getState() ).thenReturn( ONLINE );
            indexMap.putIndexProxy( anotherIndexProxy );

            // when
            controller.recoverIndexSamples();

            // then
            final AssertableLogProvider.MessageMatcher messageMatcher = logProvider.formattedMessageMatcher();
            messageMatcher.assertContains( "Index requires sampling, id=2, name=index_2." );
            messageMatcher.assertContains( "Index does not require sampling, id=3, name=index_3." );
        }
        finally
        {
            clear( IndexSamplingController.class, IndexSamplingController.LOG_RECOVER_INDEX_SAMPLES_NAME );
        }
    }

    @Test
    void triggerAsyncSamplesByDefault()
    {
        final IndexSamplingController controller = newSamplingController( always( true ), logProvider );
        when( indexProxy.getState() ).thenReturn( ONLINE );
        when( jobFactory.create( indexId, indexProxy ) ).thenReturn( job );
        when( tracker.scheduleSamplingJob( any( IndexSamplingJob.class ) ) ).thenReturn( mock( JobHandle.class ) );

        controller.recoverIndexSamples();

        verify( tracker ).scheduleSamplingJob( job );
    }

    @Test
    void shouldNotTriggerAsyncSamplesIfNotToggled()
    {
        set( IndexSamplingController.class, ASYNC_RECOVER_INDEX_SAMPLES_NAME, false );
        try
        {
            final IndexSamplingController controller = newSamplingController( always( true ), logProvider );
            when( indexProxy.getState() ).thenReturn( ONLINE );

            controller.recoverIndexSamples();

            verifyNoMoreInteractions( tracker );
        }
        finally
        {
            clear( IndexSamplingController.class, ASYNC_RECOVER_INDEX_SAMPLES_NAME );
        }
    }

    @Test
    void waitForAsyncIndexSamples() throws ExecutionException, InterruptedException
    {
        final IndexSamplingController controller = newSamplingController( always( true ), logProvider );
        when( indexProxy.getState() ).thenReturn( ONLINE );
        when( jobFactory.create( indexId, indexProxy ) ).thenReturn( job );
        final JobHandle jobHandle = mock( JobHandle.class );
        when( tracker.scheduleSamplingJob( any( IndexSamplingJob.class ) ) ).thenReturn( jobHandle );

        controller.recoverIndexSamples();

        verify( tracker ).scheduleSamplingJob( job );
        verify( jobHandle ).waitTermination();
    }

    @Test
    void shouldNotWaitForAsyncIndexSamplesIfConfigured()
    {
        set( IndexSamplingController.class, ASYNC_RECOVER_INDEX_SAMPLES_WAIT_NAME, false );
        try
        {
            final IndexSamplingController controller = newSamplingController( always( true ), logProvider );
            when( indexProxy.getState() ).thenReturn( ONLINE );
            when( jobFactory.create( indexId, indexProxy ) ).thenReturn( job );
            final JobHandle jobHandle = mock( JobHandle.class );
            when( tracker.scheduleSamplingJob( any( IndexSamplingJob.class ) ) ).thenReturn( jobHandle );

            controller.recoverIndexSamples();

            verify( tracker ).scheduleSamplingJob( job );
            verifyNoMoreInteractions( jobHandle );
        }
        finally
        {
            clear( IndexSamplingController.class, ASYNC_RECOVER_INDEX_SAMPLES_WAIT_NAME );
        }
    }

    private RecoveryCondition always( boolean ans )
    {
        return new Always( ans );
    }

    private IndexSamplingController newSamplingController( RecoveryCondition recoveryPredicate, LogProvider logProvider )
    {
        return new IndexSamplingController( samplingConfig, jobFactory, samplingUpdatePredicate, tracker, snapshotProvider, scheduler, recoveryPredicate,
                logProvider );
    }

    private static class Always implements RecoveryCondition
    {
        private final boolean answer;

        Always( boolean answer )
        {
            this.answer = answer;
        }

        @Override
        public boolean test( IndexDescriptor descriptor )
        {
            return answer;
        }
    }
}
