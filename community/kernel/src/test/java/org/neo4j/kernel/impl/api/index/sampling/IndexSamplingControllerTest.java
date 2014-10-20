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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.index.InternalIndexState.ONLINE;
import static org.neo4j.kernel.api.index.InternalIndexState.POPULATING;

import org.junit.Test;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexMap;
import org.neo4j.kernel.impl.api.index.IndexMapSnapshotProvider;
import org.neo4j.kernel.impl.api.index.IndexProxy;

public class IndexSamplingControllerTest
{
    private final IndexSamplingJobFactory jobFactory = mock( IndexSamplingJobFactory.class );
    private final IndexSamplingJobTracker tracker = mock( IndexSamplingJobTracker.class );
    private final IndexMapSnapshotProvider snapshotProvider = mock( IndexMapSnapshotProvider.class );
    private final IndexMap indexMap = new IndexMap();
    private final IndexProxy indexProxy = mock( IndexProxy.class );

    {
        when( indexProxy.getDescriptor() ).thenReturn( new IndexDescriptor( 3, 4 ) );
        when( snapshotProvider.indexMapSnapshot() ).thenReturn( indexMap );
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
        controller.run();

        // then
        Runnable runnable = verify( jobFactory ).create( indexProxy );
        verify( tracker ).scheduleSamplingJob( runnable );
        verify( tracker, times( 2 ) ).canExecuteMoreSamplingJobs();
        verifyNoMoreInteractions( jobFactory, tracker );
    }

    @Test
    public void shouldNotStartAJobIfTheIndexIsNotOnline()
    {
        // given
        IndexSamplingController controller = new IndexSamplingController( jobFactory, tracker, snapshotProvider );
        when( tracker.canExecuteMoreSamplingJobs() ).thenReturn( true );
        when( indexProxy.getState() ).thenReturn( POPULATING );

        // when
        controller.run();

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
        controller.run();

        // then
        verify( tracker, times( 1 ) ).canExecuteMoreSamplingJobs();
        verifyNoMoreInteractions( jobFactory, tracker );
    }
}
