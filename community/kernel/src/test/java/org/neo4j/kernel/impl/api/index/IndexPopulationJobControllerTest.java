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
package org.neo4j.kernel.impl.api.index;


import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.neo4j.test.OnDemandJobScheduler;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IndexPopulationJobControllerTest
{

    private final OnDemandJobScheduler executer = new OnDemandJobScheduler();
    private final IndexPopulationJobController jobController = new IndexPopulationJobController( executer );

    @Test
    public void trackPopulationJobs()
    {
        assertThat( jobController.getPopulationJobs(), is( empty() ) );

        IndexPopulationJob populationJob = mock( IndexPopulationJob.class );
        jobController.startIndexPopulation( populationJob );
        assertThat( jobController.getPopulationJobs(), hasSize( 1 ) );

        IndexPopulationJob populationJob2 = mock( IndexPopulationJob.class );
        jobController.startIndexPopulation( populationJob2 );
        assertThat( jobController.getPopulationJobs(), hasSize( 2 ) );
    }

    @Test
    public void stopOngoingPopulationJobs() throws ExecutionException, InterruptedException
    {
        IndexPopulationJob populationJob = getIndexPopulationJob();
        IndexPopulationJob populationJob2 = getIndexPopulationJob();
        jobController.startIndexPopulation( populationJob );
        jobController.startIndexPopulation( populationJob2 );

        jobController.stop();

        verify( populationJob ).cancel();
        verify( populationJob2 ).cancel();
    }

    @Test
    public void untrackFinishedPopulations()
    {
        IndexPopulationJob populationJob = getIndexPopulationJob();
        jobController.startIndexPopulation( populationJob );

        assertThat( jobController.getPopulationJobs(), hasSize( 1 ) );

        executer.runJob();

        assertThat( jobController.getPopulationJobs(), hasSize( 0 ) );
        verify( populationJob ).run();
    }

    private IndexPopulationJob getIndexPopulationJob()
    {
        IndexPopulationJob populationJob = mock( IndexPopulationJob.class );
        when( populationJob.cancel() ).thenReturn( CompletableFuture.completedFuture( null ) );
        return populationJob;
    }
}
