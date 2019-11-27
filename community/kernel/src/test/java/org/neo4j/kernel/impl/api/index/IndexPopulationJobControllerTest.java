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

import org.junit.jupiter.api.Test;

import org.neo4j.test.OnDemandJobScheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class IndexPopulationJobControllerTest
{

    private final OnDemandJobScheduler executer = new OnDemandJobScheduler();
    private final IndexPopulationJobController jobController = new IndexPopulationJobController( executer );

    @Test
    void trackPopulationJobs()
    {
        assertThat( jobController.getPopulationJobs() ).isEmpty();

        IndexPopulationJob populationJob = mock( IndexPopulationJob.class );
        jobController.startIndexPopulation( populationJob );
        assertThat( jobController.getPopulationJobs() ).hasSize( 1 );

        IndexPopulationJob populationJob2 = mock( IndexPopulationJob.class );
        jobController.startIndexPopulation( populationJob2 );
        assertThat( jobController.getPopulationJobs() ).hasSize( 2 );
    }

    @Test
    void stopOngoingPopulationJobs() throws InterruptedException
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
    void untrackFinishedPopulations()
    {
        IndexPopulationJob populationJob = getIndexPopulationJob();
        jobController.startIndexPopulation( populationJob );

        assertThat( jobController.getPopulationJobs() ).hasSize( 1 );

        executer.runJob();

        assertThat( jobController.getPopulationJobs() ).hasSize( 0 );
        verify( populationJob ).run();
    }

    private IndexPopulationJob getIndexPopulationJob()
    {
        return mock( IndexPopulationJob.class );
    }
}
