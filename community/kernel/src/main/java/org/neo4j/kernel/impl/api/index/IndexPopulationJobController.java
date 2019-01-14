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

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.scheduler.JobScheduler.Groups.indexPopulation;

class IndexPopulationJobController
{
    private final Set<IndexPopulationJob> populationJobs =
            Collections.newSetFromMap( new ConcurrentHashMap<IndexPopulationJob,Boolean>() );
    private final JobScheduler scheduler;

    IndexPopulationJobController( JobScheduler scheduler )
    {
        this.scheduler = scheduler;
    }

    void stop() throws ExecutionException, InterruptedException
    {
        for ( IndexPopulationJob job : populationJobs )
        {
            job.cancel().get();
        }
    }

    void startIndexPopulation( IndexPopulationJob job )
    {
        populationJobs.add( job );
        scheduler.schedule( indexPopulation, new IndexPopulationJobWrapper( job, this ) );
    }

    void indexPopulationCompleted( IndexPopulationJob populationJob )
    {
        populationJobs.remove( populationJob );
    }

    Set<IndexPopulationJob> getPopulationJobs()
    {
        return populationJobs;
    }

    private static class IndexPopulationJobWrapper implements Runnable
    {
        private IndexPopulationJob indexPopulationJob;
        private IndexPopulationJobController jobController;

        IndexPopulationJobWrapper( IndexPopulationJob indexPopulationJob, IndexPopulationJobController jobController )
        {
            this.indexPopulationJob = indexPopulationJob;
            this.jobController = jobController;
        }

        @Override
        public void run()
        {
            try
            {
                indexPopulationJob.run();
            }
            finally
            {
                jobController.indexPopulationCompleted( indexPopulationJob );
            }
        }
    }
}
