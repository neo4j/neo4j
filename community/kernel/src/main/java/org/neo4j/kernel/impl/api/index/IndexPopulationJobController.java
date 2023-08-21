/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api.index;

import static org.neo4j.scheduler.Group.INDEX_POPULATION;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.scheduler.JobScheduler;

class IndexPopulationJobController {
    private final Set<IndexPopulationJob> populationJobs = ConcurrentHashMap.newKeySet();
    private final JobScheduler scheduler;

    IndexPopulationJobController(JobScheduler scheduler) {
        this.scheduler = scheduler;
    }

    void stop() throws InterruptedException {
        for (IndexPopulationJob job : populationJobs) {
            job.stop();
        }

        InterruptedException interrupted = null;
        for (IndexPopulationJob job : populationJobs) {
            try {
                job.awaitCompletion(0, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                interrupted = Exceptions.chain(interrupted, e);
            }
        }
        if (interrupted != null) {
            throw interrupted;
        }
    }

    void startIndexPopulation(IndexPopulationJob job) {
        populationJobs.add(job);

        // There is a small race where this jobHandle hasn't been set yet and the population job gets to run and
        // is stopped (job.jobHandle == null and won't get cancelled).
        // It is a benign race since we will wait for the IndexPopulationJob:s to get to done in stop() of
        // this class, which means there is not really a need to cancel the handle. The population jobs will do
        // as little as possible when seeing the stopped status.
        job.setHandle(scheduler.schedule(
                INDEX_POPULATION, job.getMonitoringParams(), new IndexPopulationJobWrapper(job, this)));
    }

    private void indexPopulationCompleted(IndexPopulationJob populationJob) {
        populationJobs.remove(populationJob);
    }

    Set<IndexPopulationJob> getPopulationJobs() {
        return populationJobs;
    }

    private static class IndexPopulationJobWrapper implements Runnable {
        private final IndexPopulationJob indexPopulationJob;
        private final IndexPopulationJobController jobController;

        IndexPopulationJobWrapper(IndexPopulationJob indexPopulationJob, IndexPopulationJobController jobController) {
            this.indexPopulationJob = indexPopulationJob;
            this.jobController = jobController;
        }

        @Override
        public void run() {
            try {
                indexPopulationJob.run();
            } finally {
                jobController.indexPopulationCompleted(indexPopulationJob);
            }
        }
    }
}
