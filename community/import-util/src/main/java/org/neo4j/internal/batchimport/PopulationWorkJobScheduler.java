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
package org.neo4j.internal.batchimport;

import static org.neo4j.scheduler.Group.INDEX_POPULATION_WORK;

import java.util.concurrent.Callable;
import org.neo4j.common.Subject;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.scheduler.JobScheduler;

/**
 * Simple wrapper making a {@link JobScheduler} look like a
 * {@link org.neo4j.kernel.api.index.IndexPopulator.PopulationWorkScheduler}.
 */
public class PopulationWorkJobScheduler implements IndexPopulator.PopulationWorkScheduler {
    private final JobScheduler jobScheduler;
    private final DatabaseLayout databaseLayout;

    public PopulationWorkJobScheduler(JobScheduler jobScheduler, DatabaseLayout databaseLayout) {
        this.jobScheduler = jobScheduler;
        this.databaseLayout = databaseLayout;
    }

    @Override
    public <T> JobHandle<T> schedule(IndexPopulator.JobDescriptionSupplier descriptionSupplier, Callable<T> job) {
        return jobScheduler.schedule(
                INDEX_POPULATION_WORK,
                new JobMonitoringParams(
                        Subject.ANONYMOUS, databaseLayout.getDatabaseName(), "complete index population scan"),
                job);
    }

    public JobScheduler jobScheduler() {
        return jobScheduler;
    }
}
