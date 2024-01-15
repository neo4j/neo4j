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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

/**
 * A small convenience for scheduling multiple "scanCompleted" index population calls and then waiting for
 * them to finish
 */
public class BuildCompletionScheduler implements Consumer<Runnable>, AutoCloseable {
    private final JobScheduler jobScheduler;
    private final List<JobHandle<?>> handles = new ArrayList<>();

    public BuildCompletionScheduler(JobScheduler jobScheduler) {
        this.jobScheduler = jobScheduler;
    }

    @Override
    public void accept(Runnable runnable) {
        handles.add(jobScheduler.schedule(Group.INDEX_POPULATION, runnable));
    }

    @Override
    public void close() {
        try {
            for (JobHandle<?> handle : handles) {
                handle.waitTermination();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
