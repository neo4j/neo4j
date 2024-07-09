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
package org.neo4j.internal.batchimport.staging;

import static java.util.concurrent.TimeUnit.SECONDS;

import org.neo4j.batchimport.api.Configuration;
import org.neo4j.internal.batchimport.stats.Keys;
import org.neo4j.internal.helpers.collection.Iterables;

/**
 * An {@link ExecutionMonitor} that prints progress in percent, knowing the max number of nodes and relationships
 * in advance.
 */
public abstract class CoarseBoundedProgressExecutionMonitor extends ExecutionMonitor.Adapter {
    private final long totalNumberOfBatches;
    private long prevDoneBatches;
    private long totalReportedBatches;

    public CoarseBoundedProgressExecutionMonitor(
            long highNodeId, long highRelationshipId, Configuration configuration) {
        super(1, SECONDS);
        // This calculation below is aware of internals of the parallel importer and may
        // be wrong for other importers.
        this.totalNumberOfBatches = (highNodeId / configuration.batchSize()) * 3
                + // node records encountered three times
                (highRelationshipId / configuration.batchSize()) * 4; // rel records encountered four times
    }

    protected long total() {
        return totalNumberOfBatches;
    }

    @Override
    public void check(StageExecution execution) {
        update(execution);
    }

    @Override
    public void start(StageExecution execution) {
        prevDoneBatches = 0;
    }

    private void update(StageExecution execution) {
        long diff = 0;
        long doneBatches = doneBatches(execution);
        diff += doneBatches - prevDoneBatches;
        prevDoneBatches = doneBatches;

        if (diff > 0) {
            totalReportedBatches += diff;
            progress(diff);
        }
    }

    /**
     * @param progress Relative progress.
     */
    protected abstract void progress(long progress);

    private static long doneBatches(StageExecution execution) {
        Step<?> step = Iterables.last(execution.steps());
        return step.stats().stat(Keys.done_batches).asLong();
    }

    @Override
    public void done(boolean successful, long totalTimeMillis, String additionalInformation) {
        // Just report the last progress
        progress(totalNumberOfBatches - totalReportedBatches);
    }
}
