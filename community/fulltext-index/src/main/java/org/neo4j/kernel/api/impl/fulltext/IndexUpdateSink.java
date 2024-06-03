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
package org.neo4j.kernel.api.impl.fulltext;

import static org.neo4j.util.concurrent.OutOfOrderSequence.EMPTY_META;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.util.concurrent.ArrayQueueOutOfOrderSequence;
import org.neo4j.util.concurrent.OutOfOrderSequence;

/**
 * A sink for index updates that will eventually be applied.
 */
public class IndexUpdateSink {
    private final JobScheduler scheduler;
    private final Semaphore updateQueueLimit;
    private final int eventuallyConsistentUpdateQueueLimit;
    private final OutOfOrderSequence jobSequence = new ArrayQueueOutOfOrderSequence(-1, 10, EMPTY_META);
    private final AtomicLong nextJobId = new AtomicLong();

    IndexUpdateSink(JobScheduler scheduler, int eventuallyConsistentUpdateQueueLimit) {
        this.scheduler = scheduler;
        this.updateQueueLimit = new Semaphore(eventuallyConsistentUpdateQueueLimit);
        this.eventuallyConsistentUpdateQueueLimit = eventuallyConsistentUpdateQueueLimit;
    }

    public void enqueueTransactionBatchOfUpdates(
            DatabaseIndex<? extends IndexReader> index,
            IndexUpdater indexUpdater,
            Collection<IndexEntryUpdate<?>> updates) {
        int numberOfUpdates = Math.min(updates.size(), eventuallyConsistentUpdateQueueLimit);
        try {
            updateQueueLimit.acquire(numberOfUpdates);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

        long jobId = nextJobId.getAndIncrement();
        Runnable eventualUpdate = () -> {
            try (indexUpdater) {
                for (var update : updates) {
                    indexUpdater.process(update);
                }
            } catch (IndexEntryConflictException e) {
                markAsFailed(index, e);
            } finally {
                try {
                    updateQueueLimit.release(numberOfUpdates);
                } finally {
                    jobSequence.offer(jobId, EMPTY_META);
                }
            }
        };

        try {
            var monitoringParams = JobMonitoringParams.systemJob(
                    "Background update of index '" + index.getDescriptor().getName() + "'");
            scheduler.schedule(Group.INDEX_UPDATING, monitoringParams, eventualUpdate);
        } catch (Exception e) {
            updateQueueLimit.release(numberOfUpdates); // Avoid leaking permits if job scheduling fails.
            throw e;
        }
    }

    private static void markAsFailed(DatabaseIndex<? extends IndexReader> index, IndexEntryConflictException conflict) {
        try {
            index.markAsFailed(conflict.getMessage());
        } catch (IOException ioe) {
            ioe.addSuppressed(conflict);
            throw new UncheckedIOException(ioe);
        }
    }

    public void awaitUpdateApplication() {
        long targetJobId = nextJobId.get() - 1;
        while (jobSequence.getHighestGapFreeNumber() < targetJobId) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }
    }
}
