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

import static org.neo4j.configuration.GraphDatabaseInternalSettings.index_populator_block_size;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.neo4j.common.EntityType;
import org.neo4j.common.Subject;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.io.memory.UnsafeDirectByteBufferAllocator;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.memory.ThreadSafePeakMemoryTracker;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.util.concurrent.Runnables;

/**
 * A background job for initially populating one or more index over existing data in the database.
 * Use provided store view to scan store. Participating {@link IndexPopulator} are added with
 * {@link #addPopulator(IndexPopulator, IndexProxyStrategy, FlippableIndexProxy)}
 * before {@link #run() running} this job.
 */
public class IndexPopulationJob implements Runnable {
    private static final String INDEX_POPULATION_TAG = "indexPopulationJob";
    private final IndexMonitor monitor;
    private final CursorContextFactory contextFactory;
    private final MemoryTracker memoryTracker;
    private final ByteBufferFactory bufferFactory;
    private final ThreadSafePeakMemoryTracker memoryAllocationTracker;
    private final MultipleIndexPopulator multiPopulator;
    private final CountDownLatch doneSignal = new CountDownLatch(1);
    private final String databaseName;
    private final Subject subject;
    private final EntityType populatedEntityType;

    /**
     * A list of all indexes populated by this job.
     */
    private final List<IndexDescriptor> populatedIndexes = new ArrayList<>();

    private volatile StoreScan storeScan;
    private volatile boolean stopped;
    /**
     * The {@link JobHandle} that represents the scheduling of this index population job.
     * This is used in the cancellation of the job.
     */
    private volatile JobHandle<?> jobHandle;

    public IndexPopulationJob(
            MultipleIndexPopulator multiPopulator,
            IndexMonitor monitor,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker,
            String databaseName,
            Subject subject,
            EntityType populatedEntityType,
            Config config) {
        this.multiPopulator = multiPopulator;
        this.monitor = monitor;
        this.contextFactory = contextFactory;
        this.memoryTracker = memoryTracker;
        this.memoryAllocationTracker = new ThreadSafePeakMemoryTracker();
        this.bufferFactory = new ByteBufferFactory(
                UnsafeDirectByteBufferAllocator::new,
                config.get(index_populator_block_size).intValue());
        this.databaseName = databaseName;
        this.subject = subject;
        this.populatedEntityType = populatedEntityType;
    }

    /**
     * Adds an {@link IndexPopulator} to be populated in this store scan. All participating populators must
     * be added before calling {@link #run()}.
     *  @param populator {@link IndexPopulator} to participate.
     * @param indexProxyStrategy {@link IndexProxyStrategy} meta information about index.
     * @param flipper {@link FlippableIndexProxy} to call after a successful population.
     */
    MultipleIndexPopulator.IndexPopulation addPopulator(
            IndexPopulator populator, IndexProxyStrategy indexProxyStrategy, FlippableIndexProxy flipper) {
        assert storeScan == null : "Population have already started, too late to add populators at this point";
        populatedIndexes.add(indexProxyStrategy.getIndexDescriptor());
        return this.multiPopulator.addPopulator(populator, indexProxyStrategy, flipper);
    }

    /**
     * Scans the store using store view and populates all participating {@link IndexPopulator} with data relevant to
     * each index.
     * The scan continues as long as there's at least one non-failed populator.
     */
    @Override
    public void run() {
        try (var cursorContext = contextFactory.create(INDEX_POPULATION_TAG)) {
            var indexDescriptors = multiPopulator.indexDescriptors();
            monitor.indexPopulationJobStarting(indexDescriptors);
            if (!multiPopulator.hasPopulators() || stopped) { // Don't start if asked to stop
                return;
            }
            if (storeScan != null) {
                throw new IllegalStateException("Population already started.");
            }

            try {
                multiPopulator.create(cursorContext);
                multiPopulator.resetIndexCounts(cursorContext);

                monitor.indexPopulationScanStarting(indexDescriptors);
                indexAllEntities(contextFactory);
                monitor.indexPopulationScanComplete();
                if (stopped) {
                    multiPopulator.stop(cursorContext);
                    // We remain in POPULATING state
                    return;
                }
                multiPopulator.flipAfterStoreScan(cursorContext);
            } catch (Throwable t) {
                multiPopulator.cancel(t, cursorContext);
            }
        } finally {
            // will only close "additional" resources, not the actual populators, since that's managed by flip
            Runnables.runAll(
                    "Failed to close resources in IndexPopulationJob",
                    multiPopulator::close,
                    bufferFactory::close,
                    () -> monitor.populationJobCompleted(memoryAllocationTracker.peakMemoryUsage()),
                    doneSignal::countDown);
        }
    }

    private void indexAllEntities(CursorContextFactory contextFactory) {
        storeScan = multiPopulator.createStoreScan(contextFactory);
        if (!stopped) {
            storeScan.run(multiPopulator);
        }
    }

    PopulationProgress getPopulationProgress(MultipleIndexPopulator.IndexPopulation indexPopulation) {
        if (storeScan == null) {
            // indexing hasn't begun yet
            return PopulationProgress.NONE;
        }
        PopulationProgress storeScanProgress = storeScan.getProgress();
        return indexPopulation.progress(storeScanProgress);
    }

    /**
     * Signal to stop index population.
     * All populating indexes will remain in {@link InternalIndexState#POPULATING populating state} to be rebuilt on next db start up.
     * Asynchronous call, need to {@link #awaitCompletion(long, TimeUnit) await completion}.
     */
    public void stop() {
        stopped = true;
        // Stop the population
        if (storeScan != null) {
            storeScan.stop();

            // Only cancelling if we have the storeScan because we need the doneSignal to not hang in awaitCompletion
            if (jobHandle != null) {
                jobHandle.cancel();
            }
        }
        monitor.populationCancelled(multiPopulator.indexDescriptors(), storeScan != null);
    }

    /**
     * Stop population of specific index. Index will remain in {@link InternalIndexState#POPULATING populating state} to be rebuilt on next db start up.
     * @param population {@link MultipleIndexPopulator.IndexPopulation} to be stopped.
     */
    void stop(MultipleIndexPopulator.IndexPopulation population, CursorContext cursorContext) {
        multiPopulator.stop(population, cursorContext);
    }

    /**
     * Stop population of specific index and drop it.
     * @param population {@link MultipleIndexPopulator.IndexPopulation} to be dropped.
     */
    void dropPopulation(MultipleIndexPopulator.IndexPopulation population) {
        multiPopulator.dropIndexPopulation(population);
    }

    /**
     * A transaction happened that produced the given updates. Let this job incorporate its data,
     * feeding it to the {@link IndexPopulator}.
     *
     * @param update {@link IndexEntryUpdate} to queue.
     */
    public void update(IndexEntryUpdate<?> update) {
        multiPopulator.queueConcurrentUpdate(update);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[populator:" + multiPopulator + "]";
    }

    /**
     * Awaits completion of this population job, but waits maximum the given time.
     *
     * @param time time to wait at the most for completion. A value of 0 means indefinite wait.
     * @param unit {@link TimeUnit unit} of the {@code time}.
     * @return {@code true} if the job is still running when leaving this method, otherwise {@code false} meaning that the job is completed.
     * @throws InterruptedException if the wait got interrupted.
     */
    public boolean awaitCompletion(long time, TimeUnit unit) throws InterruptedException {
        if (time == 0) {
            doneSignal.await();
            return false;
        }
        boolean completed = doneSignal.await(time, unit);
        return !completed;
    }

    /**
     * Assign the job-handle that was created when this index population job was scheduled.
     * This makes it possible to {@link JobHandle#cancel() cancel} the scheduled index population,
     * making it never start, through {@link IndexPopulationJob#stop()}.
     */
    public void setHandle(JobHandle handle) {
        this.jobHandle = handle;
    }

    public ByteBufferFactory bufferFactory() {
        return bufferFactory;
    }

    public MemoryTracker getMemoryTracker() {
        return memoryTracker;
    }

    public JobMonitoringParams getMonitoringParams() {
        return new JobMonitoringParams(subject, databaseName, getMonitoringDescription(), () -> {
            var stateDescriptionBuilder = new StringBuilder();
            // Print index names only if there is more than 1,
            // because if there is only one, its name will already be in the job description
            if (populatedIndexes.size() > 1) {
                stateDescriptionBuilder.append("Population of indexes ");
                boolean first = true;

                for (var index : populatedIndexes) {
                    if (first) {
                        first = false;
                    } else {
                        stateDescriptionBuilder.append(",");
                    }

                    stateDescriptionBuilder.append("'").append(index.getName()).append("'");
                }

                stateDescriptionBuilder.append("; ");
            }

            PopulationProgress populationProgress = PopulationProgress.NONE;
            if (storeScan != null) {
                populationProgress = storeScan.getProgress();
            }

            stateDescriptionBuilder
                    .append("Total progress: ")
                    .append(populationProgress.toIndexPopulationProgress().getCompletedPercentage())
                    .append("%");

            return stateDescriptionBuilder.toString();
        });
    }

    private String getMonitoringDescription() {
        if (populatedIndexes.isEmpty()) {
            // this should not happen
            // but it is better to show this over throwing an exception.
            return "Empty index population";
        }

        if (populatedIndexes.size() == 1) {
            var index = populatedIndexes.get(0);
            return "Population of index '" + index.getName() + "'";
        }

        return "Population of " + populatedIndexes.size() + " '" + populatedEntityType + "' indexes";
    }
}
