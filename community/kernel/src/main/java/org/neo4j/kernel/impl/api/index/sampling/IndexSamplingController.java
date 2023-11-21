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
package org.neo4j.kernel.impl.api.index.sampling;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.internal.schema.IndexType.LOOKUP;
import static org.neo4j.kernel.impl.api.index.IndexSamplingMode.backgroundRebuildUpdated;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongPredicate;
import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.kernel.impl.api.index.IndexMap;
import org.neo4j.kernel.impl.api.index.IndexMapSnapshotProvider;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.IndexSamplingMode;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.scheduler.JobScheduler;

public class IndexSamplingController {
    private final IndexSamplingJobFactory jobFactory;
    private final LongPredicate samplingUpdatePredicate;
    private final IndexSamplingJobTracker jobTracker;
    private final IndexMapSnapshotProvider indexMapSnapshotProvider;
    private final JobScheduler scheduler;
    private final RecoveryCondition indexRecoveryCondition;
    private final boolean backgroundSampling;
    private final Lock samplingLock = new ReentrantLock();
    private final InternalLog log;
    private final boolean logRecoverIndexSamples;
    private final boolean asyncRecoverIndexSamples;
    private final boolean asyncRecoverIndexSamplesWait;
    private final String databaseName;

    private JobHandle backgroundSamplingHandle;

    // use IndexSamplingControllerFactory.create do not instantiate directly
    IndexSamplingController(
            IndexSamplingConfig samplingConfig,
            IndexSamplingJobFactory jobFactory,
            LongPredicate samplingUpdatePredicate,
            IndexSamplingJobTracker jobTracker,
            IndexMapSnapshotProvider indexMapSnapshotProvider,
            JobScheduler scheduler,
            RecoveryCondition indexRecoveryCondition,
            InternalLogProvider logProvider,
            Config config,
            String databaseName) {
        this.backgroundSampling = samplingConfig.backgroundSampling();
        this.jobFactory = jobFactory;
        this.indexMapSnapshotProvider = indexMapSnapshotProvider;
        this.samplingUpdatePredicate = samplingUpdatePredicate;
        this.jobTracker = jobTracker;
        this.scheduler = scheduler;
        this.indexRecoveryCondition = indexRecoveryCondition;
        this.log = logProvider.getLog(getClass());
        this.logRecoverIndexSamples = config.get(GraphDatabaseInternalSettings.log_recover_index_samples);
        this.asyncRecoverIndexSamples = config.get(GraphDatabaseInternalSettings.async_recover_index_samples);
        this.asyncRecoverIndexSamplesWait = config.get(GraphDatabaseInternalSettings.async_recover_index_samples_wait);
        this.databaseName = databaseName;
    }

    public void sampleIndexes(IndexSamplingMode mode) {
        IndexMap indexMap = indexMapSnapshotProvider.indexMapSnapshot();
        LongIterable indexesToSample = indexesToSample(mode, indexMap);
        scheduleSampling(indexesToSample, mode, indexMap);
    }

    public void sampleIndex(long indexId, IndexSamplingMode mode) {
        IndexMap indexMap = indexMapSnapshotProvider.indexMapSnapshot();
        if (shouldSampleIndex(mode, indexId)) {
            scheduleSampling(LongLists.immutable.of(indexId), mode, indexMap);
        }
    }

    public void recoverIndexSamples() {
        samplingLock.lock();
        try {
            IndexMap indexMap = indexMapSnapshotProvider.indexMapSnapshot();
            final LongIterator indexIds = indexMap.indexIds();
            List<IndexSamplingJobHandle> asyncSamplingJobs = Lists.mutable.of();
            while (indexIds.hasNext()) {
                long indexId = indexIds.next();
                IndexDescriptor descriptor = indexMap.getIndexProxy(indexId).getDescriptor();
                if (indexRecoveryCondition.test(descriptor) && descriptor.getIndexType() != IndexType.LOOKUP) {
                    if (logRecoverIndexSamples) {
                        log.info("Index requires sampling, id=%d, name=%s.", indexId, descriptor.getName());
                    }

                    if (asyncRecoverIndexSamples) {
                        asyncSamplingJobs.add(sampleIndexOnTracker(indexMap, indexId));
                    } else {
                        sampleIndexOnCurrentThread(indexMap, indexId);
                    }
                } else {
                    if (logRecoverIndexSamples) {
                        log.info("Index does not require sampling, id=%d, name=%s.", indexId, descriptor.getName());
                    }
                }
            }
            if (asyncRecoverIndexSamplesWait) {
                waitForAsyncIndexSamples(asyncSamplingJobs);
            }
        } finally {
            samplingLock.unlock();
        }
    }

    private static void waitForAsyncIndexSamples(List<IndexSamplingJobHandle> asyncSamplingJobs) {
        for (IndexSamplingJobHandle asyncSamplingJob : asyncSamplingJobs) {
            try {
                asyncSamplingJob.waitTermination();
            } catch (InterruptedException | ExecutionException | CancellationException e) {
                String indexName = asyncSamplingJob.descriptor.getName();
                throw new RuntimeException(
                        "Failed to asynchronously sample index during recovery, index '" + indexName + "'.", e);
            }
        }
    }

    private void scheduleSampling(LongIterable indexesToSample, IndexSamplingMode mode, IndexMap indexMap) {
        List<IndexSamplingJobHandle> allJobs = scheduleAllSampling(indexesToSample, indexMap);

        long millisToWait = mode.millisToWaitForCompletion();
        if (millisToWait != IndexSamplingMode.NO_WAIT) {
            waitForAsyncIndexSamples(allJobs, millisToWait);
        }
    }

    private static void waitForAsyncIndexSamples(List<IndexSamplingJobHandle> allJobs, long millisToWait) {
        long start = System.nanoTime();
        long deadline = start + MILLISECONDS.toNanos(millisToWait);

        for (IndexSamplingJobHandle job : allJobs) {
            try {
                job.waitTermination(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
            } catch (TimeoutException e) {
                throw new RuntimeException(
                        format(
                                "Could not finish index sampling within the given time limit, %d milliseconds.",
                                millisToWait),
                        e);
            } catch (InterruptedException | ExecutionException e) {
                IndexDescriptor index = job.descriptor;
                throw new RuntimeException(
                        String.format(
                                "Index sampling of index '%s' failed, cause: %s", index.getName(), e.getMessage()),
                        e);
            }
        }
    }

    private List<IndexSamplingJobHandle> scheduleAllSampling(LongIterable indexesToSample, IndexMap indexMap) {
        samplingLock.lock();
        try {
            MutableList<IndexSamplingJobHandle> allJobs = Lists.mutable.of();
            indexesToSample.forEach(l -> allJobs.add(sampleIndexOnTracker(indexMap, l)));
            return allJobs;
        } finally {
            samplingLock.unlock();
        }
    }

    private IndexSamplingJobHandle sampleIndexOnTracker(IndexMap indexMap, long indexId) {
        IndexSamplingJob job = createSamplingJob(indexMap, indexId);
        IndexDescriptor descriptor = indexMap.getIndexProxy(indexId).getDescriptor();
        if (job != null) {
            return new IndexSamplingJobHandle(jobTracker.scheduleSamplingJob(job), descriptor);
        }
        return new IndexSamplingJobHandle(JobHandle.EMPTY, descriptor);
    }

    private void sampleIndexOnCurrentThread(IndexMap indexMap, long indexId) {
        IndexSamplingJob job = createSamplingJob(indexMap, indexId);
        if (job != null) {
            job.run(new AtomicBoolean(false));
        }
    }

    private IndexSamplingJob createSamplingJob(IndexMap indexMap, long indexId) {
        IndexProxy proxy = indexMap.getIndexProxy(indexId);
        if (proxy == null
                || proxy.getState() != InternalIndexState.ONLINE
                || proxy.getDescriptor().getIndexType() == LOOKUP) {
            return null;
        }
        return jobFactory.create(indexId, proxy);
    }

    public void start() {
        if (backgroundSampling) {
            Runnable samplingRunner = () -> sampleIndexes(backgroundRebuildUpdated());
            var monitoringParams =
                    JobMonitoringParams.systemJob(databaseName, "Background rebuilding of updated indexes");
            backgroundSamplingHandle =
                    scheduler.scheduleRecurring(Group.INDEX_SAMPLING, monitoringParams, samplingRunner, 10, SECONDS);
        }
    }

    public void stop() {
        if (backgroundSamplingHandle != null) {
            backgroundSamplingHandle.cancel();
        }
        jobTracker.stopAndAwaitAllJobs();
    }

    private LongList indexesToSample(IndexSamplingMode mode, IndexMap indexMap) {
        MutableLongList indexesToSample = LongLists.mutable.of();
        LongIterator allIndexes = indexMap.indexIds();
        while (allIndexes.hasNext()) {
            long indexId = allIndexes.next();
            if (shouldSampleIndex(mode, indexId)) {
                indexesToSample.add(indexId);
            }
        }
        return indexesToSample;
    }

    private boolean shouldSampleIndex(IndexSamplingMode mode, long indexId) {
        return !mode.sampleOnlyIfUpdated() || samplingUpdatePredicate.test(indexId);
    }

    private static class IndexSamplingJobHandle {
        private final JobHandle jobHandle;
        private final IndexDescriptor descriptor;

        IndexSamplingJobHandle(JobHandle jobHandle, IndexDescriptor descriptor) {
            this.jobHandle = jobHandle;
            this.descriptor = descriptor;
        }

        public void waitTermination() throws ExecutionException, InterruptedException {
            this.jobHandle.waitTermination();
        }

        public void waitTermination(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            this.jobHandle.waitTermination(timeout, unit);
        }
    }
}
