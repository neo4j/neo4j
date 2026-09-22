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

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.neo4j.internal.schema.IndexType.LOOKUP;
import static org.neo4j.io.IOUtils.closeAllUnchecked;
import static org.neo4j.kernel.impl.api.TransactionVisibilityProvider.EMPTY_VISIBILITY_PROVIDER;
import static org.neo4j.kernel.impl.api.index.IndexPopulationFailure.failure;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;
import org.neo4j.common.EntityType;
import org.neo4j.common.Subject;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.api.exceptions.index.ExceptionDuringFlipKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexProxyAlreadyClosedKernelException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.TransactionVisibilityProvider;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.EntityUpdates;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.TokenIndexEntryUpdate;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.storable.Value;

/**
 * There are two ways data is fed to this multi-populator:
 * <ul>
 * <li>A {@link StoreScan} is created through {@link #createStoreScan(CursorContextFactory)}. The store scan is started by
 * {@link StoreScan#run(StoreScan.ExternalUpdatesCheck)}, which is a blocking call and will scan the entire store and generate
 * updates that are fed into the {@link IndexPopulator populators}. Only a single call to this
 * method should be made during the life time of a {@link MultipleIndexPopulator} and should be called by the
 * same thread instantiating this instance.</li>
 * <li>{@link #queueConcurrentUpdate(IndexEntryUpdate, CursorContext)} which queues updates which will be read by the thread currently executing
 * the store scan and incorporated into that data stream. Calls to this method may come from any number
 * of concurrent threads.</li>
 * </ul>
 * <p>
 * Usage of this class should be something like:
 * <ol>
 * <li>Instantiation.</li>
 * <li>One or more calls to {@link #addPopulator(IndexPopulator, IndexProxyStrategy, FlippableIndexProxy)}.</li>
 * <li>Call to {@link #create(CursorContext)} to create data structures and files to start accepting updates.</li>
 * <li>Call to {@link #createStoreScan(CursorContextFactory)} and {@link StoreScan#run(StoreScan.ExternalUpdatesCheck)}(blocking call).</li>
 * <li>While all nodes are being indexed, calls to {@link #queueConcurrentUpdate(IndexEntryUpdate, CursorContext)} are accepted.</li>
 * <li>Call to {@link #flipAfterStoreScan(CursorContext, boolean)} after successful population, or {@link #cancel(Throwable, CursorContext)} if not</li>
 * </ol>
 * <p>
 * It is possible for concurrent updates from transactions to arrive while index population is in progress. Such
 * updates are inserted in the {@link #queueConcurrentUpdate(IndexEntryUpdate, CursorContext) queue}. When store scan notices that
 * queue size has reached {@link #queueThreshold} then it drains all batched updates and waits for all job scheduler
 * tasks to complete and flushes updates from the queue using {@link MultipleIndexUpdater}. If queue size never reaches
 * {@link #queueThreshold} than all queued concurrent updates are flushed after the store scan in
 * {@link #flipAfterStoreScan(CursorContext, boolean)}.
 * <p>
 */
public class MultipleIndexPopulator implements StoreScan.ExternalUpdatesCheck, AutoCloseable {
    private static final String MULTIPLE_INDEX_POPULATOR_TAG = "multipleIndexPopulator";
    private static final String EXTERNAL_UPDATES_QUEUE_TAG = "multipleIndexPopulator.externalUpdatesQueue";
    private static final String POPULATION_WORK_FLUSH_TAG = "populationWorkFlush";
    private static final String EOL = System.lineSeparator();

    private static final long VERSIONED_ENTRY_UPDATE_SIZE =
            HeapEstimator.shallowSizeOfInstance(VersionedEntryUpdate.class);
    private static final long HORIZON_POLL_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(10);

    private final int queueThreshold;
    final int batchMaxByteSizeScan;

    // Concurrency queue since multiple concurrent threads may enqueue updates into it. It is important for this queue
    // to have fast #size() method since it might be drained in batches
    private final Queue<VersionedEntryUpdate> concurrentUpdateQueue = new LinkedBlockingQueue<>();
    private final AtomicLong concurrentUpdateQueueByteSize = new AtomicLong();

    // Populators are added into this list. The same thread adding populators will later call #createStoreScan.
    // Multiple concurrent threads might fail individual populations.
    // Failed populations are removed from this list while iterating over it.
    private final ConcurrentHashMap<IndexDescriptor, IndexPopulation> populations = new ConcurrentHashMap<>();

    private final AtomicLong activeTasks = new AtomicLong();
    private final IndexStoreView storeView;
    private final CursorContextFactory contextFactory;
    private final InternalLogProvider logProvider;
    private final InternalLog log;
    private final EntityType type;
    private final SchemaState schemaState;
    private final PhaseTracker phaseTracker;
    private final JobScheduler jobScheduler;
    private final CursorContext cursorContext;
    private final MemoryTracker memoryTracker;
    private volatile StoreScan storeScan;
    private final TokenNameLookup tokenNameLookup;
    private final String databaseName;
    private final Subject subject;
    private final TransactionVisibilityProvider transactionVisibilityProvider;
    private final Runnable transactionMonitorRefresher;
    private final IndexMonitor monitor;
    private final AtomicBoolean populationJobStopped = new AtomicBoolean(false);
    private final long transactionIdCreatedIndexes;
    private final boolean multiversion;
    private volatile long populationHorizon;

    public MultipleIndexPopulator(
            IndexStoreView storeView,
            InternalLogProvider logProvider,
            EntityType type,
            SchemaState schemaState,
            JobScheduler jobScheduler,
            TokenNameLookup tokenNameLookup,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker,
            String databaseName,
            Subject subject,
            Config config,
            TransactionVisibilityProvider transactionVisibilityProvider,
            Runnable transactionMonitorRefresher,
            IndexMonitor monitor,
            CursorContext cursorContextOfIndexCreator,
            boolean multiversion) {
        this.storeView = storeView;
        this.contextFactory = contextFactory;
        this.cursorContext = contextFactory.create(MULTIPLE_INDEX_POPULATOR_TAG);
        this.memoryTracker = memoryTracker;
        this.logProvider = logProvider;
        this.log = logProvider.getLog(IndexPopulationJob.class);
        this.type = type;
        this.schemaState = schemaState;
        this.phaseTracker = new LoggingPhaseTracker(logProvider.getLog(IndexPopulationJob.class));
        this.jobScheduler = jobScheduler;
        this.tokenNameLookup = tokenNameLookup;
        this.databaseName = databaseName;
        this.subject = subject;

        this.queueThreshold = config.get(GraphDatabaseInternalSettings.index_population_queue_threshold);
        this.batchMaxByteSizeScan = config.get(GraphDatabaseInternalSettings.index_population_batch_max_byte_size)
                .intValue();
        this.transactionVisibilityProvider = transactionVisibilityProvider;
        this.transactionMonitorRefresher = transactionMonitorRefresher;
        this.monitor = monitor;
        this.transactionIdCreatedIndexes =
                cursorContextOfIndexCreator.getVersionContext().committingTransactionId();
        this.multiversion = multiversion;
        this.populationHorizon = Long.MAX_VALUE;
    }

    IndexPopulation addPopulator(
            IndexPopulator populator, IndexProxyStrategy indexProxyStrategy, FlippableIndexProxy flipper) {
        IndexPopulation population = createPopulation(populator, indexProxyStrategy, flipper);
        populations.put(indexProxyStrategy.getIndexDescriptor(), population);
        return population;
    }

    private IndexPopulation createPopulation(
            IndexPopulator populator, IndexProxyStrategy indexProxyStrategy, FlippableIndexProxy flipper) {
        return new IndexPopulation(populator, indexProxyStrategy, flipper);
    }

    boolean hasPopulators() {
        return !populations.isEmpty();
    }

    public void create(CursorContext cursorContext) {
        forEachPopulation(
                population -> {
                    log.info("Index population started: [%s]", population.userDescription(tokenNameLookup));
                    population.create(cursorContext);
                },
                cursorContext);
    }

    StoreScan createStoreScan(CursorContextFactory contextFactory) {
        int[] entityTokenIds = entityTokenIds();
        int[] propertyKeyIds = propertyKeyIds();
        var propertySelection = PropertySelection.selection(propertyKeyIds);

        if (type == EntityType.RELATIONSHIP) {
            StoreScan innerStoreScan = storeView.visitRelationships(
                    entityTokenIds,
                    propertySelection,
                    createPropertyScanConsumer(),
                    createTokenScanConsumer(),
                    false,
                    true,
                    contextFactory,
                    memoryTracker);
            storeScan = new LoggingStoreScan(innerStoreScan, false);
        } else {
            StoreScan innerStoreScan = storeView.visitNodes(
                    entityTokenIds,
                    propertySelection,
                    createPropertyScanConsumer(),
                    createTokenScanConsumer(),
                    false,
                    true,
                    contextFactory,
                    memoryTracker);
            storeScan = new LoggingStoreScan(innerStoreScan, true);
        }
        storeScan.setPhaseTracker(phaseTracker);
        return storeScan;
    }

    /**
     * Queues an update to be fed into the index populators. These updates come from changes being made
     * to storage while a concurrent scan is happening to keep populators up to date with all latest changes.
     *
     * @param update        {@link IndexEntryUpdate} to queue.
     * @param cursorContext context of transaction applying update
     */
    void queueConcurrentUpdate(IndexEntryUpdate update, CursorContext cursorContext) {
        var entryUpdate = new VersionedEntryUpdate(
                update.eagerly(), cursorContext.getVersionContext().committingTransactionId());
        concurrentUpdateQueue.add(entryUpdate);
        concurrentUpdateQueueByteSize.addAndGet(entryUpdate.heapSize());
    }

    /**
     * Cancel all {@link IndexPopulation index populations}, putting the indexes in {@link InternalIndexState#FAILED failed state}.
     * To repopulate them they will need to be dropped and recreated.
     *
     * @param failure the cause.
     */
    public void cancel(Throwable failure, CursorContext cursorContext) {
        for (IndexPopulation population : populations.values()) {
            cancel(population, failure, cursorContext);
        }
    }

    /**
     * Cancel a single {@link IndexPopulation index population}, putting the index in {@link InternalIndexState#FAILED failed state}.
     * To repopulate the index it needs to be dropped and recreated.
     *
     * @param population Index population to cancel.
     * @param failure the cause.
     */
    protected void cancel(IndexPopulation population, Throwable failure, CursorContext cursorContext) {
        if (!removeFromOngoingPopulations(population)) {
            return;
        }

        // If the cause of index population failure is a conflict in a (unique) index, the conflict is the failure
        if (failure instanceof IndexPopulationFailedKernelException) {
            Throwable cause = failure.getCause();
            if (cause instanceof IndexEntryConflictException) {
                failure = cause;
            }
        }

        log.error(format("Failed to populate index: [%s]", population.userDescription(tokenNameLookup)), failure);

        // The flipper will have already flipped to a failed index context here, but
        // it will not include the cause of failure, so we do another flip to a failed
        // context that does.

        // The reason for having the flipper transition to the failed index context in the first
        // place is that we would otherwise introduce a race condition where updates could come
        // in to the old context, if something failed in the job we send to the flipper.
        IndexPopulationFailure indexPopulationFailure = failure(failure);
        population.cancel(indexPopulationFailure);
        try {
            population.populator.markAsFailed(indexPopulationFailure.asString());
            population.populator.close(false, cursorContext);
        } catch (Throwable e) {
            log.error(
                    format(
                            "Unable to close failed populator for index: [%s]",
                            population.userDescription(tokenNameLookup)),
                    e);
        }
    }

    @VisibleForTesting
    MultipleIndexUpdater newPopulatingUpdater(CursorContext cursorContext, CursorContext populatorContext) {
        MutableLongObjectMap<IndexPopulationUpdater> updaters =
                LongObjectMaps.mutable.withInitialCapacity(populations.size());
        forEachPopulation(
                population -> updaters.put(
                        population.indexProxyStrategy.getIndexDescriptor().getId(),
                        new IndexPopulationUpdater(
                                population, population.populator.newPopulatingUpdater(populatorContext))),
                cursorContext);
        return new MultipleIndexUpdater(this, updaters, logProvider, cursorContext);
    }

    /**
     * Close this {@link MultipleIndexPopulator multiple index populator}.
     * This means population job has finished, successfully or unsuccessfully and resources can be released.
     *
     * Note that {@link IndexPopulation index populations} cannot be closed. Instead, the underlying
     * {@link IndexPopulator index populator} is closed by {@link #flipAfterStoreScan(CursorContext, boolean)},
     * {@link #cancel(IndexPopulation, Throwable, CursorContext)} or {@link #stop(IndexPopulation, CursorContext)}.
     */
    @Override
    public void close() {
        phaseTracker.stop();
        closeAllUnchecked(storeScan, cursorContext);
    }

    void resetIndexCounts(CursorContext cursorContext) {
        forEachPopulation(this::resetIndexCountsForPopulation, cursorContext);
    }

    private void resetIndexCountsForPopulation(IndexPopulation indexPopulation) {
        indexPopulation.indexProxyStrategy.replaceStatisticsForIndex(new IndexSample(0, 0, 0));
    }

    /**
     * This concludes a successful index population.
     *
     * The last updates will be applied to every index,
     * tell {@link IndexPopulator index populators} that scan has been completed,
     * {@link IndexStatisticsStore index statistics store} will be updated with {@link IndexSample index samples},
     * {@link SchemaState schema cache} will be cleared,
     * {@link IndexPopulator index populators} will be closed and
     * {@link IndexProxy index proxy} will be {@link FlippableIndexProxy#flip(Callable)}  flipped}
     * to {@link OnlineIndexProxy online}, given that nothing goes wrong.
     *
     */
    void flipAfterStoreScan(CursorContext cursorContext, boolean awaitHorizon) {
        for (IndexPopulation population : populations.values()) {
            try {
                population.scanCompleted(cursorContext);
                if (populationJobStopped.getAcquire()) {
                    // Stopped while doing post-scan work, so that work was abandoned part-way through. Flipping now
                    // would publish an index built from whatever it happened to finish - for a vector index, one left
                    // permanently uncompacted, with nothing to ever trigger the merge again. Keep it populating so
                    // that it gets rebuilt instead.
                    stop(population, cursorContext);
                    continue;
                }
                population.flip(cursorContext, awaitHorizon);
            } catch (Throwable t) {
                cancel(population, t, cursorContext);
            }
        }
    }

    private int[] propertyKeyIds() {
        return populations.values().stream()
                .flatMapToInt(this::propertyKeyIds)
                .distinct()
                .toArray();
    }

    private IntStream propertyKeyIds(IndexPopulation population) {
        return IntStream.of(population.schema().getPropertyIds());
    }

    private int[] entityTokenIds() {
        return populations.values().stream()
                .flatMapToInt(population -> Arrays.stream(population.schema().getEntityTokenIds()))
                .sorted()
                .distinct()
                .toArray();
    }

    /**
     * Stop all {@link IndexPopulation index populations}, closing backing {@link IndexPopulator index populators},
     * keeping them in {@link InternalIndexState#POPULATING populating state}.
     */
    public void stop(CursorContext cursorContext) {
        forEachPopulation(population -> this.stop(population, cursorContext), cursorContext);
    }

    /**
     * Close specific {@link IndexPopulation index population}, closing backing {@link IndexPopulator index populator},
     * keeping it in {@link InternalIndexState#POPULATING populating state}.
     * @param indexPopulation {@link IndexPopulation} to stop.
     */
    void stop(IndexPopulation indexPopulation, CursorContext cursorContext) {
        indexPopulation.disconnectAndStop(cursorContext);
        checkEmpty();
    }

    private void checkEmpty() {
        StoreScan scan = storeScan;
        if (populations.isEmpty() && scan != null) {
            scan.stop();
        }
    }

    /**
     * Stop population of given {@link IndexPopulation} and drop the index.
     * @param indexPopulation {@link IndexPopulation} to drop.
     */
    void dropIndexPopulation(IndexPopulation indexPopulation) {
        indexPopulation.disconnectAndDrop();
        checkEmpty();
    }

    private boolean removeFromOngoingPopulations(IndexPopulation indexPopulation) {
        return populations.remove(indexPopulation.indexProxyStrategy.getIndexDescriptor()) != null;
    }

    @Override
    public boolean needToApplyExternalUpdates() {
        int queueSize = concurrentUpdateQueue.size();
        return (queueSize > 0 && queueSize >= queueThreshold)
                || concurrentUpdateQueueByteSize.get() >= batchMaxByteSizeScan;
    }

    @Override
    public void applyExternalUpdates(long currentlyIndexedEntityId) {
        if (concurrentUpdateQueue.isEmpty()) {
            return;
        }

        // index updates for the entry are guaranteed to come from transactions that observe exact previous version of
        // the entry
        // in concurrent updates queue we only preserve commiting transaction id and use unbounded visibility here,
        // so index updater can correctly process entries and merge them if needed.
        // this is especially important for token indexes where one tree entry represents multiple entities
        try (var populatorContext = cursorContext.createUnboundedReadRelatedContext(EXTERNAL_UPDATES_QUEUE_TAG);
                var updater = newPopulatingUpdater(cursorContext, populatorContext)) {
            long updateByteSizeDrained = 0;
            do {
                var update = concurrentUpdateQueue.poll();
                if (update != null) {
                    // Since updates can be added concurrently with us draining the queue simply setting the value to 0
                    // after drained will not be 100% synchronized with the queue contents and could potentially cause a
                    // large
                    // drift over time. Therefore each update polled from the queue will subtract its size instead.
                    var entryUpdate = update.entryUpdate;
                    updateByteSizeDrained += update.heapSize();
                    if (entryUpdate.getEntityId() <= currentlyIndexedEntityId) {
                        populatorContext.getVersionContext().initWrite(update.transactionId);
                        updater.process(entryUpdate);
                    }
                }
            } while (!concurrentUpdateQueue.isEmpty());
            concurrentUpdateQueueByteSize.addAndGet(-updateByteSizeDrained);
            monitor.concurrentUpdatesQueueDrained(updateByteSizeDrained);
        }
    }

    private void forEachPopulation(ThrowingConsumer<IndexPopulation, Exception> action, CursorContext cursorContext) {
        for (IndexPopulation population : populations.values()) {
            try {
                action.accept(population);
            } catch (Throwable failure) {
                cancel(population, failure, cursorContext);
            }
        }
    }

    private PropertyScanConsumer createPropertyScanConsumer() {
        // are we going to populate only token indexes?
        if (populations.values().stream()
                .allMatch(population ->
                        population.indexProxyStrategy.getIndexDescriptor().getIndexType() == LOOKUP)) {
            return null;
        }

        return new PropertyScanConsumerImpl();
    }

    private TokenScanConsumer createTokenScanConsumer() {
        // is there a token index among the to-be-populated indexes?
        var maybeTokenIdxPopulation = populations.values().stream()
                .filter(population ->
                        population.indexProxyStrategy.getIndexDescriptor().getIndexType() == LOOKUP)
                .findAny();
        return maybeTokenIdxPopulation.map(TokenScanConsumerImpl::new).orElse(null);
    }

    @Override
    public String toString() {
        String updatesString =
                populations.values().stream().map(Object::toString).collect(joining(", ", "[", "]"));

        return "MultipleIndexPopulator{activeTasks=" + activeTasks + ", " + "batchedUpdatesFromScan = " + updatesString
                + ", concurrentUpdateQueue = " + concurrentUpdateQueue.size() + "}";
    }

    IndexDescriptor[] indexDescriptors() {
        return populations.values().stream()
                .map(p -> p.indexProxyStrategy.getIndexDescriptor())
                .toArray(IndexDescriptor[]::new);
    }

    public void notifyPopulationJobStopped() {
        populationJobStopped.setRelease(true);
        // Populators doing long-running post-scan work need to hear about this directly: the job is stopped by a
        // thread that then waits for the population to finish, so it would otherwise wait for that work to complete.
        for (IndexPopulation population : populations.values()) {
            population.populator.cancelPostScanWork();
        }
    }

    public void refreshVisibility(CursorContext cursorContext) {
        cursorContext.getVersionContext().refreshVisibilityBoundaries();
        populationHorizon = cursorContext.getVersionContext().highestGapFree();
        forEachPopulation(population -> population.resetVisibility(cursorContext), cursorContext);
    }

    /**
     * Earliest transaction id that should be accessible for this population to continue.
     * Job starts with horizon equal transactionIdCreatedIndexes, to prevent global horizon from moving past it and
     * causing race in {@link #refreshVisibility(CursorContext)}
     * When population ready to start store scan it bumps horizon to the last closed transaction at the moment
     */
    public long populationHorison() {
        return populationHorizon;
    }

    static final class MultipleIndexUpdater implements AutoCloseable {
        private final MutableLongObjectMap<IndexPopulationUpdater> populationsWithUpdaters;
        private final MultipleIndexPopulator multipleIndexPopulator;
        private final InternalLog log;
        private final CursorContext cursorContext;

        MultipleIndexUpdater(
                MultipleIndexPopulator multipleIndexPopulator,
                MutableLongObjectMap<IndexPopulationUpdater> populationsWithUpdaters,
                InternalLogProvider logProvider,
                CursorContext cursorContext) {
            this.multipleIndexPopulator = multipleIndexPopulator;
            this.populationsWithUpdaters = populationsWithUpdaters;
            this.log = logProvider.getLog(getClass());
            this.cursorContext = cursorContext;
        }

        public void process(IndexEntryUpdate update) {
            IndexPopulationUpdater populationUpdater =
                    populationsWithUpdaters.get(update.indexKey().getId());
            if (populationUpdater != null) {
                IndexPopulation population = populationUpdater.population;
                IndexUpdater updater = populationUpdater.updater;

                try {
                    population.populator.includeSample(update);
                    updater.process(update);
                } catch (Throwable t) {
                    try {
                        updater.close();
                    } catch (Throwable ce) {
                        log.error(format("Failed to close index updater: [%s]", updater), ce);
                    }
                    populationsWithUpdaters.remove(update.indexKey().getId());
                    multipleIndexPopulator.cancel(population, t, cursorContext);
                }
            }
        }

        @Override
        public void close() {
            for (IndexPopulationUpdater populationUpdater : populationsWithUpdaters.values()) {
                try {
                    populationUpdater.updater.close();
                } catch (Throwable t) {
                    multipleIndexPopulator.cancel(populationUpdater.population, t, cursorContext);
                }
            }
            populationsWithUpdaters.clear();
        }
    }

    public class IndexPopulation implements SchemaDescriptorSupplier {
        public final IndexPopulator populator;
        final FlippableIndexProxy flipper;
        private final IndexProxyStrategy indexProxyStrategy;
        private boolean populationOngoing = true;
        private final ReentrantLock populatorLock = new ReentrantLock();
        private long highestClosedTxAtPopulationStart = BASE_TX_ID;

        IndexPopulation(IndexPopulator populator, IndexProxyStrategy indexProxyStrategy, FlippableIndexProxy flipper) {
            this.populator = populator;
            this.indexProxyStrategy = indexProxyStrategy;
            this.flipper = flipper;
        }

        private void cancel(IndexPopulationFailure failure) {
            flipper.flipTo(new FailedIndexProxy(indexProxyStrategy, populator, failure, logProvider));
        }

        void create(CursorContext cursorContext) throws IOException {
            populatorLock.lock();
            try {
                if (populationOngoing) {
                    populator.create();
                    highestClosedTxAtPopulationStart =
                            cursorContext.getVersionContext().highestClosed();
                }
            } finally {
                populatorLock.unlock();
            }
        }

        void resetVisibility(CursorContext cursorContext) {
            populatorLock.lock();
            try {
                highestClosedTxAtPopulationStart =
                        cursorContext.getVersionContext().highestClosed();
            } finally {
                populatorLock.unlock();
            }
        }

        /**
         * Disconnect this single {@link IndexPopulation index population} from ongoing multiple index population
         * and close {@link IndexPopulator index populator}, leaving it in {@link InternalIndexState#POPULATING populating state}.
         */
        void disconnectAndStop(CursorContext cursorContext) {
            disconnect(() -> populator.close(false, cursorContext));
        }

        /**
         * Disconnect this single {@link IndexPopulation index population} from ongoing multiple index population
         * and {@link IndexPopulator#drop() drop} the index.
         */
        void disconnectAndDrop() {
            disconnect(populator::drop);
        }

        private void disconnect(Runnable specificPopulatorOperation) {
            populatorLock.lock();
            try {
                if (populationOngoing) {
                    // First of all remove this population from the list of ongoing populations so that it won't receive
                    // more updates.
                    // This is good because closing the populator may wait for an opportunity to perform the close,
                    // among the incoming writes to it.
                    removeFromOngoingPopulations(this);
                    specificPopulatorOperation.run();
                    resetIndexCountsForPopulation(this);
                    populationOngoing = false;
                }
            } finally {
                populatorLock.unlock();
            }
        }

        void flip(CursorContext cursorContext, boolean awaitHorizon)
                throws IndexProxyAlreadyClosedKernelException, ExceptionDuringFlipKernelException {
            phaseTracker.enterPhase(PhaseTracker.Phase.FLIP);
            if (awaitHorizon && populationOngoing) {
                // scan is completed, population should allow horizon to progress further to avoid deadlock with the
                // next statement
                populationHorizon = highestClosedTxAtPopulationStart;
                // In multiversion database index must remain pouplating until everything that was added into index
                // through the store scan is visible by any current and future transactions.
                // To achieve this we remember highestEverSeen transaction at population start and don't flip until
                // horizon reaches that transaction
                awaitUntilHorizonReached(highestClosedTxAtPopulationStart);
            }
            flipper.flip(() -> {
                populatorLock.lock();
                try {
                    if (populationOngoing) {
                        applyExternalUpdates(Long.MAX_VALUE);
                        var indexDescriptor = indexProxyStrategy.getIndexDescriptor();
                        if (populations.containsKey(indexDescriptor)) {
                            if (indexDescriptor.getIndexType() != IndexType.LOOKUP) {
                                IndexSample sample = populator.sample(cursorContext);
                                indexProxyStrategy.replaceStatisticsForIndex(sample);
                            }
                            populator.close(true, cursorContext);
                            schemaState.clear();
                            return true;
                        }
                    }
                    return false;
                } finally {
                    logCompletionMessage();
                    populationOngoing = false;
                    populatorLock.unlock();
                }
            });
            removeFromOngoingPopulations(this);
        }

        private void logCompletionMessage() {
            log.info("Index creation finished for index [%s].", indexProxyStrategy.getIndexUserDescription());
        }

        @Override
        public SchemaDescriptor schema() {
            return indexProxyStrategy.getIndexDescriptor().schema();
        }

        @Override
        public String userDescription(TokenNameLookup tokenNameLookup) {
            return indexProxyStrategy.getIndexUserDescription();
        }

        void scanCompleted(CursorContext cursorContext) throws IndexEntryConflictException {
            IndexPopulator.PopulationWorkScheduler populationWorkScheduler =
                    new IndexPopulator.PopulationWorkScheduler() {
                        @Override
                        public <T> JobHandle<T> schedule(
                                IndexPopulator.JobDescriptionSupplier descriptionSupplier, Callable<T> job) {
                            var description = descriptionSupplier.getJobDescription(
                                    indexProxyStrategy.getIndexDescriptor().getName());
                            var jobMonitoringParams = new JobMonitoringParams(subject, databaseName, description);
                            return jobScheduler.schedule(Group.INDEX_POPULATION_WORK, jobMonitoringParams, job);
                        }
                    };

            if (multiversion) {
                cursorContext.getVersionContext().initWrite(transactionIdCreatedIndexes);
            }
            populator.scanCompleted(phaseTracker, populationWorkScheduler, cursorContext);
        }

        PopulationProgress progress(PopulationProgress storeScanProgress) {
            return populator.progress(storeScanProgress);
        }
    }

    public void awaitHorizonBeforeScan() {
        awaitUntilHorizonReached(transactionIdCreatedIndexes);
    }

    private void awaitUntilHorizonReached(long targetTransaction) {
        if (EMPTY_VISIBILITY_PROVIDER.equals(transactionVisibilityProvider)) {
            return;
        }
        while (!populationJobStopped.getAcquire()
                && transactionVisibilityProvider.oldestCleanupHorizon() < targetTransaction) {
            LockSupport.parkNanos(HORIZON_POLL_INTERVAL_NANOS);
            if (needToApplyExternalUpdates()) {
                applyExternalUpdates(Long.MAX_VALUE);
            }
            transactionMonitorRefresher.run();
        }
    }

    private class PropertyScanConsumerImpl implements PropertyScanConsumer {
        @Override
        public Batch newBatch() {
            return new Batch() {
                final List<EntityUpdates> updates = new ArrayList<>();

                @Override
                public long addRecord(
                        long entityId, int[] tokens, Map<Integer, Value> properties, MemoryTracker memoryTracker) {
                    long heapSize = EntityUpdates.SHALLOW_SIZE + HeapEstimator.sizeOf(tokens);
                    var builder = EntityUpdates.forEntity(entityId, true).withTokens(tokens);
                    for (var property : properties.entrySet()) {
                        var key = property.getKey();
                        var value = property.getValue();
                        builder.added(key, value);
                        heapSize += value.estimatedHeapUsage();
                    }
                    memoryTracker.allocateHeap(heapSize);
                    updates.add(builder.build());
                    return heapSize;
                }

                @Override
                public void process() {
                    try (var cursorContext = contextFactory.create(POPULATION_WORK_FLUSH_TAG)) {
                        addFromScan(updates, cursorContext);
                    }
                }
            };
        }

        private void addFromScan(List<EntityUpdates> entityUpdates, CursorContext cursorContext) {
            // This is called from a full store node scan, meaning that all node properties are included in the
            // EntityUpdates object. Therefore no additional properties need to be loaded.
            Map<IndexPopulation, List<IndexEntryUpdate>> updates = HashMap.newHashMap(populations.size());
            var descriptors = populations.keySet();
            for (EntityUpdates update : entityUpdates) {
                for (var indexUpdate : update.valueUpdatesForIndexKeys(descriptors)) {
                    IndexPopulation population = populations.get(indexUpdate.indexKey());
                    // population could be cancelled concurrently and removed from the map
                    if (population != null) {
                        population.populator.includeSample(indexUpdate);
                        updates.computeIfAbsent(population, p -> new ArrayList<>())
                                .add(indexUpdate);
                    }
                }
            }
            for (Map.Entry<IndexPopulation, List<IndexEntryUpdate>> entry : updates.entrySet()) {
                try {
                    entry.getKey().populator.add(entry.getValue(), cursorContext);
                } catch (Throwable e) {
                    cancel(entry.getKey(), e, cursorContext);
                }
            }
        }
    }

    private class TokenScanConsumerImpl implements TokenScanConsumer {
        private final IndexPopulation population;

        TokenScanConsumerImpl(IndexPopulation population) {
            this.population = population;
        }

        @Override
        public Batch newBatch() {
            return new Batch() {
                private final List<TokenIndexEntryUpdate> updates = new ArrayList<>();

                @Override
                public long addRecord(long entityId, int[] tokens, MemoryTracker memoryTracker) {
                    long heapSize = TokenIndexEntryUpdate.SHALLOW_SIZE + HeapEstimator.sizeOf(tokens);
                    memoryTracker.allocateHeap(heapSize);
                    updates.add(TokenIndexEntryUpdate.tokenChange(
                            entityId, population.indexProxyStrategy.getIndexDescriptor(), EMPTY_INT_ARRAY, tokens));
                    return heapSize;
                }

                @Override
                public void process() {
                    try (var populationContext =
                            cursorContext.createUnboundedReadRelatedContext(MULTIPLE_INDEX_POPULATOR_TAG)) {
                        population.populator.add(updates, populationContext);
                    } catch (Throwable e) {
                        cancel(population, e, cursorContext);
                    }
                }
            };
        }
    }

    /**
     * A delegating {@link StoreScan} with the only functionality being logging when the scan is completed.
     */
    private class LoggingStoreScan implements StoreScan {
        private final StoreScan delegate;
        private final boolean nodeScan;

        LoggingStoreScan(StoreScan delegate, boolean nodeScan) {
            this.delegate = delegate;
            this.nodeScan = nodeScan;
        }

        @Override
        public void run(ExternalUpdatesCheck externalUpdatesCheck) {
            delegate.run(externalUpdatesCheck);
            String entityType = nodeScan ? "node" : "relationship";
            log.debug("Completed " + entityType + " store scan. Flushing all pending updates." + EOL
                    + MultipleIndexPopulator.this);
        }

        @Override
        public void stop() {
            delegate.stop();
        }

        @Override
        public PopulationProgress getProgress() {
            return delegate.getProgress();
        }

        @Override
        public void setPhaseTracker(PhaseTracker phaseTracker) {
            delegate.setPhaseTracker(phaseTracker);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    private record IndexPopulationUpdater(IndexPopulation population, IndexUpdater updater) {}

    private record VersionedEntryUpdate(IndexEntryUpdate entryUpdate, long transactionId) {
        long heapSize() {
            return VERSIONED_ENTRY_UPDATE_SIZE + entryUpdate.roughSizeOfUpdate();
        }
    }
}
