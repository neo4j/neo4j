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
import static org.neo4j.kernel.impl.api.index.IndexPopulationFailure.failure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;
import org.neo4j.common.EntityType;
import org.neo4j.common.Subject;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.function.ThrowingConsumer;
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
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
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
 * <li>{@link #queueConcurrentUpdate(IndexEntryUpdate)} which queues updates which will be read by the thread currently executing
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
 * <li>While all nodes are being indexed, calls to {@link #queueConcurrentUpdate(IndexEntryUpdate)} are accepted.</li>
 * <li>Call to {@link #flipAfterStoreScan(CursorContext)} after successful population, or {@link #cancel(Throwable, CursorContext)} if not</li>
 * </ol>
 * <p>
 * It is possible for concurrent updates from transactions to arrive while index population is in progress. Such
 * updates are inserted in the {@link #queueConcurrentUpdate(IndexEntryUpdate) queue}. When store scan notices that
 * queue size has reached {@link #queueThreshold} then it drains all batched updates and waits for all job scheduler
 * tasks to complete and flushes updates from the queue using {@link MultipleIndexUpdater}. If queue size never reaches
 * {@link #queueThreshold} than all queued concurrent updates are flushed after the store scan in
 * {@link #flipAfterStoreScan(CursorContext)}.
 * <p>
 */
public class MultipleIndexPopulator implements StoreScan.ExternalUpdatesCheck, AutoCloseable {
    private static final String MULTIPLE_INDEX_POPULATOR_TAG = "multipleIndexPopulator";
    private static final String POPULATION_WORK_FLUSH_TAG = "populationWorkFlush";
    private static final String EOL = System.lineSeparator();

    private final int queueThreshold;
    final int batchMaxByteSizeScan;
    private final boolean printDebug;

    // Concurrency queue since multiple concurrent threads may enqueue updates into it. It is important for this queue
    // to have fast #size() method since it might be drained in batches
    private final Queue<IndexEntryUpdate<?>> concurrentUpdateQueue = new LinkedBlockingQueue<>();
    private final AtomicLong concurrentUpdateQueueByteSize = new AtomicLong();

    // Populators are added into this list. The same thread adding populators will later call #createStoreScan.
    // Multiple concurrent threads might fail individual populations.
    // Failed populations are removed from this list while iterating over it.
    private final List<IndexPopulation> populations = new CopyOnWriteArrayList<>();

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
            Config config) {
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

        this.printDebug = config.get(GraphDatabaseInternalSettings.index_population_print_debug);
        this.queueThreshold = config.get(GraphDatabaseInternalSettings.index_population_queue_threshold);
        this.batchMaxByteSizeScan = config.get(GraphDatabaseInternalSettings.index_population_batch_max_byte_size)
                .intValue();
    }

    IndexPopulation addPopulator(
            IndexPopulator populator, IndexProxyStrategy indexProxyStrategy, FlippableIndexProxy flipper) {
        IndexPopulation population = createPopulation(populator, indexProxyStrategy, flipper);
        populations.add(population);
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
                    population.create();
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
     * @param update {@link IndexEntryUpdate} to queue.
     */
    void queueConcurrentUpdate(IndexEntryUpdate<?> update) {
        concurrentUpdateQueue.add(update);
        concurrentUpdateQueueByteSize.addAndGet(update.roughSizeOfUpdate());
    }

    /**
     * Cancel all {@link IndexPopulation index populations}, putting the indexes in {@link InternalIndexState#FAILED failed state}.
     * To repopulate them they will need to be dropped and recreated.
     *
     * @param failure the cause.
     */
    public void cancel(Throwable failure, CursorContext cursorContext) {
        for (IndexPopulation population : populations) {
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
    MultipleIndexUpdater newPopulatingUpdater(CursorContext cursorContext) {
        Map<SchemaDescriptor, IndexPopulationUpdater> updaters = new HashMap<>();
        forEachPopulation(
                population -> {
                    IndexUpdater updater = population.populator.newPopulatingUpdater(cursorContext);
                    updaters.put(population.schema(), new IndexPopulationUpdater(population, updater));
                },
                cursorContext);
        return new MultipleIndexUpdater(this, updaters, logProvider, cursorContext);
    }

    /**
     * Close this {@link MultipleIndexPopulator multiple index populator}.
     * This means population job has finished, successfully or unsuccessfully and resources can be released.
     *
     * Note that {@link IndexPopulation index populations} cannot be closed. Instead, the underlying
     * {@link IndexPopulator index populator} is closed by {@link #flipAfterStoreScan(CursorContext)},
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
    void flipAfterStoreScan(CursorContext cursorContext) {
        for (IndexPopulation population : populations) {
            try {
                population.scanCompleted(cursorContext);
                population.flip(cursorContext);
            } catch (Throwable t) {
                cancel(population, t, cursorContext);
            }
        }
    }

    private int[] propertyKeyIds() {
        return populations.stream()
                .flatMapToInt(this::propertyKeyIds)
                .distinct()
                .toArray();
    }

    private IntStream propertyKeyIds(IndexPopulation population) {
        return IntStream.of(population.schema().getPropertyIds());
    }

    private int[] entityTokenIds() {
        return populations.stream()
                .flatMapToInt(population -> Arrays.stream(population.schema().getEntityTokenIds()))
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
        return populations.remove(indexPopulation);
    }

    @Override
    public boolean needToApplyExternalUpdates() {
        int queueSize = concurrentUpdateQueue.size();
        return (queueSize > 0 && queueSize >= queueThreshold)
                || concurrentUpdateQueueByteSize.get() >= batchMaxByteSizeScan;
    }

    @Override
    public void applyExternalUpdates(long currentlyIndexedNodeId) {
        if (concurrentUpdateQueue.isEmpty()) {
            return;
        }

        if (printDebug) {
            log.info("Populating from queue at %d", currentlyIndexedNodeId);
        }

        long updateByteSizeDrained = 0;
        try (MultipleIndexUpdater updater = newPopulatingUpdater(cursorContext)) {
            do {
                // no need to check for null as nobody else is emptying this queue
                IndexEntryUpdate<?> update = concurrentUpdateQueue.poll();
                // Since updates can be added concurrently with us draining the queue simply setting the value to 0
                // after drained will not be 100% synchronized with the queue contents and could potentially cause a
                // large
                // drift over time. Therefore each update polled from the queue will subtract its size instead.
                updateByteSizeDrained += update != null ? update.roughSizeOfUpdate() : 0;
                if (update != null && update.getEntityId() <= currentlyIndexedNodeId) {
                    updater.process(update);
                    if (printDebug) {
                        log.info("Applied %s from queue", update.describe(tokenNameLookup));
                    }
                } else if (printDebug) {
                    log.info("Skipped %s from queue", update == null ? null : update.describe(tokenNameLookup));
                }
            } while (!concurrentUpdateQueue.isEmpty());
            concurrentUpdateQueueByteSize.addAndGet(-updateByteSizeDrained);
        }
        if (printDebug) {
            log.info("Done applying updates from queue");
        }
    }

    private void forEachPopulation(ThrowingConsumer<IndexPopulation, Exception> action, CursorContext cursorContext) {
        for (IndexPopulation population : populations) {
            try {
                action.accept(population);
            } catch (Throwable failure) {
                cancel(population, failure, cursorContext);
            }
        }
    }

    private PropertyScanConsumer createPropertyScanConsumer() {
        // are we going to populate only token indexes?
        if (populations.stream()
                .allMatch(population ->
                        population.indexProxyStrategy.getIndexDescriptor().getIndexType() == LOOKUP)) {
            return null;
        }

        return new PropertyScanConsumerImpl();
    }

    private TokenScanConsumer createTokenScanConsumer() {
        // is there a token index among the to-be-populated indexes?
        var maybeTokenIdxPopulation = populations.stream()
                .filter(population ->
                        population.indexProxyStrategy.getIndexDescriptor().getIndexType() == LOOKUP)
                .findAny();
        return maybeTokenIdxPopulation.map(TokenScanConsumerImpl::new).orElse(null);
    }

    @Override
    public String toString() {
        String updatesString = populations.stream().map(Object::toString).collect(joining(", ", "[", "]"));

        return "MultipleIndexPopulator{activeTasks=" + activeTasks + ", " + "batchedUpdatesFromScan = " + updatesString
                + ", concurrentUpdateQueue = " + concurrentUpdateQueue.size() + "}";
    }

    IndexDescriptor[] indexDescriptors() {
        return populations.stream()
                .map(p -> p.indexProxyStrategy.getIndexDescriptor())
                .toArray(IndexDescriptor[]::new);
    }

    public static class MultipleIndexUpdater implements IndexUpdater {
        private final Map<SchemaDescriptor, IndexPopulationUpdater> populationsWithUpdaters;
        private final MultipleIndexPopulator multipleIndexPopulator;
        private final InternalLog log;
        private final CursorContext cursorContext;

        MultipleIndexUpdater(
                MultipleIndexPopulator multipleIndexPopulator,
                Map<SchemaDescriptor, IndexPopulationUpdater> populationsWithUpdaters,
                InternalLogProvider logProvider,
                CursorContext cursorContext) {
            this.multipleIndexPopulator = multipleIndexPopulator;
            this.populationsWithUpdaters = populationsWithUpdaters;
            this.log = logProvider.getLog(getClass());
            this.cursorContext = cursorContext;
        }

        @Override
        public void process(IndexEntryUpdate<?> update) {
            IndexPopulationUpdater populationUpdater =
                    populationsWithUpdaters.get(update.indexKey().schema());
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
                    populationsWithUpdaters.remove(update.indexKey().schema());
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

        IndexPopulation(IndexPopulator populator, IndexProxyStrategy indexProxyStrategy, FlippableIndexProxy flipper) {
            this.populator = populator;
            this.indexProxyStrategy = indexProxyStrategy;
            this.flipper = flipper;
        }

        private void cancel(IndexPopulationFailure failure) {
            flipper.flipTo(new FailedIndexProxy(indexProxyStrategy, populator, failure, logProvider));
        }

        void create() throws IOException {
            populatorLock.lock();
            try {
                if (populationOngoing) {
                    populator.create();
                }
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

        void flip(CursorContext cursorContext)
                throws IndexProxyAlreadyClosedKernelException, ExceptionDuringFlipKernelException {
            phaseTracker.enterPhase(PhaseTracker.Phase.FLIP);
            flipper.flip(() -> {
                populatorLock.lock();
                try {
                    if (populationOngoing) {
                        applyExternalUpdates(Long.MAX_VALUE);
                        if (populations.contains(IndexPopulation.this)) {
                            if (indexProxyStrategy.getIndexDescriptor().getIndexType() != IndexType.LOOKUP) {
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

            populator.scanCompleted(phaseTracker, populationWorkScheduler, cursorContext);
        }

        PopulationProgress progress(PopulationProgress storeScanProgress) {
            return populator.progress(storeScanProgress);
        }
    }

    private class PropertyScanConsumerImpl implements PropertyScanConsumer {

        @Override
        public Batch newBatch() {
            return new Batch() {
                final List<EntityUpdates> updates = new ArrayList<>();

                @Override
                public void addRecord(long entityId, int[] tokens, Map<Integer, Value> properties) {
                    var builder = EntityUpdates.forEntity(entityId, true).withTokens(tokens);
                    properties.forEach(builder::added);
                    updates.add(builder.build());
                }

                @Override
                public void process() {
                    try (var cursorContext = contextFactory.create(POPULATION_WORK_FLUSH_TAG)) {
                        addFromScan(updates, cursorContext);
                    }

                    if (printDebug) {
                        if (!updates.isEmpty()) {
                            long lastEntityId = updates.get(updates.size() - 1).getEntityId();
                            log.info(
                                    "Added scan updates for entities %d-%d",
                                    updates.get(0).getEntityId(), lastEntityId);
                        } else {
                            log.info("Added zero scan updates");
                        }
                    }
                }
            };
        }

        private void addFromScan(List<EntityUpdates> entityUpdates, CursorContext cursorContext) {
            // This is called from a full store node scan, meaning that all node properties are included in the
            // EntityUpdates object. Therefore no additional properties need to be loaded.
            Map<IndexPopulation, List<IndexEntryUpdate<IndexPopulation>>> updates = new HashMap<>(populations.size());
            for (EntityUpdates update : entityUpdates) {
                for (IndexEntryUpdate<IndexPopulation> indexUpdate : update.valueUpdatesForIndexKeys(populations)) {
                    IndexPopulation population = indexUpdate.indexKey();
                    population.populator.includeSample(indexUpdate);
                    updates.computeIfAbsent(population, p -> new ArrayList<>()).add(indexUpdate);
                }
            }
            for (Map.Entry<IndexPopulation, List<IndexEntryUpdate<IndexPopulation>>> entry : updates.entrySet()) {
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
                private final List<TokenIndexEntryUpdate<IndexPopulation>> updates = new ArrayList<>();

                @Override
                public void addRecord(long entityId, int[] tokens) {
                    updates.add(IndexEntryUpdate.change(entityId, population, EMPTY_INT_ARRAY, tokens));
                }

                @Override
                public void process() {
                    try {
                        population.populator.add(updates, cursorContext);
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
            String entityType;
            if (nodeScan) {
                entityType = "node";
            } else {
                entityType = "relationship";
            }
            log.debug("Completed " + entityType + " store scan. " + "Flushing all pending updates." + EOL
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
}
