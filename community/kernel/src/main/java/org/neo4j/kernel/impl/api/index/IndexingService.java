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
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.common.Subject.SYSTEM;
import static org.neo4j.internal.helpers.collection.Iterables.asList;
import static org.neo4j.internal.helpers.collection.Iterators.asResourceIterator;
import static org.neo4j.internal.helpers.collection.Iterators.iterator;
import static org.neo4j.internal.kernel.api.InternalIndexState.FAILED;
import static org.neo4j.internal.kernel.api.InternalIndexState.ONLINE;
import static org.neo4j.internal.kernel.api.InternalIndexState.POPULATING;
import static org.neo4j.io.pagecache.PageCacheOpenOptions.MULTI_VERSIONED;
import static org.neo4j.kernel.impl.api.TransactionVisibilityProvider.EMPTY_VISIBILITY_PROVIDER;
import static org.neo4j.kernel.impl.api.index.IndexPopulationFailure.failure;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.api.block.procedure.primitive.LongObjectProcedure;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.set.ImmutableSet;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.neo4j.common.EntityType;
import org.neo4j.common.Subject;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.FulltextSettings;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.exceptions.KernelException;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.Format;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.exceptions.InternalKernelRuntimeException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.FulltextSchemaDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.TransactionVisibilityProvider;
import org.neo4j.kernel.impl.api.index.drop.DefaultIndexDropController;
import org.neo4j.kernel.impl.api.index.drop.IndexDropController;
import org.neo4j.kernel.impl.api.index.drop.MultiVersionIndexDropController;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingController;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.transaction.state.storeview.IndexStoreViewFactory;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.api.ReadableStorageEngine;
import org.neo4j.time.Stopwatch;
import org.neo4j.util.Preconditions;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.storable.Value;

/**
 * Manages neo4j indexes. Each index has an {@link IndexDescriptor}, which it uses to filter
 * changes that come into the database. Changes that apply to the the rule are indexed. This way, "normal" changes to
 * the database can be replayed to perform recovery after a crash.
 * <p>
 * <h3>Recovery procedure</h3>
 * <p>
 * Each index has a state, as defined in {@link InternalIndexState}, which is used during
 * recovery. If an index is anything but {@link InternalIndexState#ONLINE}, it will simply be
 * destroyed and re-created.
 * <p>
 * If, however, it is {@link InternalIndexState#ONLINE}, the index provider is required to
 * also guarantee that the index had been flushed to disk.
 */
public class IndexingService extends LifecycleAdapter implements IndexUpdateListener, IndexingProvidersService {
    private static final String INDEX_SERVICE_INDEX_CLOSING_TAG = "indexServiceIndexClosing";
    private static final String INIT_TAG = "Initialize IndexingService";
    private static final String START_TAG = "Start index population";
    public static final int USAGE_REPORT_FREQUENCY_SECONDS = 10;

    private final IndexSamplingController samplingController;
    private final IndexProxyCreator indexProxyCreator;
    private final IndexProviderMap providerMap;
    private final IndexMapReference indexMapRef;
    private final Iterable<IndexDescriptor> indexDescriptors;
    private final InternalLog internalLog;
    private final IndexStatisticsStore indexStatisticsStore;
    private final CursorContextFactory contextFactory;
    private final MemoryTracker memoryTracker;
    private final String databaseName;
    private final DatabaseReadOnlyChecker readOnlyChecker;
    private final Config config;
    private final ImmutableSet<OpenOption> openOptions;
    private final TokenNameLookup tokenNameLookup;
    private final JobScheduler jobScheduler;
    private final InternalLogProvider internalLogProvider;
    private final IndexMonitor monitor;
    private final SchemaState schemaState;
    private final IndexPopulationJobController populationJobController;
    private final IndexStoreView storeView;
    private final StorageEngineIndexingBehaviour storageEngineIndexingBehaviour;
    private final KernelVersionProvider kernelVersionProvider;
    private final IndexDropController indexDropController;
    private JobHandle<?> eventuallyConsistentFulltextIndexRefreshJob;

    private volatile JobHandle<?> usageReportJob;

    enum State {
        NOT_STARTED,
        STARTING,
        RUNNING,
        STOPPED
    }

    private volatile State state = State.NOT_STARTED;

    IndexingService(
            ReadableStorageEngine storageEngine,
            IndexProxyCreator indexProxyCreator,
            IndexProviderMap providerMap,
            IndexMapReference indexMapRef,
            IndexStoreViewFactory indexStoreViewFactory,
            Iterable<IndexDescriptor> indexDescriptors,
            IndexSamplingController samplingController,
            TokenNameLookup tokenNameLookup,
            JobScheduler scheduler,
            SchemaState schemaState,
            InternalLogProvider internalLogProvider,
            IndexMonitor monitor,
            IndexStatisticsStore indexStatisticsStore,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker,
            String databaseName,
            DatabaseReadOnlyChecker readOnlyChecker,
            Config config,
            KernelVersionProvider kernelVersionProvider,
            FileSystemAbstraction fs,
            TransactionVisibilityProvider transactionVisibilityProvider) {
        this.storageEngineIndexingBehaviour = storageEngine.indexingBehaviour();
        this.indexProxyCreator = indexProxyCreator;
        this.providerMap = providerMap;
        this.indexMapRef = indexMapRef;
        this.indexDescriptors = indexDescriptors;
        this.samplingController = samplingController;
        this.tokenNameLookup = tokenNameLookup;
        this.jobScheduler = scheduler;
        this.schemaState = schemaState;
        this.internalLogProvider = internalLogProvider;
        this.monitor = monitor;
        this.populationJobController = new IndexPopulationJobController(scheduler);
        this.internalLog = internalLogProvider.getLog(getClass());
        this.indexStatisticsStore = indexStatisticsStore;
        this.contextFactory = contextFactory;
        this.memoryTracker = memoryTracker;
        this.databaseName = databaseName;
        this.readOnlyChecker = readOnlyChecker;
        this.config = config;
        this.openOptions = storageEngine.getOpenOptions();
        this.storeView = indexStoreViewFactory.createTokenIndexStoreView(indexMapRef::getIndexProxy);
        this.kernelVersionProvider = kernelVersionProvider;
        this.indexDropController = createIndexDropController(internalLogProvider, transactionVisibilityProvider, fs);
    }

    private IndexDropController createIndexDropController(
            InternalLogProvider internalLogProvider,
            TransactionVisibilityProvider transactionVisibilityProvider,
            FileSystemAbstraction fs) {
        return openOptions.contains(MULTI_VERSIONED) && !EMPTY_VISIBILITY_PROVIDER.equals(transactionVisibilityProvider)
                ? new MultiVersionIndexDropController(
                        jobScheduler, transactionVisibilityProvider, this, fs, internalLogProvider)
                : new DefaultIndexDropController(this);
    }

    /**
     * Called while the database starts up, before recovery.
     */
    @Override
    public void init() throws IOException {
        validateDefaultProviderExisting();

        try (var cursorContext = contextFactory.create(INIT_TAG)) {
            indexMapRef.modify(indexMap -> {
                var stopwatch = Stopwatch.start();
                Map<InternalIndexState, List<IndexLogRecord>> indexStates = new EnumMap<>(InternalIndexState.class);
                for (IndexDescriptor descriptor : indexDescriptors) {
                    IndexProviderDescriptor providerDescriptor = descriptor.getIndexProvider();
                    IndexProvider provider = providerMap.lookup(providerDescriptor);
                    InternalIndexState initialState = provider.getInitialState(descriptor, cursorContext, openOptions);

                    indexStates
                            .computeIfAbsent(initialState, internalIndexState -> new ArrayList<>())
                            .add(new IndexLogRecord(descriptor));

                    internalLog.debug(indexStateInfo("init", initialState, descriptor));
                    IndexProxy indexProxy =
                            switch (initialState) {
                                case ONLINE -> {
                                    monitor.initialState(databaseName, descriptor, ONLINE);
                                    yield indexProxyCreator.createOnlineIndexProxy(descriptor);
                                }
                                case POPULATING -> {
                                    // The database was shut down during population, or a crash has occurred, or some
                                    // other sad thing.
                                    monitor.initialState(databaseName, descriptor, POPULATING);
                                    yield indexProxyCreator.createRecoveringIndexProxy(descriptor);
                                }
                                case FAILED -> {
                                    monitor.initialState(databaseName, descriptor, FAILED);
                                    IndexPopulationFailure failure = failure(
                                            provider.getPopulationFailure(descriptor, cursorContext, openOptions));
                                    yield indexProxyCreator.createFailedIndexProxy(descriptor, failure);
                                }
                            };
                    indexMap.putIndexProxy(indexProxy);
                }
                logIndexStateSummary("init", indexStates, indexMap.size(), stopwatch.elapsed());
                return indexMap;
            });
        }
    }

    private void validateDefaultProviderExisting() {
        if (providerMap == null || providerMap.getDefaultProvider() == null) {
            throw new IllegalStateException("You cannot run the database without an index provider, "
                    + "please make sure that a valid provider (subclass of "
                    + IndexProvider.class.getName() + ") is on your classpath.");
        }
    }

    // Recovery semantics: This is to be called after init, and after the database has run recovery.
    @Override
    public void start() throws Exception {
        state = State.STARTING;
        // Recovery will not do refresh (update read views) while applying recovered transactions and instead
        // do it at one point after recovery... i.e. here
        indexMapRef.indexMapSnapshot().forEachIndexProxy(indexProxyOperation("refresh", IndexProxy::refresh));

        final MutableLongObjectMap<IndexDescriptor> rebuildingDescriptors = new LongObjectHashMap<>();
        indexMapRef.modify(indexMap -> {
            var stopwatch = Stopwatch.start();
            Map<InternalIndexState, List<IndexLogRecord>> indexStates = new EnumMap<>(InternalIndexState.class);

            // Find all indexes that are not already online, do not require rebuilding, and create them
            indexMap.forEachIndexProxy((indexId, proxy) -> {
                InternalIndexState state = proxy.getState();
                IndexDescriptor descriptor = proxy.getDescriptor();
                IndexLogRecord indexLogRecord = new IndexLogRecord(descriptor);
                indexStates
                        .computeIfAbsent(state, internalIndexState -> new ArrayList<>())
                        .add(indexLogRecord);
                internalLog.debug(indexStateInfo("start", state, descriptor));
                switch (state) {
                    case ONLINE, FAILED -> proxy.start();
                    case POPULATING ->
                    // Remember for rebuilding right below in this method
                    rebuildingDescriptors.put(indexId, descriptor);
                    default -> throw new IllegalStateException("Unknown state: " + state);
                }
            });
            logIndexStateSummary("start", indexStates, indexMap.size(), stopwatch.elapsed());

            dontRebuildIndexesInReadOnlyMode(rebuildingDescriptors);
            // Drop placeholder proxies for indexes that need to be rebuilt
            dropRecoveringIndexes(indexMap, rebuildingDescriptors.keySet());
            // Rebuild indexes by recreating and repopulating them
            populateIndexesOfAllTypes(rebuildingDescriptors, indexMap);

            return indexMap;
        });

        samplingController.recoverIndexSamples();
        samplingController.start();

        indexDropController.start();

        // So at this point we've started population of indexes that needs to be rebuilt in the background.
        // Indexes backing uniqueness constraints are normally built within the transaction creating the constraint
        // and so we shouldn't leave such indexes in a populating state after recovery.
        // This is why we now go and wait for those indexes to be fully populated.
        rebuildingDescriptors.forEachKeyValue((indexId, index) -> {
            if (!index.isUnique()) {
                // It's not a uniqueness constraint, so don't wait for it to be rebuilt
                return;
            }

            IndexProxy proxy;
            try {
                proxy = getIndexProxy(index);
            } catch (IndexNotFoundKernelException e) {
                throw new IllegalStateException(
                        "What? This index was seen during recovery just now, why isn't it available now?", e);
            }

            if (proxy.getDescriptor().getOwningConstraintId().isEmpty()) {
                // Even though this is an index backing a uniqueness constraint, the uniqueness constraint wasn't
                // created
                // so there's no gain in waiting for this index.
                return;
            }

            monitor.awaitingPopulationOfRecoveredIndex(index);
            awaitOnlineAfterRecovery(proxy);
        });

        usageReportJob = jobScheduler.scheduleRecurring(
                Group.STORAGE_MAINTENANCE,
                this::reportUsageStatistics,
                USAGE_REPORT_FREQUENCY_SECONDS,
                USAGE_REPORT_FREQUENCY_SECONDS,
                TimeUnit.SECONDS);

        state = State.RUNNING;

        startEventuallyConsistentFulltextIndexRefreshThread();
    }

    /**
     * Ensures all eventually consistent fulltext indexes to be refreshed up to this point.
     */
    public void awaitFulltextIndexRefresh() {
        Duration interval = config.get(FulltextSettings.eventually_consistent_refresh_interval);
        if (!interval.isZero()) {
            for (IndexProxy indexProxy : indexMapRef.getAllIndexProxies()) {
                try {
                    if (indexProxy.getDescriptor().schema().isSchemaDescriptorType(FulltextSchemaDescriptor.class)) {
                        indexProxy.refresh();
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
        // else if the refresh interval is zero then refresh is done by the index update threads.
    }

    private void startEventuallyConsistentFulltextIndexRefreshThread() {
        Duration interval = config.get(FulltextSettings.eventually_consistent_refresh_interval);
        if (!interval.isZero()) {
            eventuallyConsistentFulltextIndexRefreshJob = jobScheduler.scheduleRecurring(
                    Group.STORAGE_MAINTENANCE,
                    this::checkAndScheduleEventuallyConsistentFulltextIndexRefresh,
                    interval.toMillis(),
                    TimeUnit.MILLISECONDS);
        }
    }

    private void checkAndScheduleEventuallyConsistentFulltextIndexRefresh() {
        for (IndexProxy indexProxy : indexMapRef.getAllIndexProxies()) {
            indexProxy.maintenance();
        }
    }

    private void dontRebuildIndexesInReadOnlyMode(MutableLongObjectMap<IndexDescriptor> rebuildingDescriptors) {
        if (readOnlyChecker.isReadOnly() && rebuildingDescriptors.notEmpty()) {
            String indexString = rebuildingDescriptors.values().stream()
                    .map(String::valueOf)
                    .collect(Collectors.joining(", ", "{", "}"));
            throw new IllegalStateException(
                    "Some indexes need to be rebuilt. This is not allowed in read only mode. Please start db in writable mode to rebuild indexes. "
                            + "Indexes needing rebuild: " + indexString);
        }
    }

    private void populateIndexesOfAllTypes(
            MutableLongObjectMap<IndexDescriptor> rebuildingDescriptors, IndexMap indexMap) {
        Map<IndexPopulationCategory, MutableLongObjectMap<IndexDescriptor>> rebuildingDescriptorsByType =
                new HashMap<>();
        for (var descriptor : rebuildingDescriptors) {
            var category = new IndexPopulationCategory(descriptor, storageEngineIndexingBehaviour);
            rebuildingDescriptorsByType
                    .computeIfAbsent(category, type -> new LongObjectHashMap<>())
                    .put(descriptor.getId(), descriptor);
        }

        for (var descriptorToPopulate : rebuildingDescriptorsByType.entrySet()) {
            var populationJob =
                    newIndexPopulationJob(descriptorToPopulate.getKey().entityType(), SYSTEM);
            populate(descriptorToPopulate.getValue(), indexMap, populationJob);
        }
    }

    private void populate(
            MutableLongObjectMap<IndexDescriptor> rebuildingDescriptors,
            IndexMap indexMap,
            IndexPopulationJob populationJob) {
        rebuildingDescriptors.forEachKeyValue((indexId, descriptor) -> {
            IndexProxy proxy = indexProxyCreator.createPopulatingIndexProxy(descriptor, monitor, populationJob);
            proxy.start();
            indexMap.putIndexProxy(proxy);
        });
        try (var cursorContext = contextFactory.create(START_TAG)) {
            startIndexPopulation(populationJob, cursorContext);
        }
    }

    /**
     * Polls the {@link IndexProxy#getState() state of the index} and waits for it to be either {@link InternalIndexState#ONLINE},
     * in which case the wait is over, or {@link InternalIndexState#FAILED}, in which an exception is logged.
     * <p>
     * This method is only called during startup, and might be called as part of recovery. If we threw an exception here, it could
     * render the database unrecoverable. That's why we only log a message about failed indexes.
     */
    private void awaitOnlineAfterRecovery(IndexProxy proxy) {
        while (true) {
            switch (proxy.getState()) {
                case ONLINE:
                    return;
                case FAILED:
                    String message = String.format(
                            "Index %s entered %s state while recovery waited for it to be fully populated.",
                            proxy.getDescriptor(), FAILED);
                    IndexPopulationFailure populationFailure = proxy.getPopulationFailure();
                    String causeOfFailure = populationFailure.asString();
                    // Log as INFO because at this point we don't know if the constraint index was ever bound to a
                    // constraint or not.
                    // If it was really bound to a constraint, then we actually ought to log as WARN or ERROR, I
                    // suppose.
                    // But by far the most likely scenario is that the constraint itself was never created.
                    internalLog.info(IndexPopulationFailure.appendCauseOfFailure(message, causeOfFailure));
                    return;
                case POPULATING:
                    // Sleep a short while and look at state again the next loop iteration
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        throw new IllegalStateException("Waiting for index to become ONLINE was interrupted", e);
                    }
                    break;
                default:
                    throw new IllegalStateException(proxy.getState().name());
            }
        }
    }

    // while indexes will be closed on shutdown we need to stop ongoing jobs before we will start shutdown to prevent
    // races between checkpoint flush and index jobs
    @Override
    public void stop() throws Exception {
        if (eventuallyConsistentFulltextIndexRefreshJob != null) {
            eventuallyConsistentFulltextIndexRefreshJob.cancel();
        }

        usageReportJob.cancel();
        indexDropController.stop();
        samplingController.stop();
        populationJobController.stop();
    }

    // We need to stop indexing service on shutdown since we can have transactions that are ongoing/finishing
    // after we start stopping components and those transactions should be able to finish successfully
    @Override
    public void shutdown() throws IOException {
        state = State.STOPPED;
        closeAllIndexes();
    }

    @Override
    public void validateBeforeCommit(IndexDescriptor index, Value[] tuple, long entityId) {
        indexMapRef.validateBeforeCommit(index, tuple, entityId);
    }

    @Override
    public void validateIndexPrototype(IndexPrototype prototype) {
        IndexProvider provider = providerMap.lookup(prototype.getIndexProvider());
        provider.validatePrototype(prototype);
    }

    @Override
    public IndexProviderDescriptor getDefaultProvider() {
        return providerMap.getDefaultProvider().getProviderDescriptor();
    }

    @Override
    public IndexProviderDescriptor getFulltextProvider() {
        return providerMap.getFulltextProvider().getProviderDescriptor();
    }

    @Override
    public Collection<IndexProvider> getIndexProviders() {
        var indexProviders = new ArrayList<IndexProvider>();
        providerMap.accept(indexProviders::add);
        return indexProviders;
    }

    @Override
    public IndexProviderDescriptor getTokenIndexProvider() {
        return providerMap.getTokenIndexProvider().getProviderDescriptor();
    }

    @Override
    public IndexProviderDescriptor getTextIndexProvider() {
        return providerMap.getTextIndexProvider().getProviderDescriptor();
    }

    @Override
    public IndexProviderDescriptor getPointIndexProvider() {
        return providerMap.getPointIndexProvider().getProviderDescriptor();
    }

    @Override
    public IndexProviderDescriptor getVectorIndexProvider() {
        return providerMap.getVectorIndexProvider().getProviderDescriptor();
    }

    @Override
    public IndexProvider getIndexProvider(IndexProviderDescriptor descriptor) {
        return providerMap.lookup(descriptor);
    }

    @Override
    public IndexDescriptor completeConfiguration(IndexDescriptor index) {
        return providerMap.completeConfiguration(index, storageEngineIndexingBehaviour);
    }

    @Override
    public IndexProviderDescriptor indexProviderByName(String providerName) {
        return providerMap.lookup(providerName).getProviderDescriptor();
    }

    @Override
    public IndexType indexTypeByProviderName(String providerName) {
        return providerMap.lookup(providerName).getIndexType();
    }

    @Override
    public List<IndexProviderDescriptor> indexProvidersByType(IndexType indexType) {
        return providerMap.lookup(indexType).stream()
                .map(IndexProvider::getProviderDescriptor)
                .toList();
    }

    /**
     * Applies the given updates, which may contain updates for one or more indexes.
     *
     * @param updates {@link IndexEntryUpdate updates} to apply.
     * @throws UncheckedIOException potentially thrown from index updating.
     * @throws KernelException potentially thrown from index updating.
     */
    @Override
    public void applyUpdates(
            Iterable<IndexEntryUpdate<IndexDescriptor>> updates, CursorContext cursorContext, boolean parallel)
            throws KernelException {
        if (state == State.NOT_STARTED) {
            // We're in recovery, which means we'll be telling indexes to apply with additional care for making
            // idempotent changes.
            apply(updates, IndexUpdateMode.RECOVERY, cursorContext, parallel);
        } else if (state == State.RUNNING || state == State.STARTING) {
            apply(updates, IndexUpdateMode.ONLINE, cursorContext, parallel);
        } else {
            throw new IllegalStateException(
                    "Can't apply index updates " + asList(updates) + " while indexing service is " + state);
        }
    }

    private void apply(
            Iterable<IndexEntryUpdate<IndexDescriptor>> updates,
            IndexUpdateMode updateMode,
            CursorContext cursorContext,
            boolean parallel)
            throws KernelException {
        try (IndexUpdaterMap updaterMap = indexMapRef.createIndexUpdaterMap(updateMode, parallel)) {
            for (IndexEntryUpdate<IndexDescriptor> indexUpdate : updates) {
                processUpdate(updaterMap, indexUpdate, cursorContext);
            }
        }
    }

    /**
     * Creates one or more indexes. They will all be populated by one and the same store scan.
     * <p>
     * This code is called from the transaction infrastructure during transaction commits, which means that
     * it is *vital* that it is stable, and handles errors very well. Failing here means that the entire db
     * will shut down.
     *
     * @param subject subject that triggered the index creation.
     * This is used for monitoring purposes, so work related to index creation and population can be linked to its originator.
     */
    @Override
    public void createIndexes(Subject subject, IndexDescriptor... rules) {
        IndexPopulationStarter populationStarter = new IndexPopulationStarter(subject, rules);
        indexMapRef.modify(populationStarter);
        populationStarter.startPopulation();
    }

    private static void processUpdate(
            IndexUpdaterMap updaterMap, IndexEntryUpdate<IndexDescriptor> indexUpdate, CursorContext cursorContext)
            throws IndexEntryConflictException {
        IndexUpdater updater = updaterMap.getUpdater(indexUpdate.indexKey(), cursorContext);
        if (updater != null) {
            updater.process(indexUpdate);
        }
    }

    @Override
    public void dropIndex(IndexDescriptor indexDescriptor) {
        indexDropController.dropIndex(indexDescriptor);
    }

    public void internalIndexDrop(IndexDescriptor indexDescriptor) {
        Preconditions.checkState(
                state == State.RUNNING || state == State.NOT_STARTED,
                "Dropping index in unexpected state %s",
                state.name());
        indexMapRef.modify(indexMap -> {
            long indexId = indexDescriptor.getId();
            IndexProxy index = indexMap.removeIndexProxy(indexId);

            if (state == State.RUNNING) {
                Preconditions.checkState(index != null, "Index %s doesn't exists", indexDescriptor);
                index.drop();
            } else if (index != null) {
                try {
                    index.drop();
                } catch (Exception e) {
                    // This is OK to get during recovery because the underlying index can be in any unknown state
                    // while we're recovering. Let's just move on to closing it instead.
                    try (var cursorContext = contextFactory.create(INDEX_SERVICE_INDEX_CLOSING_TAG)) {
                        index.close(cursorContext);
                    } catch (IOException closeException) {
                        // This is OK for the same reason as above
                    }
                }
            }
            return indexMap;
        });
    }

    @VisibleForTesting
    public void forceIndexState(String indexName, InternalIndexState state) {
        indexMapRef.modify(indexMap -> {
            var descriptor = indexMap.getAllIndexProxies().stream()
                    .map(IndexProxy::getDescriptor)
                    .filter(id -> indexName.equals(id.getName()))
                    .findFirst()
                    .orElseThrow();
            indexMap.removeIndexProxy(descriptor.getId());
            indexMap.putIndexProxy(
                    switch (state) {
                        case ONLINE -> indexProxyCreator.createOnlineIndexProxy(descriptor);
                        case POPULATING -> indexProxyCreator.createPopulatingIndexProxy(
                                descriptor,
                                IndexMonitor.NO_MONITOR,
                                newIndexPopulationJob(descriptor.schema().entityType(), SYSTEM));
                        case FAILED -> indexProxyCreator.createFailedIndexProxy(
                                descriptor, failure("test forced failure"));
                    });
            return indexMap;
        });
        schemaState.clear();
    }

    public void triggerIndexSampling(IndexSamplingMode mode) {
        internalLog.info("Manual trigger for sampling all indexes [" + mode + "]");
        monitor.indexSamplingTriggered(mode);
        samplingController.sampleIndexes(mode);
    }

    public void triggerIndexSampling(IndexDescriptor index, IndexSamplingMode mode) {
        String description = index.userDescription(tokenNameLookup);
        internalLog.info("Manual trigger for sampling index " + description + " [" + mode + "]");
        samplingController.sampleIndex(index.getId(), mode);
    }

    private static void dropRecoveringIndexes(IndexMap indexMap, LongIterable indexesToRebuild) {
        indexesToRebuild.forEach(idx -> {
            IndexProxy indexProxy = indexMap.removeIndexProxy(idx);
            assert indexProxy != null;
            indexProxy.drop();
        });
    }

    @Override
    public void activateIndex(IndexDescriptor descriptor)
            throws IndexNotFoundKernelException, IndexPopulationFailedKernelException {
        try {
            if (state == State.RUNNING) // don't do this during recovery.
            {
                IndexProxy index = getIndexProxy(descriptor);
                index.awaitStoreScanCompleted(0, TimeUnit.MILLISECONDS);
                index.activate();
                internalLog.info("Constraint %s is %s.", index.getDescriptor(), ONLINE.name());
            }
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new InternalKernelRuntimeException(e, "Unable to activate index, thread was interrupted.");
        }
    }

    public IndexProxy getIndexProxy(IndexDescriptor index) throws IndexNotFoundKernelException {
        return indexMapRef.getIndexProxy(index);
    }

    public Iterable<IndexProxy> getIndexProxies() {
        return indexMapRef.getAllIndexProxies();
    }

    public void checkpoint(DatabaseFlushEvent flushEvent, CursorContext cursorContext) throws IOException {
        try (var fileFlushEvent = flushEvent.beginFileFlush()) {
            internalLog.debug(
                    "Checkpointing %s", indexStatisticsStore.storeFile().getFileName());
            indexStatisticsStore.checkpoint(fileFlushEvent, cursorContext);
        }
        indexMapRef.indexMapSnapshot().forEachIndexProxy(indexProxyOperation("force", proxy -> {
            internalLog.debug("Checkpointing %s", proxy.getDescriptor().userDescription(tokenNameLookup));
            try (var fileFlushEvent = flushEvent.beginFileFlush()) {
                proxy.force(fileFlushEvent, cursorContext);
            }
        }));
    }

    private LongObjectProcedure<IndexProxy> indexProxyOperation(
            String name, ThrowingConsumer<IndexProxy, Exception> operation) {
        return (id, indexProxy) -> {
            try {
                operation.accept(indexProxy);
            } catch (Exception e) {
                try {
                    if (indexMapRef.getIndexProxy(indexProxy.getDescriptor()) != null) {
                        throw new UnderlyingStorageException("Unable to " + name + " " + indexProxy, e);
                    }
                } catch (IndexNotFoundKernelException infe) {
                    // index was dropped while trying to operate on it, we can continue to
                }
            }
        };
    }

    private void closeAllIndexes() {
        try (var cursorContext = contextFactory.create(INDEX_SERVICE_INDEX_CLOSING_TAG)) {
            indexMapRef.modify(indexMap -> {
                Iterable<IndexProxy> indexesToStop = indexMap.getAllIndexProxies();
                for (IndexProxy index : indexesToStop) {
                    try {
                        index.close(cursorContext);
                    } catch (Exception e) {
                        internalLog.error("Unable to close index", e);
                    }
                }
                // Effectively clearing it
                return new IndexMap();
            });
        }
    }

    public ResourceIterator<Path> snapshotIndexFiles() throws IOException {
        Collection<ResourceIterator<Path>> snapshots = new ArrayList<>();
        snapshots.add(asResourceIterator(iterator(indexStatisticsStore.storeFile())));
        for (IndexProxy indexProxy : indexMapRef.getAllIndexProxies()) {
            snapshots.add(indexProxy.snapshotFiles());
        }
        return Iterators.concatResourceIterators(snapshots.iterator());
    }

    public IndexMonitor getMonitor() {
        return monitor;
    }

    private IndexPopulationJob newIndexPopulationJob(EntityType type, Subject subject) {
        MultipleIndexPopulator multiPopulator = new MultipleIndexPopulator(
                storeView,
                internalLogProvider,
                type,
                schemaState,
                jobScheduler,
                tokenNameLookup,
                contextFactory,
                memoryTracker,
                databaseName,
                subject,
                config);
        return new IndexPopulationJob(
                multiPopulator, monitor, contextFactory, memoryTracker, databaseName, subject, NODE, config);
    }

    private void startIndexPopulation(IndexPopulationJob job, CursorContext cursorContext) {
        if (storeView.isEmpty(cursorContext)) {
            // Creating indexes and constraints on an empty database, before ingesting data doesn't need to do
            // unnecessary scheduling juggling,
            // instead just run it on the caller thread.
            job.run();
        } else {
            populationJobController.startIndexPopulation(job);
        }
    }

    private String indexStateInfo(String tag, InternalIndexState state, IndexDescriptor descriptor) {
        return format(
                "IndexingService.%s: index %d on %s is %s",
                tag, descriptor.getId(), descriptor.schema().userDescription(tokenNameLookup), state.name());
    }

    @SuppressWarnings("DurationToLongTimeUnit")
    private void logIndexStateSummary(
            String method,
            Map<InternalIndexState, List<IndexLogRecord>> indexStates,
            int totalIndexes,
            Duration elapsed) {
        if (indexStates.isEmpty()) {
            return;
        }
        int mostPopularStateCount = Integer.MIN_VALUE;
        InternalIndexState mostPopularState = null;
        for (Map.Entry<InternalIndexState, List<IndexLogRecord>> indexStateEntry : indexStates.entrySet()) {
            if (indexStateEntry.getValue().size() > mostPopularStateCount) {
                mostPopularState = indexStateEntry.getKey();
                mostPopularStateCount = indexStateEntry.getValue().size();
            }
        }
        indexStates.remove(mostPopularState);
        for (Map.Entry<InternalIndexState, List<IndexLogRecord>> indexStateEntry : indexStates.entrySet()) {
            InternalIndexState state = indexStateEntry.getKey();
            List<IndexLogRecord> logRecords = indexStateEntry.getValue();
            for (IndexLogRecord logRecord : logRecords) {
                internalLog.info(indexStateInfo(method, state, logRecord.descriptor()));
            }
        }
        internalLog.info(format(
                "IndexingService.%s: indexes not specifically mentioned above are %s. Total %d indexes. Processed in %s",
                method,
                mostPopularState,
                totalIndexes,
                Format.duration(elapsed.toMillis(), TimeUnit.HOURS, TimeUnit.MILLISECONDS)));
    }

    @VisibleForTesting
    public void reportUsageStatistics() {
        if (kernelVersionProvider.kernelVersion().isAtLeast(KernelVersion.VERSION_INDEX_USAGE_STATISTICS_INTRODUCED)) {
            indexMapRef.getAllIndexProxies().forEach(p -> p.reportUsageStatistics(indexStatisticsStore));
        }
    }

    private final class IndexPopulationStarter implements UnaryOperator<IndexMap> {
        private final Subject subject;
        private final IndexDescriptor[] descriptors;
        private final Map<IndexPopulationCategory, IndexPopulationJob> populationJobs = new HashMap<>();

        IndexPopulationStarter(Subject subject, IndexDescriptor[] descriptors) {
            this.subject = subject;
            this.descriptors = descriptors;
        }

        @Override
        public IndexMap apply(IndexMap indexMap) {
            for (IndexDescriptor descriptor : descriptors) {
                IndexProxy index = indexMap.getIndexProxy(descriptor);
                if (index != null && state == State.NOT_STARTED) {
                    // This index already has a proxy. No need to build another.
                    continue;
                }

                final var completeDescriptor = completeConfiguration(descriptor);
                if (state == State.RUNNING) {
                    var populationJob = populationJobs.computeIfAbsent(
                            new IndexPopulationCategory(completeDescriptor, storageEngineIndexingBehaviour),
                            category -> newIndexPopulationJob(
                                    completeDescriptor.schema().entityType(), subject));
                    index = indexProxyCreator.createPopulatingIndexProxy(completeDescriptor, monitor, populationJob);
                    index.start();
                } else {
                    index = indexProxyCreator.createRecoveringIndexProxy(completeDescriptor);
                }

                indexMap.putIndexProxy(index);
            }
            return indexMap;
        }

        void startPopulation() {
            try (var cursorContext = contextFactory.create(START_TAG)) {
                populationJobs.keySet().stream()
                        // Sort these categories so that relationship lookup index will be created last.
                        // This avoids a locking issue when creating lookup indexes and other indexes in the same
                        // transaction.
                        .sorted((o1, o2) -> Boolean.compare(o1.lookupIndexDifferentiator, o2.lookupIndexDifferentiator))
                        .forEach(category -> startIndexPopulation(populationJobs.get(category), cursorContext));
            }
        }
    }

    private record IndexLogRecord(IndexDescriptor descriptor) {}

    @FunctionalInterface
    public interface IndexProxyProvider {
        IndexProxy getIndexProxy(IndexDescriptor indexDescriptor) throws IndexNotFoundKernelException;
    }

    /**
     * Category key to use when splitting up indexes to be populated, so that multiple indexes of a particular category can be
     * populated using the same scan.
     *
     * @param entityType type of entity (node/relateionship) for indexes in this category.
     * @param lookupIndexDifferentiator whether the category is for {@link IndexType#LOOKUP lookup index} and the
     * database's {@link StorageEngineIndexingBehaviour} hints that such indexes needs to be populated by a specific scan,
     * i.e. requiring its own category (which translates to its own population job).
     */
    private record IndexPopulationCategory(EntityType entityType, boolean lookupIndexDifferentiator) {
        IndexPopulationCategory(IndexDescriptor descriptor, StorageEngineIndexingBehaviour indexingBehaviour) {
            this(
                    descriptor.schema().entityType(),
                    descriptor.schema().entityType() == RELATIONSHIP
                            && indexingBehaviour.useNodeIdsInRelationshipTokenIndex()
                            && descriptor.isTokenIndex());
        }
    }
}
