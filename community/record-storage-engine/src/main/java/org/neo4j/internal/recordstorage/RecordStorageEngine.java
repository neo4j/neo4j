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
package org.neo4j.internal.recordstorage;

import static java.util.Collections.emptyList;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.counts_store_max_cached_entries;
import static org.neo4j.function.ThrowingAction.executeAll;
import static org.neo4j.internal.recordstorage.RecordStorageCommandHandling.handleRecordStorageCommands;
import static org.neo4j.internal.recordstorage.RecordStorageEngineFactory.ID;
import static org.neo4j.internal.recordstorage.RecordStorageEngineFactory.NAME;
import static org.neo4j.lock.LockService.NO_LOCK_SERVICE;
import static org.neo4j.storageengine.api.TransactionApplicationMode.MVCC_INCOMPLETE_REVERSE_RECOVERY;
import static org.neo4j.storageengine.api.TransactionApplicationMode.RECOVERY;
import static org.neo4j.util.Preconditions.checkState;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.eclipse.collections.api.set.ImmutableSet;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.collection.diffset.LongDiffSets;
import org.neo4j.collection.trackable.HeapTrackingCollections;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.counts.CountsStore;
import org.neo4j.counts.CountsUpdater;
import org.neo4j.exceptions.KernelException;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.batchimport.cache.NumberArrayFactories;
import org.neo4j.internal.batchimport.cache.NumberArrayFactory;
import org.neo4j.internal.counts.CountsBuilder;
import org.neo4j.internal.counts.CountsStoreProvider;
import org.neo4j.internal.counts.DegreesRebuildFromStore;
import org.neo4j.internal.counts.GBPTreeGenericCountsStore;
import org.neo4j.internal.counts.GBPTreeRelationshipGroupDegreesStore;
import org.neo4j.internal.counts.RelationshipGroupDegreesStore;
import org.neo4j.internal.diagnostics.DiagnosticsLogger;
import org.neo4j.internal.diagnostics.DiagnosticsManager;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.kernel.api.exceptions.TransactionApplyKernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.internal.recordstorage.Command.RecordEnrichmentCommand;
import org.neo4j.internal.recordstorage.NeoStoresDiagnostics.NeoStoreIdUsage;
import org.neo4j.internal.recordstorage.NeoStoresDiagnostics.NeoStoreRecords;
import org.neo4j.internal.recordstorage.TransactionAppliersDispatcherFactory.IdUpdateListenerFactory;
import org.neo4j.internal.schema.IndexConfigCompleter;
import org.neo4j.internal.schema.SchemaCache;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.async.AsyncBlockAccessor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseFile;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.OutOfDiskSpaceException;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.impl.muninn.VersionStorage;
import org.neo4j.io.pagecache.prefetch.PagePrefetcher;
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.DatabaseCreationOptions;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.store.CountsComputer;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.stats.RecordDatabaseEntityCounters;
import org.neo4j.kernel.impl.store.stats.StoreEntityCounters;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.lock.LockService;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.ConstraintRuleAccessor;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.api.LogMetadataProvider;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageEngineCharacteristics;
import org.neo4j.storageengine.api.StorageEngineTransaction;
import org.neo4j.storageengine.api.StorageFileSelection;
import org.neo4j.storageengine.api.StorageLocks;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.api.enrichment.Enrichment;
import org.neo4j.storageengine.api.enrichment.EnrichmentCommand;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TransactionCountingStateVisitor;
import org.neo4j.storageengine.api.txstate.TransactionStateBehaviour;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.storageengine.api.txstate.TxStateVisitor.Decorator;
import org.neo4j.storageengine.api.txstate.validation.TransactionValidatorFactory;
import org.neo4j.storageengine.util.IdGeneratorUpdatesWorkSync;
import org.neo4j.storageengine.util.IdUpdateListener;
import org.neo4j.storageengine.util.IndexUpdatesWorkSync;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokensLoader;
import org.neo4j.util.VisibleForTesting;

public class RecordStorageEngine implements StorageEngine, Lifecycle {
    private static final String STORAGE_ENGINE_START_TAG = "storageEngineStart";
    private static final String SCHEMA_CACHE_START_TAG = "schemaCacheStart";
    private static final String TOKENS_INIT_TAG = "tokensInitialisation";

    private final NeoStores neoStores;
    private final RecordDatabaseLayout databaseLayout;
    private final Config config;
    private final InternalLogProvider internalLogProvider;
    private final TokenHolders tokenHolders;
    private final DatabaseHealth databaseHealth;
    private final SchemaCache schemaCache;
    private final CacheAccessBackDoor cacheAccess;
    private final SchemaState schemaState;
    private final SchemaRuleAccess schemaRuleAccess;
    private final ConstraintRuleAccessor constraintSemantics;
    private final LockService lockService;
    private final boolean consistencyCheckApply;
    private final boolean parallelIndexUpdatesApply;
    private final InternalLog log;
    private final PagePrefetcher pagePrefetcher;
    private final String format;
    private IndexUpdatesWorkSync indexUpdatesSync;
    private final IdGeneratorFactory idGeneratorFactory;
    private final LogMetadataProvider logMetadataProvider;
    private final CursorContextFactory contextFactory;
    private final MemoryTracker otherMemoryTracker;
    private final LockVerificationFactory lockVerificationFactory;
    private final CountsStore countsStore;
    private final RelationshipGroupDegreesStore groupDegreesStore;
    private final int denseNodeThreshold;
    private final IdGeneratorUpdatesWorkSync idGeneratorWorkSyncs;
    private final Map<TransactionApplicationMode, TransactionAppliersDispatcherFactory> applierDispatchers =
            new EnumMap<>(TransactionApplicationMode.class);
    private final RecordDatabaseEntityCounters storeEntityCounters;
    private final RecordStorageIndexingBehaviour indexingBehaviour;
    private final RecordStorageCharacteristics characteristics;
    private final TransactionStateBehaviour txStateBehaviour;

    // installed later
    private IndexUpdateListener indexUpdateListener;
    private volatile boolean closed;

    public RecordStorageEngine(
            RecordDatabaseLayout databaseLayout,
            Config config,
            PageCache pageCache,
            FileSystemAbstraction fs,
            InternalLogProvider internalLogProvider,
            InternalLogProvider userLogProvider,
            TokenHolders tokenHolders,
            SchemaState schemaState,
            ConstraintRuleAccessor constraintSemantics,
            IndexConfigCompleter indexConfigCompleter,
            LockService lockService,
            DatabaseHealth databaseHealth,
            IdGeneratorFactory idGeneratorFactory,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            MemoryTracker otherMemoryTracker,
            LogMetadataProvider logMetadataProvider,
            CursorContextFactory contextFactory,
            PageCacheTracer pageCacheTracer,
            VersionStorage versionStorage,
            PagePrefetcher pagePrefetcher,
            DatabaseCreationOptions databaseCreationOptions) {
        this.databaseLayout = databaseLayout;
        this.config = config;
        this.internalLogProvider = internalLogProvider;
        this.log = internalLogProvider.getLog(getClass());
        this.tokenHolders = tokenHolders;
        this.schemaState = schemaState;
        this.lockService = lockService;
        this.databaseHealth = databaseHealth;
        this.constraintSemantics = constraintSemantics;
        this.idGeneratorFactory = idGeneratorFactory;
        this.logMetadataProvider = logMetadataProvider;
        this.contextFactory = contextFactory;
        this.otherMemoryTracker = otherMemoryTracker;
        this.pagePrefetcher = pagePrefetcher;
        this.neoStores = new StoreFactory(
                        databaseLayout,
                        config,
                        idGeneratorFactory,
                        pageCache,
                        pageCacheTracer,
                        fs,
                        internalLogProvider,
                        contextFactory,
                        false,
                        databaseCreationOptions)
                .openAllNeoStores();
        this.format = this.neoStores.getRecordFormats().name();
        this.lockVerificationFactory = LockVerificationFactory.select(config);
        this.idGeneratorWorkSyncs = new IdGeneratorUpdatesWorkSync(false);
        Stream.of(RecordIdType.values()).forEach(idType -> idGeneratorWorkSyncs.add(idGeneratorFactory.get(idType)));

        this.indexingBehaviour = new RecordStorageIndexingBehaviour(
                neoStores.getNodeStore().getRecordsPerPage(),
                neoStores.getRelationshipStore().getRecordsPerPage());
        this.characteristics = new RecordStorageCharacteristics();
        txStateBehaviour = new RecordTransactionStateBehaviour();
        try {
            schemaRuleAccess = SchemaRuleAccess.getSchemaRuleAccess(neoStores.getSchemaStore(), tokenHolders);
            schemaCache = new SchemaCache(constraintSemantics, indexConfigCompleter, indexingBehaviour);

            cacheAccess = new BridgingCacheAccess(schemaCache, schemaState, tokenHolders);

            denseNodeThreshold = config.get(GraphDatabaseSettings.dense_node_threshold);

            countsStore = openCountsStore(
                    pageCache,
                    fs,
                    databaseLayout,
                    internalLogProvider,
                    userLogProvider,
                    recoveryCleanupWorkCollector,
                    config,
                    contextFactory,
                    pageCacheTracer,
                    versionStorage);

            groupDegreesStore = openDegreesStore(
                    pageCache,
                    fs,
                    databaseLayout,
                    internalLogProvider,
                    userLogProvider,
                    recoveryCleanupWorkCollector,
                    config,
                    contextFactory,
                    pageCacheTracer);

            consistencyCheckApply = config.get(GraphDatabaseInternalSettings.consistency_check_on_apply);
            storeEntityCounters = new RecordDatabaseEntityCounters(idGeneratorFactory, countsStore, schemaCache);
            parallelIndexUpdatesApply = config.get(GraphDatabaseInternalSettings.parallel_index_updates_apply);
        } catch (Throwable failure) {
            neoStores.close();
            throw failure;
        }
    }

    private TransactionAppliersDispatcherFactory buildRegularAppliersDispatcherFactory(
            TransactionApplicationMode mode, KernelVersionTransactionApplierFactory kernelVersionApplierFactory) {
        var appliers = new ArrayList<TransactionApplierFactory>();
        if (consistencyCheckApply && mode.needsAuxiliaryStores()) {
            appliers.add(new ConsistencyCheckingApplierFactory(neoStores));
        }
        appliers.add(kernelVersionApplierFactory);
        appliers.add(new LockGuardedNeoStoreTransactionApplierFactory(mode, neoStores, cacheAccess, lockService(mode)));
        if (mode.needsHighIdTracking()) {
            appliers.add(new HighIdTransactionApplierFactory(neoStores));
        }
        if (mode.needsAuxiliaryStores()) {
            appliers.add(new CountsStoreTransactionApplierFactory(countsStore, groupDegreesStore));
            appliers.add(new IndexTransactionApplierFactory(mode, indexUpdateListener));
        }
        return new TransactionAppliersDispatcherFactory(
                idUpdateListenerFunction(mode), appliers.toArray(TransactionApplierFactory[]::new));
    }

    private static IdUpdateListenerFactory idUpdateListenerFunction(TransactionApplicationMode mode) {
        return mode.isReverseStep()
                ? (w, c) -> IdUpdateListener.IGNORE
                : (workSync, cursorContext) ->
                        workSync.newBatch(cursorContext, mode == MVCC_INCOMPLETE_REVERSE_RECOVERY);
    }

    private CountsStore openCountsStore(
            PageCache pageCache,
            FileSystemAbstraction fs,
            RecordDatabaseLayout layout,
            InternalLogProvider internalLogProvider,
            InternalLogProvider userLogProvider,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            Config config,
            CursorContextFactory contextFactory,
            PageCacheTracer pageCacheTracer,
            VersionStorage versionStorage) {
        return CountsStoreProvider.getInstance()
                .openCountsStore(
                        pageCache,
                        fs,
                        layout,
                        userLogProvider,
                        recoveryCleanupWorkCollector,
                        config,
                        contextFactory,
                        pageCacheTracer,
                        getOpenOptions(),
                        new RecordCountsBuilder(internalLogProvider, fs, contextFactory, layout, logMetadataProvider),
                        false,
                        versionStorage);
    }

    private RelationshipGroupDegreesStore openDegreesStore(
            PageCache pageCache,
            FileSystemAbstraction fs,
            RecordDatabaseLayout layout,
            InternalLogProvider internalLogProvider,
            InternalLogProvider userLogProvider,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            Config config,
            CursorContextFactory contextFactory,
            PageCacheTracer pageCacheTracer) {
        try {
            return new GBPTreeRelationshipGroupDegreesStore(
                    pageCache,
                    layout.relationshipGroupDegreesStore(),
                    fs,
                    recoveryCleanupWorkCollector,
                    new DegreesRebuildFromStore(
                            neoStores,
                            databaseLayout,
                            logMetadataProvider,
                            contextFactory,
                            internalLogProvider,
                            Configuration.DEFAULT),
                    false,
                    GBPTreeGenericCountsStore.NO_MONITOR,
                    layout.getDatabaseName(),
                    config.get(counts_store_max_cached_entries),
                    userLogProvider,
                    contextFactory,
                    pageCacheTracer,
                    getOpenOptions());
        } catch (IOException e) {
            throw new UnderlyingStorageException(e);
        }
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public byte id() {
        return ID;
    }

    @Override
    public RecordStorageReader newReader() {
        return new RecordStorageReader(tokenHolders, neoStores, countsStore, groupDegreesStore, schemaCache);
    }

    @Override
    public RecordStorageCommandCreationContext newCommandCreationContext(boolean multiVersioned) {
        return new RecordStorageCommandCreationContext(
                neoStores, tokenHolders, internalLogProvider, denseNodeThreshold, config, format);
    }

    @Override
    public TransactionValidatorFactory createTransactionValidatorFactory(Config config) {
        return TransactionValidatorFactory.EMPTY_VALIDATOR_FACTORY;
    }

    @Override
    public StoreCursors createStorageCursors(CursorContext cursorContext) {
        return new CachedStoreCursors(neoStores, cursorContext);
    }

    @Override
    public StorageLocks createStorageLocks(ResourceLocker locker) {
        return new RecordStorageLocks(locker);
    }

    @Override
    public void addIndexUpdateListener(IndexUpdateListener listener) {
        checkState(
                this.indexUpdateListener == null,
                "Only supports a single listener. Tried to add " + listener + ", but " + this.indexUpdateListener
                        + " has already been added");
        this.indexUpdateListener = listener;
        this.indexUpdatesSync = new IndexUpdatesWorkSync(listener, parallelIndexUpdatesApply);
    }

    /**
     * @throws TransactionFailureException      if command generation fails or some prerequisite of some command didn't validate,
     *                                          for example if trying to delete a node that still has relationships.
     * @throws CreateConstraintFailureException if this transaction was set to create a constraint and that failed.
     * @throws ConstraintValidationException    if this transaction was set to create a constraint and some data violates that constraint.
     */
    @Override
    public List<StorageCommand> createCommands(
            ReadableTransactionState txState,
            StorageReader storageReader,
            CommandCreationContext commandCreationContext,
            LockTracer lockTracer,
            Decorator additionalTxStateVisitor,
            CursorContext cursorContext,
            StoreCursors storeCursors,
            MemoryTracker memoryTracker)
            throws KernelException {
        if (txState == null) {
            return emptyList();
        }
        var commands = HeapTrackingCollections.<StorageCommand>newArrayList(memoryTracker);

        // We can make this cast here because we expected that the storageReader passed in here comes from
        // this storage engine itself, anything else is considered a bug. And we do know the inner workings
        // of the storage statements that we create.
        RecordStorageCommandCreationContext creationContext =
                (RecordStorageCommandCreationContext) commandCreationContext;
        LogCommandSerialization serialization =
                RecordStorageCommandReaderFactory.INSTANCE.get(commandCreationContext.kernelVersion());
        var locks = creationContext.getLocks();
        TransactionRecordState recordState = creationContext.createTransactionRecordState(
                locks,
                lockTracer,
                serialization,
                memoryTracker,
                lockVerificationFactory.createLockVerification(
                        locks, txState, neoStores, schemaRuleAccess, storeCursors, memoryTracker));

        // Visit transaction state and populate these record state objects
        TxStateVisitor txStateVisitor = new TransactionToRecordStateVisitor(
                recordState,
                schemaState,
                schemaRuleAccess,
                constraintSemantics,
                storeCursors,
                memoryTracker,
                tokenHolders.lookupWithIds(),
                format);
        CountsRecordState countsRecordState = new CountsRecordState(serialization);
        txStateVisitor = additionalTxStateVisitor.apply(txStateVisitor);
        txStateVisitor = new TransactionCountingStateVisitor(
                txStateVisitor, storageReader, txState, countsRecordState, cursorContext, storeCursors, memoryTracker);
        try (TxStateVisitor visitor = txStateVisitor) {
            txState.accept(visitor);
        }

        // Convert record state into commands
        recordState.extractCommands(commands, memoryTracker);
        countsRecordState.extractCommands(commands, memoryTracker);

        // Verify sufficient locks
        CommandLockVerification commandLockVerification = lockVerificationFactory.createCommandVerification(
                locks, txState, neoStores, schemaRuleAccess, storeCursors, memoryTracker);
        commandLockVerification.verifySufficientlyLocked(commands);

        return commands;
    }

    @Override
    public StorageCommand.VersionUpgradeCommand createUpgradeCommand(
            KernelVersion from, KernelVersion to, LogFormat logFormatTo) {
        return Command.MetaDataCommand.upgradeCommand(
                RecordStorageCommandReaderFactory.INSTANCE.get(from), from, to, logFormatTo);
    }

    @Override
    public EnrichmentCommand createEnrichmentCommand(KernelVersion kernelVersion, Enrichment enrichment) {
        return new RecordEnrichmentCommand(RecordStorageCommandReaderFactory.INSTANCE.get(kernelVersion), enrichment);
    }

    @Override
    public void lockRecoveryCommands(
            CommandBatch commands, LockService.Client lockService, TransactionApplicationMode mode) throws IOException {
        handleRecordStorageCommands(commands, c -> c.lockForRecovery(lockService, mode));
    }

    @Override
    public void apply(StorageEngineTransaction batch, TransactionApplicationMode mode) throws Exception {
        TransactionAppliersDispatcherFactory batchApplier = applierDispatcherFactory(mode);
        StorageEngineTransaction initialBatch = batch;
        try (BatchContext context = createBatchContext(batchApplier, batch)) {
            while (batch != null) {
                if (batch.commandBatch().isEmptyTransaction()) {
                    applyEmptyTransaction(batch, mode);
                } else {
                    try (var txApplier = batchApplier.startTx(batch, context)) {
                        batch.commandBatch().accept(txApplier);
                    }
                }
                batch = batch.next();
            }
        } catch (Throwable cause) {
            TransactionApplyKernelException kernelException = TransactionApplyKernelException.internalError(
                    cause,
                    this.getClass().getSimpleName(),
                    "Failed to apply transaction: %s",
                    batch == null ? initialBatch : batch);
            databaseHealth.panic(kernelException);
            throw kernelException;
        }
    }

    private void applyEmptyTransaction(StorageEngineTransaction batch, TransactionApplicationMode mode) {
        if (!mode.isReverseStep()) {
            countsStore.noCountUpdate(batch.transactionId(), batch.cursorContext());
            groupDegreesStore.noCountUpdate(batch.transactionId(), batch.cursorContext());
        }
    }

    @Override
    public void release(
            ReadableTransactionState txState,
            CursorContext cursorContext,
            CommandCreationContext commandCreationContext,
            boolean rolledBack) {
        if (rolledBack && !txState.isMultiChunk() && commandCreationContext.resetIds()) {
            return;
        }

        // Extract allocated IDs from created nodes/relationships from txState
        // (optionally) flick through the commands to try and salvage other types of IDs, like property/dynamic record
        // IDs, but that's way less bang for your buck.
        unallocateIds(txState.addedAndRemovedNodes(), RecordIdType.NODE, cursorContext, rolledBack);
        unallocateIds(txState.addedAndRemovedRelationships(), RecordIdType.RELATIONSHIP, cursorContext, rolledBack);
    }

    private void unallocateIds(LongDiffSets ids, IdType idType, CursorContext cursorContext, boolean rolledBack) {
        // Free those that were created
        if (rolledBack) {
            unallocateIds(ids.getAdded(), idType, cursorContext);
        }
        // Free those that were created and then deleted
        unallocateIds(ids.getRemovedFromAdded(), idType, cursorContext);
    }

    private void unallocateIds(LongSet ids, IdType idType, CursorContext cursorContext) {
        if (!ids.isEmpty()) {
            try (var marker = idGeneratorFactory.get(idType).transactionalMarker(cursorContext)) {
                ids.forEach(marker::markUnallocated);
            }
        }
    }

    private BatchContext createBatchContext(
            TransactionAppliersDispatcherFactory batchApplier, StorageEngineTransaction initialBatch) {
        return new BatchContextImpl(
                indexUpdateListener,
                indexUpdatesSync,
                neoStores.getNodeStore(),
                neoStores.getPropertyStore(),
                this,
                schemaCache,
                initialBatch.cursorContext(),
                otherMemoryTracker,
                batchApplier.getIdUpdateListener(idGeneratorWorkSyncs, initialBatch.cursorContext()),
                initialBatch.storeCursors());
    }

    /**
     * Provides a {@link TransactionAppliersDispatcherFactory} that is to be used for all transactions
     * in a batch. Each transaction is handled by a {@link TransactionAppliersDispatcher} which wraps the
     * individual {@link TransactionApplier}s returned by the wrapped {@link TransactionApplierFactory}s.
     */
    protected TransactionAppliersDispatcherFactory applierDispatcherFactory(TransactionApplicationMode mode) {
        return applierDispatchers.get(mode);
    }

    private LockService lockService(TransactionApplicationMode mode) {
        return mode == RECOVERY || mode.isReverseStep() ? NO_LOCK_SERVICE : lockService;
    }

    @Override
    public void init() {
        var kernelVersionApplierFactory =
                new KernelVersionTransactionApplierFactory(logMetadataProvider, internalLogProvider);
        for (TransactionApplicationMode mode : TransactionApplicationMode.values()) {
            applierDispatchers.put(mode, buildRegularAppliersDispatcherFactory(mode, kernelVersionApplierFactory));
        }
    }

    @Override
    public void start() throws Exception {
        try (var cursorContext = contextFactory.create(STORAGE_ENGINE_START_TAG)) {
            neoStores.start(cursorContext);
            countsStore.start(cursorContext, otherMemoryTracker);
            groupDegreesStore.start(cursorContext, otherMemoryTracker);
        }
    }

    @VisibleForTesting
    public void loadSchemaCache(boolean ignoreUnreadable) {
        try (var cursorContext = contextFactory.create(SCHEMA_CACHE_START_TAG);
                var storeCursors = new CachedStoreCursors(neoStores, cursorContext)) {
            Iterable<SchemaRule> schemaRules = ignoreUnreadable
                    ? schemaRuleAccess.getAllIgnoreMalformed(storeCursors, otherMemoryTracker)
                    : schemaRuleAccess.getAll(storeCursors, otherMemoryTracker);
            schemaCache.load(schemaRules);
        }
    }

    @Override
    public void stop() throws Exception {}

    @Override
    public void shutdown() {
        if (!closed) {
            try {
                executeAll(countsStore::close, groupDegreesStore::close, neoStores::close);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            } finally {
                closed = true;
            }
        }
    }

    @Override
    public void checkpoint(
            DatabaseFlushEvent flushEvent, AsyncBlockAccessor asyncBlockAccessor, CursorContext cursorContext)
            throws IOException {

        log.debug("Checkpointing %s", RecordDatabaseFile.COUNTS_STORE.getName());
        try (var fileFlushEvent = flushEvent.beginFileFlush()) {
            countsStore.checkpoint(fileFlushEvent, asyncBlockAccessor, cursorContext);
        }
        log.debug("Checkpointing %s", RecordDatabaseFile.RELATIONSHIP_GROUP_DEGREES_STORE.getName());
        try (var fileFlushEvent = flushEvent.beginFileFlush()) {
            groupDegreesStore.checkpoint(fileFlushEvent, asyncBlockAccessor, cursorContext);
        }
        neoStores.checkpoint(flushEvent, asyncBlockAccessor, cursorContext);
    }

    @Override
    public void dumpDiagnostics(InternalLog errorLog, DiagnosticsLogger diagnosticsLog) {
        DiagnosticsManager.dump(new NeoStoreIdUsage(neoStores), errorLog, diagnosticsLog);
        DiagnosticsManager.dump(new NeoStoreRecords(neoStores), errorLog, diagnosticsLog);
    }

    @Override
    public Collection<Path> listStorageFiles(StorageFileSelection selection) {
        List<Path> files = new ArrayList<>();
        if (selection.includeAtomicStoreFiles() && selection.includeRecoverableFiles()) {
            files.addAll(databaseLayout.countStore().allSegments(neoStores.getFileSystem()));
            files.addAll(databaseLayout.relationshipGroupDegreesStore().allSegments(neoStores.getFileSystem()));
        }
        if (selection.includeReplayableStoreFiles()) {
            for (StoreType type : StoreType.STORE_TYPES) {
                final RecordStore<AbstractBaseRecord> recordStore = neoStores.getRecordStore(type);
                files.addAll(recordStore.getStoreFile().allSegments(neoStores.getFileSystem()));
            }
        }
        if (selection.includeIdFiles()) {
            for (var file : RecordDatabaseFile.values()) {
                databaseLayout.idFile(file).ifPresent(sp -> files.addAll(sp.allSegments(neoStores.getFileSystem())));
            }
        }
        return files;
    }

    /**
     * @return the underlying {@link NeoStores} which should <strong>ONLY</strong> be accessed by tests
     * until all tests are properly converted to not rely on access to {@link NeoStores}. Currently, there
     * are important tests which asserts details about the neo stores that are very important to test,
     * but to convert all those tests might be a bigger piece of work.
     */
    @VisibleForTesting
    public NeoStores testAccessNeoStores() {
        return neoStores;
    }

    @VisibleForTesting
    public SchemaRuleAccess testAccessSchemaRules() {
        return schemaRuleAccess;
    }

    @Override
    public Lifecycle schemaAndTokensLifecycle(boolean ignoreUnreadable) {
        return new LifecycleAdapter() {
            @Override
            public void init() {
                try (var cursorContext = contextFactory.create(TOKENS_INIT_TAG);
                        var storeCursors = new CachedStoreCursors(neoStores, cursorContext)) {
                    TokensLoader tokensLoader = ignoreUnreadable
                            ? StoreTokens.allReadableTokens(neoStores)
                            : StoreTokens.allTokens(neoStores);
                    tokenHolders.setInitialTokens(tokensLoader, storeCursors, otherMemoryTracker);
                }
                loadSchemaCache(ignoreUnreadable);
            }
        };
    }

    @Override
    public CountsStore countsAccessor() {
        return countsStore;
    }

    @VisibleForTesting
    public RelationshipGroupDegreesStore relationshipGroupDegreesStore() {
        return groupDegreesStore;
    }

    @Override
    public MetaDataStore metadataProvider() {
        return neoStores.getMetaDataStore();
    }

    @Override
    public LogMetadataProvider logMetadataProvider() {
        return logMetadataProvider;
    }

    @Override
    public StoreEntityCounters storeEntityCounters() {
        return storeEntityCounters;
    }

    @Override
    public ImmutableSet<OpenOption> getOpenOptions() {
        return neoStores.getOpenOptions();
    }

    @Override
    public StorageEngineIndexingBehaviour indexingBehaviour() {
        return indexingBehaviour;
    }

    @Override
    public TransactionStateBehaviour transactionStateBehaviour() {
        return txStateBehaviour;
    }

    @Override
    public StorageEngineCharacteristics characteristics() {
        return characteristics;
    }

    @Override
    public long estimateAvailableReservedSpace() {
        return neoStores.estimateAvailableReservedSpace();
    }

    @Override
    public void preAllocateStoreFilesForCommands(StorageEngineTransaction batch, TransactionApplicationMode mode)
            throws IOException {
        if (!mode.isReverseStep() && batch != null) {
            try (var txApplier = new SingleApplierDispatcher(new PreAllocationTransactionApplier(neoStores))) {
                while (batch != null) {
                    batch.commandBatch().accept(txApplier);
                    batch = batch.next();
                }
            } catch (OutOfDiskSpaceException e) {
                databaseHealth.outOfDiskSpace(e);
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void prefetchPagesForCommands(StorageEngineTransaction batch, TransactionApplicationMode mode) {
        if (!mode.isReverseStep() && batch != null) {
            try (var txApplier =
                    new SingleApplierDispatcher(new PrefetchingTransactionApplier(neoStores, pagePrefetcher))) {
                while (batch != null) {
                    batch.commandBatch().accept(txApplier);
                    batch = batch.next();
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public CommandReaderFactory commandReaderFactory() {
        return RecordStorageCommandReaderFactory.INSTANCE;
    }

    private class RecordCountsBuilder implements CountsBuilder {
        private final InternalLog log;
        private final LogMetadataProvider logMetadataProvider;
        private final FileSystemAbstraction fs;
        private final CursorContextFactory contextFactory;
        private final RecordDatabaseLayout layout;

        public RecordCountsBuilder(
                InternalLogProvider internalLogProvider,
                FileSystemAbstraction fs,
                CursorContextFactory contextFactory,
                RecordDatabaseLayout layout,
                LogMetadataProvider logMetadataProvider) {
            this.fs = fs;
            this.contextFactory = contextFactory;
            this.layout = layout;
            log = internalLogProvider.getLog(MetaDataStore.class);
            this.logMetadataProvider = logMetadataProvider;
        }

        @Override
        public void initialize(CountsUpdater updater, CursorContext cursorContext, MemoryTracker memoryTracker) {
            log.warn("Missing counts store, rebuilding it.");
            try (NumberArrayFactory numberArrayFactory =
                    NumberArrayFactories.auto(fs, layout.databaseDirectory(), log)) {
                new CountsComputer(
                                neoStores,
                                logMetadataProvider.getLastCommittedTransactionId(),
                                contextFactory,
                                memoryTracker,
                                numberArrayFactory)
                        .initialize(updater, cursorContext, memoryTracker);
            }
            log.warn("Counts store rebuild completed.");
        }

        @Override
        public long lastCommittedTxId() {
            return logMetadataProvider.getLastCommittedTransactionId();
        }
    }

    private record RecordTransactionStateBehaviour() implements TransactionStateBehaviour {
        @Override
        public boolean keepMetaDataForDeletedRelationship() {
            return false;
        }

        @Override
        public boolean useIndexCommands() {
            return false;
        }
    }
}
