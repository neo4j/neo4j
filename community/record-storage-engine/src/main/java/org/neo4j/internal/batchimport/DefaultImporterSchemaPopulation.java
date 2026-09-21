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

import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.async.AsyncBlockAccessor.EMPTY_ASYNC_BLOCK_ACCESSOR;
import static org.neo4j.io.memory.ByteBufferFactory.heapBufferFactory;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.lifecycle.LifecycleAdapter.onShutdown;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.eclipse.collections.api.factory.primitive.LongSets;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.function.Predicates;
import org.neo4j.internal.batchimport.OtherAffectedSchemaMonitors.NodeLabelChecker;
import org.neo4j.internal.batchimport.store.BatchingNeoStores;
import org.neo4j.internal.recordstorage.RecordStorageIndexingBehaviour;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaCache;
import org.neo4j.internal.schema.SchemaCommand;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryConflictHandler;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvidersAccess;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.PhaseTracker;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StandardConstraintRuleAccessor;
import org.neo4j.values.ElementIdMapper;

class DefaultImporterSchemaPopulation implements ImporterSchemaPopulation {
    static final String TEMP_INDEXES_DIRECTORY = "temp-schema-indexes";
    private static final String CHECKPOINT_STATISTICS_TAG = "importerSchemaIndexStatisticsCheckpoint";

    private final BatchingNeoStores neoStores;
    private final FileSystemAbstraction fileSystem;
    private final Configuration configuration;
    private final Config dbConfig;
    private final PageCacheTracer pageCacheTracer;
    private final CursorContextFactory contextFactory;
    private final MemoryTracker memoryTracker;
    private final IndexProviderMap targetProviders;
    private final IndexProviderMap tempProviders;
    private final Path tempIndexesRoot;
    private final StorageEngineIndexingBehaviour indexingBehaviour;
    private final PopulationWorkJobScheduler workScheduler;
    private final IndexStatisticsStore indexStatisticsStore;
    private final Lifespan indexStatisticsStoreLife;
    private final ImporterSchemaCommandExecutor executor;
    private final SchemaCache schemaCache;
    private final List<SchemaMonitors> issuedMonitors = new ArrayList<>();
    private boolean completed;

    static DefaultImporterSchemaPopulation create(
            List<SchemaCommand> commands,
            BatchingNeoStores neoStores,
            FileSystemAbstraction fileSystem,
            Supplier<IndexProvidersAccess> indexProvidersAccess,
            DatabaseLayout databaseLayout,
            Configuration configuration,
            Config dbConfig,
            JobScheduler jobScheduler,
            PageCacheTracer pageCacheTracer,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker)
            throws IOException {
        var providersAccess = indexProvidersAccess.get();
        try {
            return new DefaultImporterSchemaPopulation(
                    commands,
                    neoStores,
                    fileSystem,
                    providersAccess,
                    databaseLayout,
                    configuration,
                    dbConfig,
                    jobScheduler,
                    pageCacheTracer,
                    contextFactory,
                    memoryTracker);
        } catch (Throwable e) {
            IOUtils.closeAllSilently(providersAccess);
            deleteTempIndexes(fileSystem, databaseLayout, e);
            throw e;
        }
    }

    private static void deleteTempIndexes(
            FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout, Throwable original) {
        try {
            var tempIndexesRoot = databaseLayout.databaseDirectory().resolve(TEMP_INDEXES_DIRECTORY);
            if (fileSystem.fileExists(tempIndexesRoot)) {
                fileSystem.deleteRecursively(tempIndexesRoot);
            }
        } catch (IOException e) {
            original.addSuppressed(e);
        }
    }

    private DefaultImporterSchemaPopulation(
            List<SchemaCommand> commands,
            BatchingNeoStores neoStores,
            FileSystemAbstraction fileSystem,
            IndexProvidersAccess providersAccess,
            DatabaseLayout databaseLayout,
            Configuration configuration,
            Config dbConfig,
            JobScheduler jobScheduler,
            PageCacheTracer pageCacheTracer,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker)
            throws IOException {
        this.neoStores = neoStores;
        this.fileSystem = fileSystem;
        this.configuration = configuration;
        this.dbConfig = dbConfig;
        this.pageCacheTracer = pageCacheTracer;
        this.contextFactory = contextFactory;
        this.memoryTracker = memoryTracker;
        this.indexingBehaviour = new RecordStorageIndexingBehaviour(
                neoStores.getNodeStore().getRecordsPerPage(),
                neoStores.getRelationshipStore().getRecordsPerPage());
        this.targetProviders = providersAccess.access(
                neoStores.getPageCache(), databaseLayout, writable(), neoStores.getTokenHolders());
        this.tempIndexesRoot = databaseLayout.databaseDirectory().resolve(TEMP_INDEXES_DIRECTORY);
        this.tempProviders = providersAccess.access(
                neoStores.getPageCache(),
                RecordDatabaseLayout.ofFlat(tempIndexesRoot),
                writable(),
                neoStores.getTokenHolders());
        this.executor = new ImporterSchemaCommandExecutor(
                neoStores,
                commands,
                index -> targetProviders.completeConfiguration(index, indexingBehaviour),
                contextFactory,
                memoryTracker);
        executor.createSchemaRules();
        this.schemaCache = new SchemaCache(new StandardConstraintRuleAccessor(), (c, ignored) -> c, indexingBehaviour);
        executor.indexes().forEach(schemaCache::addSchemaRule);
        executor.constraints().forEach(schemaCache::addSchemaRule);
        this.workScheduler = new PopulationWorkJobScheduler(jobScheduler, databaseLayout);
        this.indexStatisticsStore = new IndexStatisticsStore(
                neoStores.getPageCache(),
                fileSystem,
                databaseLayout,
                immediate(),
                false,
                contextFactory,
                pageCacheTracer,
                neoStores.getOpenOptions());
        this.indexStatisticsStoreLife = new Lifespan(
                onShutdown(this::deleteTempIndexes), onShutdown(providersAccess::close), indexStatisticsStore);
        indexStatisticsStoreLife.start();
    }

    List<IndexDescriptor> indexes() {
        return executor.indexes();
    }

    private SchemaMonitors monitors(EntityType entityType) {
        if (completed) {
            throw new IllegalStateException("Schema population has already completed");
        }
        var monitors = new OtherAffectedSchemaMonitors(
                fileSystem,
                targetProviders,
                tempProviders,
                schemaCache,
                neoStores.getTokenHolders(),
                entityType,
                neoStores.getOpenOptions(),
                workScheduler,
                id -> id,
                id -> id,
                configuration,
                indexStatisticsStore,
                indexingBehaviour,
                true,
                Predicates.alwaysFalse(),
                dbConfig,
                IndexPopulator.DEFAULT_CONFIGURATION,
                NodeLabelChecker.DISABLED_NODE_CHECKER_FACTORY);
        // the caller closes these, but closing does not clear what they populated, so they stay queryable for
        // populateUntouchedIndexesAsEmpty
        issuedMonitors.add(monitors);
        return monitors;
    }

    @Override
    public SchemaMonitors nodeMonitors() {
        return monitors(EntityType.NODE);
    }

    @Override
    public SchemaMonitors relationshipMonitors() {
        return monitors(EntityType.RELATIONSHIP);
    }

    /**
     * The data-import stages that used {@link #nodeMonitors()}/{@link #relationshipMonitors()} have already
     * validated the monitors and written the populated indexes to the target at the end of each stage —
     * re-running validation here would fail on the already-moved indexes, so this only backfills and
     * checkpoints statistics.
     */
    @Override
    public void completeAfterImport() throws IOException {
        completed = true;
        populateUntouchedIndexesAsEmpty();
        try (var cursorContext = contextFactory.create(CHECKPOINT_STATISTICS_TAG);
                var flushEvent = pageCacheTracer.beginFileFlush()) {
            indexStatisticsStore.checkpoint(flushEvent, EMPTY_ASYNC_BLOCK_ACCESSOR, cursorContext);
        }
    }

    private void populateUntouchedIndexesAsEmpty() throws IOException {
        var populated = LongSets.mutable.empty();
        issuedMonitors.forEach(monitors -> populated.addAll(monitors.affectedIndexes()));
        var samplingConfig = new IndexSamplingConfig(dbConfig);
        try (var bufferFactory = heapBufferFactory((int) kibiBytes(64))) {
            for (IndexDescriptor index : executor.indexes()) {
                if (populated.contains(index.getId())) {
                    continue;
                }
                var provider = targetProviders.lookup(index.getIndexProvider());
                var populator = provider.getPopulator(
                        index,
                        samplingConfig,
                        bufferFactory,
                        memoryTracker,
                        neoStores.getTokenHolders(),
                        ElementIdMapper.PLACEHOLDER,
                        neoStores.getOpenOptions(),
                        indexingBehaviour,
                        IndexPopulator.DEFAULT_CONFIGURATION);
                boolean success = false;
                try {
                    populator.create();
                    populator.scanCompleted(
                            PhaseTracker.nullInstance, workScheduler, IndexEntryConflictHandler.THROW, NULL_CONTEXT);
                    indexStatisticsStore.setSampleStats(index.getId(), populator.sample(NULL_CONTEXT));
                    success = true;
                } catch (IndexEntryConflictException e) {
                    throw new IllegalStateException("Empty index population cannot conflict", e);
                } finally {
                    populator.close(success, NULL_CONTEXT);
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        indexStatisticsStoreLife.close();
    }

    private void deleteTempIndexes() throws IOException {
        if (fileSystem.fileExists(tempIndexesRoot)) {
            fileSystem.deleteRecursively(tempIndexesRoot);
        }
    }
}
