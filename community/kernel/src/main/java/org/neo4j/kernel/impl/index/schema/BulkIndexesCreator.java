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
package org.neo4j.kernel.impl.index.schema;

import static java.util.Objects.requireNonNull;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.kernel.impl.api.TransactionVisibilityProvider.EMPTY_VISIBILITY_PROVIDER;
import static org.neo4j.kernel.impl.locking.LockManager.NO_LOCKS_LOCK_MANAGER;
import static org.neo4j.lock.LockService.NO_LOCK_SERVICE;
import static org.neo4j.scheduler.Group.INDEX_POPULATION;
import static org.neo4j.scheduler.Group.INDEX_POPULATION_WORK;

import java.io.IOException;
import java.util.List;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.primitive.ObjectFloatMaps;
import org.neo4j.batchimport.api.IndexesCreator;
import org.neo4j.collection.Dependencies;
import org.neo4j.common.Subject;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HostedOnMode;
import org.neo4j.index.internal.gbptree.GroupingRecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.impl.muninn.VersionStorage;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.index.BulkIndexCreationContext;
import org.neo4j.kernel.database.MetadataCache;
import org.neo4j.kernel.impl.api.DatabaseSchemaState;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.IndexingServiceFactory;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.index.DatabaseIndexStats;
import org.neo4j.kernel.impl.transaction.state.StaticIndexProviderMapFactory;
import org.neo4j.kernel.impl.transaction.state.storeview.FullScanStoreView;
import org.neo4j.kernel.impl.transaction.state.storeview.IndexStoreViewFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.ReadableStorageEngine;
import org.neo4j.time.Clocks;
import org.neo4j.token.TokenHolders;

public class BulkIndexesCreator implements IndexesCreator {

    private final Config config;
    private final ReadableStorageEngine storageEngine;
    private final DatabaseLayout databaseLayout;
    private final FileSystemAbstraction fileSystem;
    private final PageCache pageCache;
    private final MetadataCache metadataCache;
    private final JobScheduler jobScheduler;
    private final TokenHolders tokenHolders;
    private final CursorContextFactory contextFactory;
    private final PageCacheTracer pageCacheTracer;
    private final LogService logService;
    private final MemoryTracker memoryTracker;

    public BulkIndexesCreator(BulkIndexCreationContext context) {
        this.config = requireNonNull(context).config();
        this.storageEngine = context.storageEngine();
        this.databaseLayout = context.databaseLayout();
        this.fileSystem = context.fileSystem();
        this.pageCache = context.pageCache();
        this.metadataCache = context.metadataCache();
        this.jobScheduler = context.jobScheduler();
        this.tokenHolders = context.tokenHolders();
        this.contextFactory = context.contextFactory();
        this.pageCacheTracer = context.pageCacheTracer();
        this.logService = context.logService();
        this.memoryTracker = context.memoryTracker();
    }

    @Override
    public void create(CreationListener creationListener, List<IndexDescriptor> indexDescriptors) throws IOException {
        final var descriptorCount = indexDescriptors.size();
        if (descriptorCount == 0) {
            return;
        }

        // need to create the lifecycle in the NONE state as the scheduler has already been started
        // adding an already started lifecycle component and then calling start then fails
        try (var creationContext = contextFactory.create("Indexing creation");
                var life = Lifespan.createWithNoneState()) {
            final var indexingService = indexingService(life);

            life.start(); // this will call life.init automatically

            indexingService.createIndexes(
                    Subject.SYSTEM,
                    indexDescriptors.stream()
                            .map(indexingService::completeConfiguration)
                            .toArray(IndexDescriptor[]::new));

            final var failed = Lists.mutable.<String>empty();
            final var progressTracker = ObjectFloatMaps.mutable.<IndexDescriptor>empty();
            var completed = 0;
            while (completed < descriptorCount) {
                for (var indexProxy : indexingService.getIndexProxies()) {
                    final var descriptor = indexProxy.getDescriptor();
                    final var latestProgress =
                            indexProxy.getIndexPopulationProgress().getProgress();
                    final var updatedValue = progressTracker.updateValue(descriptor, latestProgress, lastProgress -> {
                        creationListener.onUpdate(descriptor, latestProgress - lastProgress);
                        return latestProgress;
                    });
                    if (updatedValue == latestProgress && latestProgress > 0f) {
                        // already had some updates on first pass through the loop so update here too
                        creationListener.onUpdate(descriptor, latestProgress);
                    }

                    final var state = indexProxy.getState();
                    if (state == InternalIndexState.FAILED) {
                        completed++;
                        failed.add(indexProxy
                                .getPopulationFailure()
                                .asIndexPopulationFailure(descriptor.schema(), descriptor.userDescription(tokenHolders))
                                .getMessage());
                    } else if (state == InternalIndexState.ONLINE || latestProgress == 1.0f) {
                        // some proxies are 'tentative' and stay at POPULATING even though they are actually done
                        completed++;
                    }
                }

                sleepIgnoreInterrupt();
            }

            if (!failed.isEmpty()) {
                throw new IOException("Index creation failed -  %d of %d failed to complete: %s"
                        .formatted(failed.size(), descriptorCount, failed.makeString("[", ", ", "]")));
            }

            creationListener.onCreationCompleted();

            try (var flushEvent = pageCacheTracer.beginDatabaseFlush()) {
                indexingService.checkpoint(flushEvent, creationContext);
                creationListener.onCheckpointingCompleted();
            }
        }
    }

    private IndexingService indexingService(LifeSupport life) throws IOException {
        final var clock = Clocks.nanoClock();
        final var logProvider = logService.getInternalLogProvider();
        final var schemaState = new DatabaseSchemaState(logProvider);
        final var cleanupCollector = life.add(new GroupingRecoveryCleanupWorkCollector(
                jobScheduler, INDEX_POPULATION, INDEX_POPULATION_WORK, databaseLayout.getDatabaseName()));
        final var readOnlyChecker = DatabaseReadOnlyChecker.writable();

        final var indexDependencies = new Dependencies();
        indexDependencies.satisfyDependencies(VersionStorage.EMPTY_STORAGE);

        final var indexProviderMap = life.add(StaticIndexProviderMapFactory.create(
                life,
                config,
                pageCache,
                fileSystem,
                logService,
                new Monitors(),
                readOnlyChecker,
                HostedOnMode.SINGLE,
                cleanupCollector,
                databaseLayout,
                tokenHolders,
                jobScheduler,
                contextFactory,
                pageCacheTracer,
                indexDependencies));

        final var fullScanStoreView = new FullScanStoreView(NO_LOCK_SERVICE, storageEngine, config, jobScheduler);
        final var indexStoreViewFactory = new IndexStoreViewFactory(
                config, storageEngine, NO_LOCKS_LOCK_MANAGER, fullScanStoreView, NO_LOCK_SERVICE, logProvider);

        final var indexStatisticsStore = life.add(new IndexStatisticsStore(
                pageCache,
                fileSystem,
                databaseLayout.indexStatisticsStore(),
                immediate(),
                false,
                databaseLayout.getDatabaseName(),
                contextFactory,
                pageCacheTracer,
                storageEngine.getOpenOptions()));

        return life.add(IndexingServiceFactory.createIndexingService(
                storageEngine,
                config,
                jobScheduler,
                indexProviderMap,
                indexStoreViewFactory,
                tokenHolders,
                List.of(),
                logService.getInternalLogProvider(),
                IndexMonitor.NO_MONITOR,
                schemaState,
                indexStatisticsStore,
                new DatabaseIndexStats(),
                contextFactory,
                memoryTracker,
                databaseLayout.getDatabaseName(),
                readOnlyChecker,
                clock,
                metadataCache,
                fileSystem,
                EMPTY_VISIBILITY_PROVIDER));
    }

    private static void sleepIgnoreInterrupt() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            // Ignore interrupted exceptions here.
        }
    }
}
