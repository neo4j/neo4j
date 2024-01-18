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

import static org.apache.commons.lang3.StringUtils.defaultIfEmpty;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.MetadataMismatchException;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.impl.index.SchemaIndexMigrator;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure.Factory;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.MinimalIndexAccessor;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;

/**
 * Base class for native indexes on top of {@link GBPTree}.
 *
 * @param <KEY> type of {@link NativeIndexKey}
 * @param <LAYOUT> type of {@link IndexLayout}
 */
abstract class NativeIndexProvider<KEY extends NativeIndexKey<KEY>, LAYOUT extends IndexLayout<KEY>>
        extends IndexProvider {
    protected final DatabaseIndexContext databaseIndexContext;
    protected final RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;
    private final Monitor monitor;
    protected final Config config;
    protected final boolean archiveFailedIndex;

    protected NativeIndexProvider(
            DatabaseIndexContext databaseIndexContext,
            IndexProviderDescriptor descriptor,
            Factory directoryStructureFactory,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            Config config) {
        super(KernelVersion.VERSION_RANGE_POINT_TEXT_INDEXES_ARE_INTRODUCED, descriptor, directoryStructureFactory);
        this.databaseIndexContext = databaseIndexContext;
        this.recoveryCleanupWorkCollector = recoveryCleanupWorkCollector;
        this.monitor =
                databaseIndexContext.monitors.newMonitor(IndexProvider.Monitor.class, databaseIndexContext.monitorTag);
        this.config = config;
        this.archiveFailedIndex = config.get(GraphDatabaseInternalSettings.archive_failed_index);
    }

    /**
     * Instantiates the {@link Layout} which is used in the index backing this native index provider.
     *
     * @param descriptor the {@link IndexDescriptor} for this index.
     * @return the correct {@link Layout} for the index.
     */
    abstract LAYOUT layout(IndexDescriptor descriptor);

    @Override
    public MinimalIndexAccessor getMinimalIndexAccessor(IndexDescriptor descriptor, boolean forRebuildDuringRecovery) {
        return new NativeMinimalIndexAccessor(
                descriptor,
                indexFiles(descriptor),
                databaseIndexContext.readOnlyChecker,
                archiveFailedIndex && forRebuildDuringRecovery);
    }

    @Override
    public IndexPopulator getPopulator(
            IndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig,
            ByteBufferFactory bufferFactory,
            MemoryTracker memoryTracker,
            TokenNameLookup tokenNameLookup,
            ImmutableSet<OpenOption> openOptions,
            StorageEngineIndexingBehaviour indexingBehaviour) {
        if (databaseIndexContext.readOnlyChecker.isReadOnly()) {
            throw new UnsupportedOperationException("Can't create populator for read only index");
        }

        IndexFiles indexFiles = indexFiles(descriptor);
        return newIndexPopulator(
                indexFiles, layout(descriptor), descriptor, bufferFactory, memoryTracker, tokenNameLookup, openOptions);
    }

    protected abstract IndexPopulator newIndexPopulator(
            IndexFiles indexFiles,
            LAYOUT layout,
            IndexDescriptor descriptor,
            ByteBufferFactory bufferFactory,
            MemoryTracker memoryTracker,
            TokenNameLookup tokenNameLookup,
            ImmutableSet<OpenOption> openOptions);

    @Override
    public IndexAccessor getOnlineAccessor(
            IndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig,
            TokenNameLookup tokenNameLookup,
            ImmutableSet<OpenOption> openOptions,
            boolean readOnly,
            StorageEngineIndexingBehaviour indexingBehaviour) {
        IndexFiles indexFiles = indexFiles(descriptor);
        return newIndexAccessor(indexFiles, layout(descriptor), descriptor, tokenNameLookup, openOptions, readOnly);
    }

    protected abstract IndexAccessor newIndexAccessor(
            IndexFiles indexFiles,
            LAYOUT layout,
            IndexDescriptor descriptor,
            TokenNameLookup tokenNameLookup,
            ImmutableSet<OpenOption> openOptions,
            boolean readOnly);

    @Override
    public String getPopulationFailure(
            IndexDescriptor descriptor, CursorContext cursorContext, ImmutableSet<OpenOption> openOptions) {
        try {
            String failureMessage = NativeIndexes.readFailureMessage(
                    databaseIndexContext.pageCache,
                    storeFile(descriptor),
                    databaseIndexContext.databaseName,
                    cursorContext,
                    openOptions);
            return defaultIfEmpty(failureMessage, StringUtils.EMPTY);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InternalIndexState getInitialState(
            IndexDescriptor descriptor, CursorContext cursorContext, ImmutableSet<OpenOption> openOptions) {
        try {
            return NativeIndexes.readState(
                    databaseIndexContext.pageCache,
                    storeFile(descriptor),
                    databaseIndexContext.databaseName,
                    cursorContext,
                    openOptions);
        } catch (MetadataMismatchException | IOException e) {
            monitor.failedToOpenIndex(descriptor, "Requesting re-population.", e);
            return InternalIndexState.POPULATING;
        }
    }

    @Override
    public StoreMigrationParticipant storeMigrationParticipant(
            FileSystemAbstraction fs,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            StorageEngineFactory storageEngineFactory,
            CursorContextFactory contextFactory) {
        return new SchemaIndexMigrator(
                getProviderDescriptor().name() + " indexes",
                fs,
                pageCache,
                pageCacheTracer,
                directoryStructure(),
                storageEngineFactory,
                contextFactory);
    }

    private Path storeFile(IndexDescriptor descriptor) {
        IndexFiles indexFiles = indexFiles(descriptor);
        return indexFiles.getStoreFile();
    }

    private IndexFiles indexFiles(IndexDescriptor descriptor) {
        return new IndexFiles(databaseIndexContext.fileSystem, directoryStructure(), descriptor.getId());
    }
}
