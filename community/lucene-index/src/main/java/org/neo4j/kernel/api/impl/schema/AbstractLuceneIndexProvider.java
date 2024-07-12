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
package org.neo4j.kernel.api.impl.schema;

import static org.apache.commons.lang3.StringUtils.defaultIfEmpty;

import java.io.IOException;
import java.nio.file.OpenOption;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.impl.index.LuceneMinimalIndexAccessor;
import org.neo4j.kernel.api.impl.index.MinimalDatabaseIndex;
import org.neo4j.kernel.api.impl.index.SchemaIndexMigrator;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.IndexStorageFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.MinimalIndexAccessor;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;
import org.neo4j.util.VisibleForTesting;

public abstract class AbstractLuceneIndexProvider extends IndexProvider {
    private final IndexStorageFactory indexStorageFactory;
    private final Monitor monitor;
    private final IndexType supportedIndexType;
    protected final Config config;
    protected final DatabaseReadOnlyChecker readOnlyChecker;

    public AbstractLuceneIndexProvider(
            KernelVersion minimumRequiredVersion,
            IndexType supportedIndexType,
            IndexProviderDescriptor descriptor,
            FileSystemAbstraction fileSystem,
            DirectoryFactory directoryFactory,
            IndexDirectoryStructure.Factory directoryStructureFactory,
            Monitors monitors,
            Config config,
            DatabaseReadOnlyChecker readOnlyChecker) {
        super(minimumRequiredVersion, descriptor, directoryStructureFactory);
        this.supportedIndexType = supportedIndexType;
        this.readOnlyChecker = readOnlyChecker;
        this.monitor = monitors.newMonitor(Monitor.class, descriptor.toString());
        this.indexStorageFactory = buildIndexStorageFactory(fileSystem, directoryFactory);
        this.config = config;
    }

    @VisibleForTesting
    protected IndexStorageFactory buildIndexStorageFactory(
            FileSystemAbstraction fileSystem, DirectoryFactory directoryFactory) {
        return new IndexStorageFactory(directoryFactory, fileSystem, directoryStructure());
    }

    @Override
    public IndexPrototype validatePrototype(IndexPrototype prototype) {
        final var indexType = prototype.getIndexType();
        final var providerName = getProviderDescriptor().name();
        if (indexType != supportedIndexType) {
            throw new IllegalArgumentException("The '%s' index provider does not support %s indexes: %s"
                    .formatted(providerName, indexType, prototype));
        }
        if (prototype.isUnique()) {
            throw new IllegalArgumentException(
                    "The '%s' index provider does not support unique indexes: %s".formatted(providerName, prototype));
        }
        return prototype;
    }

    @Override
    public IndexType getIndexType() {
        return supportedIndexType;
    }

    @Override
    public MinimalIndexAccessor getMinimalIndexAccessor(IndexDescriptor descriptor, boolean forRebuildDuringRecovery) {
        PartitionedIndexStorage indexStorage = indexStorageFactory.indexStorageOf(descriptor.getId());
        final var index = new MinimalDatabaseIndex<>(indexStorage, descriptor, config);
        return new LuceneMinimalIndexAccessor<>(descriptor, index, readOnlyChecker.isReadOnly());
    }

    @Override
    public InternalIndexState getInitialState(
            IndexDescriptor descriptor, CursorContext cursorContext, ImmutableSet<OpenOption> openOptions) {
        final var indexStorage = getIndexStorage(descriptor.getId());
        final var failure = indexStorage.getStoredIndexFailure();
        if (failure != null) {
            return InternalIndexState.FAILED;
        }
        try {
            return indexIsOnline(indexStorage, descriptor, config)
                    ? InternalIndexState.ONLINE
                    : InternalIndexState.POPULATING;
        } catch (IOException e) {
            // TODO VECTOR: IndexProviderTests expects this monitor call if the index doesn't exist so indexIsOnline now
            //  throws in that case, instead of returning false. Using IOException for this isn't very elegant.
            monitor.failedToOpenIndex(descriptor, "Requesting re-population.", e);
            return InternalIndexState.POPULATING;
        }
    }

    @Override
    public StoreMigrationParticipant storeMigrationParticipant(
            final FileSystemAbstraction fs,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            StorageEngineFactory storageEngineFactory,
            CursorContextFactory contextFactory) {
        return new SchemaIndexMigrator(
                getProviderDescriptor().name(),
                fs,
                pageCache,
                pageCacheTracer,
                this.directoryStructure(),
                storageEngineFactory,
                contextFactory);
    }

    @Override
    public String getPopulationFailure(
            IndexDescriptor descriptor, CursorContext cursorContext, ImmutableSet<OpenOption> openOptions)
            throws IllegalStateException {
        return defaultIfEmpty(getIndexStorage(descriptor.getId()).getStoredIndexFailure(), StringUtils.EMPTY);
    }

    protected PartitionedIndexStorage getIndexStorage(long indexId) {
        return indexStorageFactory.indexStorageOf(indexId);
    }

    @Override
    public void shutdown() throws Exception {
        indexStorageFactory.close();
    }

    public static boolean indexIsOnline(PartitionedIndexStorage indexStorage, IndexDescriptor descriptor, Config config)
            throws IOException {
        try (var index = new MinimalDatabaseIndex<>(indexStorage, descriptor, config)) {
            if (index.exists()) {
                index.open();
                return index.isOnline();
            }
            throw new IOException("Index does not exist");
        }
    }
}
