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
import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.index.internal.gbptree.MetadataMismatchException;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.AnyTokenSchemaDescriptor;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexQuery;
import org.neo4j.internal.schema.IndexQuery.IndexQueryType;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.MinimalIndexAccessor;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;
import org.neo4j.storageengine.migration.TokenIndexMigrator;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.ValueCategory;

public class TokenIndexProvider extends IndexProvider {
    private final DatabaseIndexContext databaseIndexContext;
    private final RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;
    private final Monitor monitor;
    private final DatabaseLayout databaseLayout;

    protected TokenIndexProvider(
            DatabaseIndexContext databaseIndexContext,
            IndexDirectoryStructure.Factory directoryStructureFactory,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            DatabaseLayout databaseLayout) {
        super(
                KernelVersion.VERSION_IN_WHICH_TOKEN_INDEXES_ARE_INTRODUCED,
                AllIndexProviderDescriptors.TOKEN_DESCRIPTOR,
                directoryStructureFactory);
        this.databaseIndexContext = databaseIndexContext;
        this.recoveryCleanupWorkCollector = recoveryCleanupWorkCollector;
        this.monitor =
                databaseIndexContext.monitors.newMonitor(IndexProvider.Monitor.class, databaseIndexContext.monitorTag);
        this.databaseLayout = databaseLayout;
    }

    @Override
    public MinimalIndexAccessor getMinimalIndexAccessor(IndexDescriptor descriptor, boolean forRebuildDuringRecovery) {
        return new NativeMinimalIndexAccessor(
                descriptor, indexFiles(descriptor), databaseIndexContext.readOnlyChecker, false);
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

        return new WorkSyncedIndexPopulator(new TokenIndexPopulator(
                databaseIndexContext, indexFiles(descriptor), descriptor, openOptions, indexingBehaviour));
    }

    @Override
    public IndexAccessor getOnlineAccessor(
            IndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig,
            TokenNameLookup tokenNameLookup,
            ImmutableSet<OpenOption> openOptions,
            boolean readOnly,
            StorageEngineIndexingBehaviour indexingBehaviour) {
        return new TokenIndexAccessor(
                databaseIndexContext,
                indexFiles(descriptor),
                descriptor,
                recoveryCleanupWorkCollector,
                openOptions,
                readOnly,
                indexingBehaviour);
    }

    @Override
    public String getPopulationFailure(
            IndexDescriptor descriptor, CursorContext cursorContext, ImmutableSet<OpenOption> openOptions) {
        try {
            String failureMessage = TokenIndexes.readFailureMessage(
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
            return TokenIndexes.readState(
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
        return new TokenIndexMigrator(
                "Token indexes",
                fs,
                pageCache,
                pageCacheTracer,
                storageEngineFactory,
                databaseLayout,
                this::storeFile,
                contextFactory);
    }

    @Override
    public IndexDescriptor completeConfiguration(
            IndexDescriptor index, StorageEngineIndexingBehaviour indexingBehaviour) {
        if (index.getCapability().equals(IndexCapability.NO_CAPABILITY)) {
            boolean hasOrdering = !(index.schema().entityType().equals(EntityType.RELATIONSHIP)
                    && indexingBehaviour.useNodeIdsInRelationshipTokenIndex());
            index = index.withIndexCapability(capability(hasOrdering));
        }
        return index;
    }

    @Override
    public IndexPrototype validatePrototype(IndexPrototype prototype) {
        IndexType indexType = prototype.getIndexType();
        if (indexType != IndexType.LOOKUP) {
            throw new IllegalArgumentException("The '" + getProviderDescriptor().name()
                    + "' index provider does not support " + indexType + " indexes: " + prototype);
        }
        if (!prototype.schema().isSchemaDescriptorType(AnyTokenSchemaDescriptor.class)) {
            throw new IllegalArgumentException("The " + prototype.schema()
                    + " index schema is not an any-token index schema, which it is required to be for the '"
                    + getProviderDescriptor().name() + "' index provider to be able to create an index.");
        }
        if (!prototype.getIndexProvider().equals(AllIndexProviderDescriptors.TOKEN_DESCRIPTOR)) {
            throw new IllegalArgumentException("The '" + getProviderDescriptor().name()
                    + "' index provider does not support " + prototype.getIndexProvider() + " indexes: " + prototype);
        }
        if (prototype.isUnique()) {
            throw new IllegalArgumentException("The '" + getProviderDescriptor().name()
                    + "' index provider does not support uniqueness indexes: " + prototype);
        }
        return prototype;
    }

    @Override
    public IndexType getIndexType() {
        return IndexType.LOOKUP;
    }

    private Path storeFile(SchemaRule schemaRule) {
        IndexFiles indexFiles = indexFiles(schemaRule);
        return indexFiles.getStoreFile();
    }

    private IndexFiles indexFiles(SchemaRule schemaRule) {
        return indexFiles(schemaRule, databaseIndexContext.fileSystem, directoryStructure());
    }

    public static IndexFiles indexFiles(
            SchemaRule schemaRule, FileSystemAbstraction fileSystem, IndexDirectoryStructure indexDirectoryStructure) {
        return new IndexFiles(fileSystem, indexDirectoryStructure, schemaRule.getId());
    }

    public static IndexCapability capability(boolean supportsOrder) {
        return new TokenIndexCapability(supportsOrder);
    }

    private static class TokenIndexCapability implements IndexCapability {
        private final boolean supportsOrdering;

        private TokenIndexCapability(boolean supportsOrdering) {
            this.supportsOrdering = supportsOrdering;
        }

        @Override
        public boolean supportsOrdering() {
            return supportsOrdering;
        }

        @Override
        public boolean supportsReturningValues() {
            return true;
        }

        @Override
        public boolean areValueCategoriesAccepted(ValueCategory... valueCategories) {
            Preconditions.requireNonEmpty(valueCategories);
            Preconditions.requireNoNullElements(valueCategories);
            return false;
        }

        @Override
        public boolean isQuerySupported(IndexQueryType queryType, ValueCategory valueCategory) {
            return queryType == IndexQueryType.TOKEN_LOOKUP && valueCategory == ValueCategory.NO_CATEGORY;
        }

        @Override
        public double getCostMultiplier(IndexQueryType... queryTypes) {
            return COST_MULTIPLIER_STANDARD;
        }

        @Override
        public boolean supportPartitionedScan(IndexQuery... queries) {
            Preconditions.requireNonEmpty(queries);
            Preconditions.requireNoNullElements(queries);
            return queries.length == 1 && queries[0].type() == IndexQueryType.TOKEN_LOOKUP;
        }
    }
}
