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

import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.index_populator_block_size;
import static org.neo4j.internal.batchimport.IncrementalBatchImportUtil.moveIndex;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.LongPredicate;
import java.util.function.Supplier;
import org.eclipse.collections.api.block.function.primitive.LongToLongFunction;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.ImmutableSet;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.IntLists;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaCache;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.io.memory.UnsafeDirectByteBufferAllocator;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryConflictHandler;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.PhaseTracker;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/**
 * uniqueness constraint index (prepare copies these):
 * - neo4j/schema/index/range-1.0/3
 * - copy to neo4j-incremental-12345/schema/index/range-1.0/3
 * - build: neo4j-incremental-12345/temp-schema/index/range-1.0/3
 *    - validate(): merge neo4j-incremental-12345/temp-schema/index/range-1.0/3 --> neo4j-incremental-12345/schema/index/range-1.0/3
 * - merge: move neo4j-incremental-12345/schema/index/range-1.0/3 --> neo4j/schema/index/range-1.0/3
 *
 * non-uniqueness constraint index (prepare does not copy these):
 * - neo4j/schema/index/range-1.0/3
 * - build: neo4j-incremental-12345/temp-schema/index/range-1.0/3
 *    - validate(): move neo4j-incremental-12345/temp-schema/index/range-1.0/3 --> neo4j-incremental-12345/schema/index/range-1.0/3
 * - merge: merge neo4j-incremental-12345/schema/index/range-1.0/3 --> neo4j/schema/index/range-1.0/3
 */
public class OtherAffectedSchemaMonitors implements Supplier<SchemaMonitor>, Closeable {
    private final FileSystemAbstraction fileSystem;
    private final IndexProviderMap indexProviderMap;
    private final IndexProviderMap tempIndexes;
    private final SchemaCache schemaCache;
    private final TokenNameLookup tokenNameLookup;
    private final EntityType entityType;
    private final ImmutableSet<OpenOption> openOptions;
    private final PopulationWorkJobScheduler workScheduler;
    private final LongToLongFunction indexedEntityIdConverter;
    private final LongToLongFunction entityIdFromIndexIdConverter;
    private final Configuration configuration;
    private final IndexStatisticsStore indexStatisticsStore;
    private final IntObjectMap<List<int[]>> propertyExistenceConstraints;
    private final Map<IndexDescriptor, IndexPopulator> indexPopulators = new ConcurrentHashMap<>();
    private final Lock populatorConstructionLock = new ReentrantLock();
    private final ByteBufferFactory bufferFactory;
    private final MutableLongSet violatingEntities = LongSets.mutable.empty().asSynchronized();
    private final StorageEngineIndexingBehaviour indexingBehaviour;

    public OtherAffectedSchemaMonitors(
            FileSystemAbstraction fileSystem,
            IndexProviderMap indexProviderMap,
            IndexProviderMap tempIndexes,
            SchemaCache schemaCache,
            TokenNameLookup tokenNameLookup,
            EntityType entityType,
            ImmutableSet<OpenOption> openOptions,
            PopulationWorkJobScheduler workScheduler,
            LongToLongFunction indexedEntityIdConverter,
            LongToLongFunction entityIdFromIndexIdConverter,
            Configuration configuration,
            IndexStatisticsStore indexStatisticsStore,
            StorageEngineIndexingBehaviour indexingBehaviour) {
        this.fileSystem = fileSystem;
        this.indexProviderMap = indexProviderMap;
        this.tempIndexes = tempIndexes;
        this.schemaCache = schemaCache;
        this.tokenNameLookup = tokenNameLookup;
        this.entityType = entityType;
        this.openOptions = openOptions;
        this.workScheduler = workScheduler;
        this.indexedEntityIdConverter = indexedEntityIdConverter;
        this.entityIdFromIndexIdConverter = entityIdFromIndexIdConverter;
        this.configuration = configuration;
        this.indexStatisticsStore = indexStatisticsStore;
        this.indexingBehaviour = indexingBehaviour;
        this.propertyExistenceConstraints = buildPropertyExistenceConstraintsMap(schemaCache, entityType);
        this.bufferFactory = new ByteBufferFactory(
                UnsafeDirectByteBufferAllocator::new,
                Config.defaults().get(index_populator_block_size).intValue());
    }

    private static MutableIntObjectMap<List<int[]>> buildPropertyExistenceConstraintsMap(
            SchemaCache schemaCache, EntityType entityType) {
        var propertyExistenceConstraints = IntObjectMaps.mutable.<List<int[]>>empty();
        for (var constraint : schemaCache.constraints()) {
            if (constraint.enforcesPropertyExistence() && constraint.schema().entityType() == entityType) {
                var schema = constraint.schema();
                for (var entityToken : schema.getEntityTokenIds()) {
                    propertyExistenceConstraints
                            .getIfAbsentPut(entityToken, ArrayList::new)
                            .add(schema.getPropertyIds());
                }
            }
        }
        return propertyExistenceConstraints.isEmpty() ? null : propertyExistenceConstraints;
    }

    /**
     * Will be invoked once for each worker, i.e. this should create a new monitor used by a single thread.
     */
    @Override
    public SchemaMonitor get() {
        return new OtherAffectedSchemaMonitor();
    }

    /**
     * Schedules "scanCompleted" calls to any index populations that are part of this ID mapper,
     * such that they can be scheduled with "scanCompleted" calls to other index populations.
     */
    public void completeBuild(Collector collector, Consumer<Runnable> scheduler) {
        for (var population : indexPopulators.entrySet()) {
            // Complete the population of the increment index
            var populator = population.getValue();
            scheduler.accept(() -> {
                var conflictHandler = new RecordingIndexEntryConflictHandler(
                        collector,
                        violatingEntities,
                        population.getKey(),
                        tokenNameLookup,
                        entityIdFromIndexIdConverter);
                try {
                    populator.scanCompleted(
                            PhaseTracker.nullInstance, workScheduler, conflictHandler, CursorContext.NULL_CONTEXT);
                    indexStatisticsStore.setSampleStats(population.getKey().getId(), populator.sample(NULL_CONTEXT));
                    populator.close(true, CursorContext.NULL_CONTEXT);
                } catch (IndexEntryConflictException e) {
                    // Should not happen
                    throw new RuntimeException(e);
                }
            });
        }
    }

    /**
     * @return a list of entity IDs that violated constraints during complete (e.g. merging of indexes).
     */
    public LongSet validate(LongSet skippedEntityIds, Collector collector) {
        // Merge increments into copied-target-indexes and skip (and remember) those that violate constraints
        var indexSamplingConfig = new IndexSamplingConfig(Config.defaults());
        try {
            for (var population : indexPopulators.entrySet()) {
                var descriptor = population.getKey();
                var conflictHandler = new RecordingIndexEntryConflictHandler(
                        collector, violatingEntities, descriptor, tokenNameLookup, entityIdFromIndexIdConverter);
                // For constraint indexes checking violations
                if (descriptor.isUnique()) {
                    // Validate uniqueness, since it's a constraint index
                    try (var copiedIncrementIndex = indexProviderMap
                                    .lookup(descriptor.getIndexProvider())
                                    .getOnlineAccessor(
                                            descriptor,
                                            indexSamplingConfig,
                                            tokenNameLookup,
                                            openOptions,
                                            indexingBehaviour);
                            var builtIncrementIndex = tempIndexes
                                    .lookup(descriptor.getIndexProvider())
                                    .getOnlineAccessor(
                                            descriptor,
                                            indexSamplingConfig,
                                            tokenNameLookup,
                                            openOptions,
                                            indexingBehaviour)) {
                        copiedIncrementIndex.validate(
                                builtIncrementIndex,
                                true,
                                conflictHandler,
                                skippedEntityIds::contains,
                                configuration.maxNumberOfWorkerThreads(),
                                workScheduler.jobScheduler());
                    }
                }
            }

            // When all violations are known then merge all increment indexes
            LongPredicate filter = skippedEntityIds.isEmpty() && violatingEntities.isEmpty()
                    ? null
                    : indexEntityId -> !skippedEntityIds.contains(indexEntityId)
                            && !violatingEntities.contains(entityIdFromIndexIdConverter.applyAsLong(indexEntityId));
            for (var descriptor : indexPopulators.keySet()) {
                if (!descriptor.isUnique() && filter == null) {
                    // For non-constraint indexes we can simply move the increment index into place
                    // if there are no violations.
                    moveIndex(fileSystem, tempIndexes, indexProviderMap, descriptor);
                } else {
                    try (var copiedIncrementIndex = indexProviderMap
                                    .lookup(descriptor.getIndexProvider())
                                    .getOnlineAccessor(
                                            descriptor,
                                            indexSamplingConfig,
                                            tokenNameLookup,
                                            openOptions,
                                            indexingBehaviour);
                            var builtIncrementIndex = tempIndexes
                                    .lookup(descriptor.getIndexProvider())
                                    .getOnlineAccessor(
                                            descriptor,
                                            indexSamplingConfig,
                                            tokenNameLookup,
                                            openOptions,
                                            indexingBehaviour)) {
                        copiedIncrementIndex.insertFrom(
                                builtIncrementIndex,
                                null,
                                false,
                                IndexEntryConflictHandler.THROW,
                                filter,
                                configuration.maxNumberOfWorkerThreads(),
                                workScheduler.jobScheduler(),
                                ProgressListener.NONE);
                        copiedIncrementIndex.force(FileFlushEvent.NULL, NULL_CONTEXT);
                    }
                }
            }
        } catch (IndexEntryConflictException e) {
            // This will not be thrown, but the method is declared to throw it so just catch it here
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return violatingEntities;
    }

    @Override
    public void close() throws IOException {
        bufferFactory.close();
    }

    public LongSet affectedIndexes() {
        var ids = LongSets.mutable.empty();
        indexPopulators.keySet().stream().map(IndexDescriptor::getId).forEach(ids::add);
        return ids;
    }

    private class OtherAffectedSchemaMonitor implements SchemaMonitor {
        private final MutableIntList entityTokens = IntLists.mutable.empty();
        private final MutableIntObjectMap<Value> properties = IntObjectMaps.mutable.empty();

        @Override
        public void property(int propertyKeyId, Object value) {
            if (value instanceof Value propValue) {
                properties.put(propertyKeyId, propValue);
            } else {
                properties.put(propertyKeyId, Values.of(value));
            }
        }

        @Override
        public void entityToken(int entityTokenId) {
            entityTokens.add(entityTokenId);
        }

        @Override
        public void entityTokens(int[] entityTokenIds) {
            entityTokens.addAll(entityTokenIds);
        }

        @Override
        public boolean endOfEntity(long entityId, ViolationVisitor violationVisitor) {
            try {
                entityTokens.sortThis();
                boolean propertyExistenceOk = checkPropertyExistenceConstraints(entityId, violationVisitor);
                if (propertyExistenceOk) {
                    generateIndexUpdatesForAffectedIndexes(entityId);
                }
                return propertyExistenceOk;
            } finally {
                entityTokens.clear();
                properties.clear();
            }
        }

        private void generateIndexUpdatesForAffectedIndexes(long entityId) {
            // TODO might be a bit expensive?
            var propertyKeyTokens = properties.keySet().toSortedArray();
            var indexes = schemaCache.getValueIndexesRelatedTo(
                    entityTokens.toArray(), EMPTY_INT_ARRAY, propertyKeyTokens, true, entityType);
            for (var index : indexes) {
                var populator = getIndexPopulator(index);
                var indexUpdate = constructIndexUpdate(entityId, index);
                try {
                    // TODO only adding one update per call, not cool?
                    // TODO cursor context?
                    populator.add(List.of(indexUpdate), NULL_CONTEXT);
                    populator.includeSample(indexUpdate);
                } catch (IndexEntryConflictException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        private IndexEntryUpdate<IndexDescriptor> constructIndexUpdate(long entityId, IndexDescriptor index) {
            var propertyIds = index.schema().getPropertyIds();
            Value[] values = new Value[propertyIds.length];
            for (int i = 0; i < propertyIds.length; i++) {
                values[i] = properties.get(propertyIds[i]);
            }
            return IndexEntryUpdate.add(indexedEntityIdConverter.applyAsLong(entityId), index, values);
        }

        private IndexPopulator getIndexPopulator(IndexDescriptor index) {
            var populator = indexPopulators.get(index);
            if (populator == null) {
                populatorConstructionLock.lock();
                try {
                    populator = indexPopulators.get(index);
                    if (populator == null) {
                        populator = constructIndexPopulator(index);
                        indexPopulators.put(index, populator);
                    }
                } finally {
                    populatorConstructionLock.unlock();
                }
            }
            return populator;
        }

        private IndexPopulator constructIndexPopulator(IndexDescriptor index) {
            var indexProvider = tempIndexes.lookup(index.getIndexProvider());
            var populator = indexProvider.getPopulator(
                    index,
                    new IndexSamplingConfig(Config.defaults()),
                    bufferFactory,
                    EmptyMemoryTracker.INSTANCE,
                    tokenNameLookup,
                    openOptions,
                    indexingBehaviour);
            try {
                populator.create();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return populator;
        }

        private boolean checkPropertyExistenceConstraints(long entityId, ViolationVisitor violationVisitor) {
            if (propertyExistenceConstraints != null) {
                var entityTokensIterator = entityTokens.intIterator();
                while (entityTokensIterator.hasNext()) {
                    var existenceConstraints = propertyExistenceConstraints.get(entityTokensIterator.next());
                    if (existenceConstraints != null) {
                        for (var mandatoryProperties : existenceConstraints) {
                            if (!properties.keySet().containsAll(mandatoryProperties)) {
                                violationVisitor.accept(
                                        entityId, entityTokens, properties, "a property existence constraint");
                                return false;
                            }
                        }
                    }
                }
            }
            return true;
        }
    }

    private record RecordingIndexEntryConflictHandler(
            Collector badCollector,
            MutableLongSet violatingEntities,
            IndexDescriptor descriptor,
            TokenNameLookup tokenNameLookup,
            LongToLongFunction entityIdFromIndexIdConverter)
            implements IndexEntryConflictHandler {

        @Override
        public IndexEntryConflictAction indexEntryConflict(long firstEntityId, long otherEntityId, Value[] values) {
            long realId = entityIdFromIndexIdConverter.applyAsLong(otherEntityId);
            violatingEntities.add(realId);
            badCollector.collectEntityViolatingConstraint(
                    null,
                    realId,
                    asPropertyMap(descriptor, values),
                    descriptor.userDescription(tokenNameLookup),
                    descriptor.schema().entityType());
            return IndexEntryConflictAction.DELETE;
        }

        private Map<String, Object> asPropertyMap(IndexDescriptor descriptor, Value[] values) {
            var properties = new HashMap<String, Object>();
            var propertyIds = descriptor.schema().getPropertyIds();
            for (var i = 0; i < propertyIds.length; i++) {
                properties.put(tokenNameLookup.propertyKeyGetName(propertyIds[i]), values[i].asObjectCopy());
            }
            return properties;
        }
    }
}
