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
package org.neo4j.internal.batchimport.cache.idmapping;

import static org.neo4j.configuration.GraphDatabaseInternalSettings.index_populator_block_size;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.exact;
import static org.neo4j.internal.kernel.api.QueryContext.NULL_CONTEXT;
import static org.neo4j.io.IOUtils.closeAllUnchecked;
import static org.neo4j.kernel.impl.index.schema.IndexUsageTracker.NO_USAGE_TRACKER;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.OpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.ImmutableSet;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.batchimport.api.PropertyValueLookup;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.batchimport.api.input.ReadableGroups;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.internal.batchimport.PopulationWorkJobScheduler;
import org.neo4j.internal.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.IOUtils;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.io.memory.UnsafeDirectByteBufferAllocator;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntryConflictHandler;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.PhaseTracker;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.index.schema.NodeValueIterator;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.values.storable.Values;

/**
 * Uses an {@link IndexAccessor} for carrying the data for the {@link IdMapper}.
 */
public class IndexIdMapper implements IdMapper {
    private final Map<String, IndexAccessor> accessors;
    private final IndexProviderMap tempIndexes;
    private final TokenNameLookup tokenNameLookup;
    private final Map<String, IndexDescriptor> indexDescriptors;
    private final PopulationWorkJobScheduler workScheduler;
    private final ImmutableSet<OpenOption> openOptions;
    private final Configuration configuration;
    private final PageCacheTracer pageCacheTracer;
    private final IndexStatisticsStore indexStatisticsStore;
    private final ReadableGroups groups;
    private final ThreadLocal<Map<String, Index>> threadLocal;
    private final List<Index> indexes = new CopyOnWriteArrayList<>();
    private final Map<String, Populator> populators = new HashMap<>();
    private final ByteBufferFactory bufferFactory;
    private final StorageEngineIndexingBehaviour indexingBehaviour;
    private final MutableLongSet duplicateNodeIds = LongSets.mutable.empty();
    private final LongAdder numAdded = new LongAdder();

    // key is groupName, and for some reason accessors doesn't expose which descriptor they're for, so pass that in too
    public IndexIdMapper(
            Map<String, IndexAccessor> accessors,
            IndexProviderMap tempIndexes,
            TokenNameLookup tokenNameLookup,
            Map<String, IndexDescriptor> indexDescriptors,
            PopulationWorkJobScheduler workScheduler,
            ImmutableSet<OpenOption> openOptions,
            Configuration configuration,
            PageCacheTracer pageCacheTracer,
            IndexStatisticsStore indexStatisticsStore,
            ReadableGroups groups,
            StorageEngineIndexingBehaviour indexingBehaviour)
            throws IOException {
        this.accessors = accessors;
        this.tempIndexes = tempIndexes;
        this.tokenNameLookup = tokenNameLookup;
        this.indexDescriptors = indexDescriptors;
        this.workScheduler = workScheduler;
        this.openOptions = openOptions;
        this.configuration = configuration;
        this.pageCacheTracer = pageCacheTracer;
        this.indexStatisticsStore = indexStatisticsStore;
        this.groups = groups;
        this.threadLocal = ThreadLocal.withInitial(HashMap::new);
        this.bufferFactory = new ByteBufferFactory(
                UnsafeDirectByteBufferAllocator::new,
                Config.defaults().get(index_populator_block_size).intValue());
        this.indexingBehaviour = indexingBehaviour;
        for (var entry : accessors.entrySet()) {
            var descriptor = indexDescriptors.get(entry.getKey());
            var indexProvider = tempIndexes.lookup(descriptor.getIndexProvider());
            var populator = indexProvider.getPopulator(
                    descriptor,
                    new IndexSamplingConfig(Config.defaults()),
                    bufferFactory,
                    EmptyMemoryTracker.INSTANCE,
                    tokenNameLookup,
                    openOptions,
                    indexingBehaviour);
            populator.create();
            populators.put(entry.getKey(), new Populator(populator, descriptor));
        }
    }

    @Override
    public void put(Object inputId, long actualId, Group group) {
        var populator = populators.get(group.name());
        var update = IndexEntryUpdate.add(actualId, populator.descriptor, Values.of(inputId));
        try {
            populator.populator.add(Collections.singleton(update), CursorContext.NULL_CONTEXT);
            populator.populator.includeSample(update);
            numAdded.increment();
        } catch (IndexEntryConflictException e) {
            throw new RuntimeException(e);
        }
    }

    private Index index(Group group) {
        return threadLocal.get().computeIfAbsent(group.name(), groupName -> {
            var accessor = accessors.get(groupName);
            var reader = accessor.newValueReader(NO_USAGE_TRACKER);
            var schemaDescriptor = indexDescriptors.get(groupName);
            var index = new Index(reader, schemaDescriptor.schema());
            indexes.add(index);
            return index;
        });
    }

    @Override
    public boolean needsPreparation() {
        return true;
    }

    /**
     * Schedules "scanCompleted" calls to any index populations that are part of this ID mapper,
     * such that they can be scheduled with "scanCompleted" calls to other index populations.
     */
    public void completeBuild(Collector collector, Consumer<Runnable> scheduler) {
        for (var entry : populators.entrySet()) {
            scheduler.accept(() -> {
                var conflictHandler = conflictHandler(collector, entry);
                try {
                    var populator = entry.getValue();
                    populator.populator.scanCompleted(
                            PhaseTracker.nullInstance, workScheduler, conflictHandler, CursorContext.NULL_CONTEXT);
                    indexStatisticsStore.setSampleStats(
                            populator.descriptor.getId(), populator.populator.sample(CursorContext.NULL_CONTEXT));
                    populator.populator.close(true, CursorContext.NULL_CONTEXT);
                } catch (IndexEntryConflictException e) {
                    // This should not happen since we use DELETE action
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private IndexEntryConflictHandler conflictHandler(Collector collector, Map.Entry<String, Populator> entry) {
        return (firstEntityId, otherEntityId, values) -> {
            synchronized (duplicateNodeIds) {
                duplicateNodeIds.add(otherEntityId);
            }
            collector.collectDuplicateNode(values[0].asObjectCopy(), otherEntityId, groups.get(entry.getKey()));
            return IndexEntryConflictHandler.IndexEntryConflictAction.DELETE;
        };
    }

    /**
     * Validates all added entries from {@link #put(Object, long, Group)} and collects duplicates into
     * the {@code collector}, also returning the IDs of violating nodes.
     * Must be run before {@link IdMapper#prepare(PropertyValueLookup, Collector, ProgressMonitorFactory)}.
     *
     * @param collector {@link Collector} to report violations into.
     * @return the IDs of the violating nodes.
     */
    public LongSet validate(Collector collector) {
        for (var entry : populators.entrySet()) {
            var conflictHandler = conflictHandler(collector, entry);

            try {
                var populator = entry.getValue();
                var indexProvider = tempIndexes.lookup(populator.descriptor.getIndexProvider());
                try (var newNodesIndex = indexProvider.getOnlineAccessor(
                        populator.descriptor,
                        new IndexSamplingConfig(Config.defaults()),
                        tokenNameLookup,
                        openOptions,
                        indexingBehaviour)) {
                    var accessor = accessors.get(entry.getKey());
                    accessor.validate(
                            newNodesIndex,
                            true,
                            conflictHandler,
                            null,
                            configuration.maxNumberOfWorkerThreads(),
                            workScheduler.jobScheduler());
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return duplicateNodeIds;
    }

    @Override
    public void prepare(
            PropertyValueLookup inputIdLookup, Collector collector, ProgressMonitorFactory progressMonitorFactory) {
        for (var entry : populators.entrySet()) {
            try {
                var descriptor = entry.getValue().descriptor;
                var indexProvider = tempIndexes.lookup(descriptor.getIndexProvider());
                try (var newNodesIndex = indexProvider.getOnlineAccessor(
                                descriptor,
                                new IndexSamplingConfig(Config.defaults()),
                                tokenNameLookup,
                                openOptions,
                                indexingBehaviour);
                        var progress = progressMonitorFactory.singlePart("Prepare ID mapper", numAdded.sum())) {
                    var accessor = accessors.get(entry.getKey());
                    accessor.insertFrom(
                            newNodesIndex,
                            null,
                            true,
                            IndexEntryConflictHandler.THROW,
                            id -> !duplicateNodeIds.contains(id),
                            configuration.maxNumberOfWorkerThreads(),
                            workScheduler.jobScheduler(),
                            progress);
                }
            } catch (IndexEntryConflictException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public Getter newGetter() {
        return new Getter() {
            @Override
            public long get(Object inputId, Group group) {
                // TODO somehow reuse client/progressor per thread?
                try (var client = new NodeValueIterator()) {
                    // TODO do we need a proper QueryContext?
                    var index = index(group);
                    index.reader.query(
                            client,
                            NULL_CONTEXT,
                            unconstrained(),
                            exact(index.schemaDescriptor.getPropertyId(), inputId));
                    return client.hasNext() ? client.next() : -1;
                } catch (IndexNotApplicableKernelException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void close() {}
        };
    }

    @Override
    public void close() {
        closeAllUnchecked(
                (Closeable) () -> closeAllUnchecked(indexes),
                () -> {
                    for (var accessor : accessors.values()) {
                        try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                            accessor.force(flushEvent, CursorContext.NULL_CONTEXT);
                        }
                    }
                },
                () -> closeAllUnchecked(accessors.values()),
                bufferFactory);
    }

    public void additionalViolatingNodes(LongSet violatingNodes) {
        synchronized (duplicateNodeIds) {
            duplicateNodeIds.addAll(violatingNodes);
        }
    }

    @Override
    public LongIterator leftOverDuplicateNodesIds() {
        // TODO could be a memory overhead for large amounts of duplicates?
        return duplicateNodeIds.toSortedList().longIterator();
    }

    @Override
    public MemoryStatsVisitor.Visitable memoryEstimation(long numberOfNodes) {
        return visitor -> {};
    }

    @Override
    public void acceptMemoryStatsVisitor(MemoryStatsVisitor visitor) {}

    private record Index(ValueIndexReader reader, SchemaDescriptor schemaDescriptor) implements Closeable {
        @Override
        public void close() throws IOException {
            IOUtils.closeAll(reader);
        }
    }

    private record Populator(IndexPopulator populator, IndexDescriptor descriptor) {}
}
