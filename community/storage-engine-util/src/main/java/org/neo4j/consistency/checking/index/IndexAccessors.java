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
package org.neo4j.consistency.checking.index;

import static org.neo4j.kernel.impl.index.schema.IndexUsageTracking.NO_USAGE_TRACKING;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.set.ImmutableSet;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.memory.MemoryTracker;

public class IndexAccessors implements Closeable {
    private static final String CONSISTENCY_INDEX_ACCESSOR_BUILDER_TAG = "consistencyIndexAccessorBuilder";
    private final MutableLongObjectMap<IndexAccessor> propertyIndexAccessors = new LongObjectHashMap<>();
    private final List<IndexDescriptor> onlineIndexRules = new ArrayList<>();
    private final List<IndexDescriptor> notOnlineIndexRules = new ArrayList<>();
    private final List<IndexDescriptor> inconsistentRules = new ArrayList<>();
    private IndexAccessor nodeLabelIndex;
    private IndexAccessor relationshipTypeIndex;

    public IndexAccessors(
            IndexProviderMap providers,
            IndexDescriptorProvider indexes,
            IndexSamplingConfig samplingConfig,
            TokenNameLookup tokenNameLookup,
            CursorContextFactory contextFactory,
            ImmutableSet<OpenOption> openOptions,
            StorageEngineIndexingBehaviour behavior,
            MemoryTracker memoryTracker) {
        this(
                providers,
                indexes,
                samplingConfig,
                null /*we'll use a default below, if this is null*/,
                tokenNameLookup,
                contextFactory,
                openOptions,
                behavior,
                memoryTracker);
    }

    public IndexAccessors(
            IndexProviderMap providers,
            IndexDescriptorProvider descriptorProvider,
            IndexSamplingConfig samplingConfig,
            IndexAccessorLookup accessorLookup,
            TokenNameLookup tokenNameLookup,
            CursorContextFactory contextFactory,
            ImmutableSet<OpenOption> openOptions,
            StorageEngineIndexingBehaviour behavior,
            MemoryTracker memoryTracker) {
        try (var cursorContext = contextFactory.create(CONSISTENCY_INDEX_ACCESSOR_BUILDER_TAG)) {
            // Default to instantiate new accessors
            accessorLookup = accessorLookup != null
                    ? accessorLookup
                    : index -> provider(providers, index)
                            .getOnlineAccessor(index, samplingConfig, tokenNameLookup, openOptions, true, behavior);
            try (var descriptors = descriptorProvider.indexDescriptors(cursorContext, memoryTracker)) {
                while (descriptors.hasNext()) {
                    try {
                        IndexDescriptor indexDescriptor = descriptors.next();
                        // we intentionally only check indexes that are online since
                        // - populating indexes will be rebuilt on next startup
                        // - failed indexes have to be dropped by the user anyways
                        IndexProvider indexProvider = provider(providers, indexDescriptor);
                        indexDescriptor = indexProvider.completeConfiguration(indexDescriptor, behavior);
                        if (indexDescriptor.isUnique()
                                && indexDescriptor.getOwningConstraintId().isEmpty()) {
                            notOnlineIndexRules.add(indexDescriptor);
                        } else if (InternalIndexState.ONLINE
                                == indexProvider.getInitialState(indexDescriptor, cursorContext, openOptions)) {
                            long indexId = indexDescriptor.getId();
                            try {
                                final IndexAccessor accessor = accessorLookup.apply(indexDescriptor);
                                if (indexDescriptor.isTokenIndex()) {
                                    if (indexDescriptor.schema().entityType() == EntityType.NODE) {
                                        nodeLabelIndex = accessor;
                                    } else {
                                        relationshipTypeIndex = accessor;
                                    }
                                } else {
                                    propertyIndexAccessors.put(indexId, accessor);
                                    onlineIndexRules.add(indexDescriptor);
                                }
                            } catch (RuntimeException e) {
                                inconsistentRules.add(indexDescriptor);
                            }
                        } else {
                            notOnlineIndexRules.add(indexDescriptor);
                        }
                    } catch (Exception e) {
                        // ignore; inconsistencies of the schema store are specifically handled elsewhere.
                    }
                }
            }
        }
    }

    private static IndexProvider provider(IndexProviderMap providers, IndexDescriptor indexRule) {
        return providers.lookup(indexRule.getIndexProvider());
    }

    public Collection<IndexDescriptor> notOnlineRules() {
        return notOnlineIndexRules;
    }

    public Collection<IndexDescriptor> inconsistentRules() {
        return inconsistentRules;
    }

    public IndexAccessor accessorFor(IndexDescriptor indexRule) {
        return accessorFor(indexRule.getId());
    }

    public IndexAccessor accessorFor(long indexRuleId) {
        return propertyIndexAccessors.get(indexRuleId);
    }

    public List<IndexDescriptor> onlineRules() {
        return onlineIndexRules;
    }

    public List<IndexDescriptor> onlineRules(EntityType entityType) {
        return onlineIndexRules.stream()
                .filter(index -> index.schema().entityType() == entityType)
                .collect(Collectors.toList());
    }

    /**
     * @return {@link IndexAccessor} for node label index or null
     */
    public IndexAccessor nodeLabelIndex() {
        return nodeLabelIndex;
    }

    /**
     * @return {@link IndexAccessor} for relationship type index or null
     */
    public IndexAccessor relationshipTypeIndex() {
        return relationshipTypeIndex;
    }

    public IndexReaders readers() {
        return new IndexReaders();
    }

    public void remove(IndexDescriptor descriptor) {
        IndexAccessor remove = propertyIndexAccessors.remove(descriptor.getId());
        if (remove != null) {
            remove.close();
        }
        onlineIndexRules.remove(descriptor);
        notOnlineIndexRules.remove(descriptor);
    }

    @Override
    public void close() {
        try {
            MutableList<IndexAccessor> closeables = propertyIndexAccessors.toList();
            closeables.add(nodeLabelIndex);
            closeables.add(relationshipTypeIndex);
            IOUtils.closeAllUnchecked(closeables);
        } finally {
            propertyIndexAccessors.clear();
            onlineIndexRules.clear();
            notOnlineIndexRules.clear();
        }
    }

    public class IndexReaders implements AutoCloseable {
        private final MutableLongObjectMap<ValueIndexReader> readers = new LongObjectHashMap<>();

        public ValueIndexReader reader(IndexDescriptor index) {
            long indexId = index.getId();
            var reader = readers.get(indexId);
            if (reader == null) {
                reader = propertyIndexAccessors.get(indexId).newValueReader(NO_USAGE_TRACKING);
                readers.put(indexId, reader);
            }
            return reader;
        }

        @Override
        public void close() {
            IOUtils.closeAllUnchecked(readers.values());
            readers.clear();
        }
    }

    @FunctionalInterface
    public interface IndexAccessorLookup {
        IndexAccessor apply(IndexDescriptor indexDescriptor) throws IOException;
    }
}
