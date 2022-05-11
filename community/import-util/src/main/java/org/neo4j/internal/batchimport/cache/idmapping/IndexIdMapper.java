/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.batchimport.cache.idmapping;

import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.exact;
import static org.neo4j.internal.kernel.api.QueryContext.NULL_CONTEXT;
import static org.neo4j.internal.kernel.api.security.AccessMode.Static.FULL;
import static org.neo4j.io.IOUtils.closeAllUnchecked;
import static org.neo4j.kernel.impl.api.index.IndexUpdateMode.ONLINE;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;
import org.neo4j.internal.batchimport.PropertyValueLookup;
import org.neo4j.internal.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.Group;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.index.schema.NodeValueIterator;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.values.storable.Values;

/**
 * Uses an {@link IndexAccessor} for carrying the data for the {@link IdMapper}.
 */
public class IndexIdMapper implements IdMapper {
    private final Map<String, IndexAccessor> accessors;
    private final Map<String, SchemaDescriptor> schemaDescriptors;
    private final ThreadLocal<Map<String, Index>> threadLocal;
    private final List<Index> indexes = new CopyOnWriteArrayList<>();

    // key is groupName, and for some reason accessors doesn't expose which descriptor they're for, so pass that in too
    public IndexIdMapper(Map<String, IndexAccessor> accessors, Map<String, SchemaDescriptor> schemaDescriptors) {
        this.accessors = accessors;
        this.schemaDescriptors = schemaDescriptors;
        this.threadLocal = ThreadLocal.withInitial(HashMap::new);
    }

    @Override
    public void put(Object inputId, long actualId, Group group) {
        try {
            var schemaDescriptor = schemaDescriptors.get(group.name());
            try (var updater = accessors.get(group.name()).newUpdater(ONLINE, CursorContext.NULL_CONTEXT, true)) {
                updater.process(IndexEntryUpdate.add(actualId, () -> schemaDescriptor, Values.of(inputId)));
            }
        } catch (IndexEntryConflictException e) {
            throw new RuntimeException(e);
        }
    }

    private Index index(Group group) {
        return threadLocal.get().computeIfAbsent(group.name(), groupName -> {
            var accessor = accessors.get(groupName);
            var reader = accessor.newValueReader();
            var schemaDescriptor = schemaDescriptors.get(groupName);
            var index = new Index(reader, schemaDescriptor);
            indexes.add(index);
            return index;
        });
    }

    @Override
    public boolean needsPreparation() {
        return false;
    }

    @Override
    public void prepare(PropertyValueLookup inputIdLookup, Collector collector, ProgressListener progress) {}

    @Override
    public long get(Object inputId, Group group) {
        try {
            // TODO somehow reuse client/progressor per thread?
            try (var client = new NodeValueIterator()) {
                // TODO do we need a proper QueryContext?
                var index = index(group);
                index.reader.query(
                        client,
                        NULL_CONTEXT,
                        FULL,
                        unconstrained(),
                        exact(index.schemaDescriptor.getPropertyId(), inputId));
                return client.hasNext() ? client.next() : -1;
            }
        } catch (IndexNotApplicableKernelException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        closeAllUnchecked(
                (Closeable) () -> closeAllUnchecked(indexes),
                () -> {
                    for (var accessor : accessors.values()) {
                        accessor.force(CursorContext.NULL_CONTEXT);
                    }
                },
                () -> closeAllUnchecked(accessors.values()));
    }

    @Override
    public LongIterator leftOverDuplicateNodesIds() {
        return ImmutableEmptyLongIterator.INSTANCE;
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
}
