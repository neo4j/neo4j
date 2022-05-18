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

import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.index.schema.PartitionedValueSeek;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;

class IndexIdMapperTest {
    private final Groups groups = new Groups();
    private final Map<String, IndexAccessor> accessors = new HashMap<>();
    private final Map<String, SchemaDescriptor> descriptors = new HashMap<>();
    private IndexIdMapper idMapper;

    @AfterEach
    void close() {
        idMapper.close();
    }

    @Test
    void shouldGetAndPutOnSingleIndex() {
        // given
        var group = groups.getOrCreate("group");
        addAccessor(group.name());
        idMapper = new IndexIdMapper(accessors, descriptors, PageCacheTracer.NULL);

        // when
        idMapper.put("myId", 2, group);

        // then
        assertThat(idMapper.get("myId", group)).isEqualTo(2);
    }

    @Test
    void shouldGetAndPutOnMultipleIndexes() {
        // given
        var group1 = groups.getOrCreate("group1");
        var group2 = groups.getOrCreate("group2");
        addAccessor(group1.name());
        addAccessor(group2.name());
        idMapper = new IndexIdMapper(accessors, descriptors, PageCacheTracer.NULL);

        // when
        idMapper.put("myId", 2, group1);
        idMapper.put("myId", 5, group2);

        // then
        assertThat(idMapper.get("myId", group1)).isEqualTo(2);
        assertThat(idMapper.get("myId", group2)).isEqualTo(5);
    }

    private void addAccessor(String group) {
        var accessor = new IndexAccessor.Adapter() {
            private final Map<String, Long> data = new HashMap<>();

            @Override
            public IndexUpdater newUpdater(IndexUpdateMode mode, CursorContext cursorContext, boolean parallel) {
                return new IndexUpdater() {
                    @Override
                    public void process(IndexEntryUpdate<?> update) {
                        var value = ((TextValue) asValueUpdate(update).values()[0]).stringValue();
                        var nodeId = update.getEntityId();
                        data.put(value, nodeId);
                    }

                    @Override
                    public void close() {}
                };
            }

            @Override
            public ValueIndexReader newValueReader() {
                return new ValueIndexReader() {
                    @Override
                    public long countIndexedEntities(
                            long entityId, CursorContext cursorContext, int[] propertyKeyIds, Value... propertyValues) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public IndexSampler createSampler() {
                        return IndexSampler.EMPTY;
                    }

                    @Override
                    public void query(
                            IndexProgressor.EntityValueClient client,
                            QueryContext context,
                            AccessMode accessMode,
                            IndexQueryConstraints constraints,
                            PropertyIndexQuery... query) {
                        var value = ((TextValue) ((PropertyIndexQuery.ExactPredicate) query[0]).value()).stringValue();
                        var entityId = data.get(value);
                        var progressor = new SimpleIndexProgressor(
                                entityId != null ? new long[] {entityId} : EMPTY_LONG_ARRAY, client);
                        client.initialize(
                                null,
                                progressor,
                                AccessMode.Static.FULL,
                                false,
                                IndexQueryConstraints.unconstrained(),
                                query);
                    }

                    @Override
                    public PartitionedValueSeek valueSeek(
                            int desiredNumberOfPartitions, QueryContext queryContext, PropertyIndexQuery... query) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void close() {}
                };
            }
        };
        accessors.put(group, accessor);
        descriptors.put(group, SchemaDescriptors.forLabel(0, 0));
    }

    private static class SimpleIndexProgressor implements IndexProgressor {
        private final long[] entityIds;
        private final EntityValueClient client;
        private int cursor;

        SimpleIndexProgressor(long[] entityIds, EntityValueClient client) {
            this.entityIds = entityIds;
            this.client = client;
        }

        @Override
        public boolean next() {
            if (cursor < entityIds.length) {
                client.acceptEntity(entityIds[cursor++], 0, (Value[]) null);
                return true;
            }
            return false;
        }

        @Override
        public void close() {}
    }
}
