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
package org.neo4j.consistency.checker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.consistency.checker.NodeIndexChecker.NUM_INDEXES_IN_CACHE;
import static org.neo4j.consistency.checker.ParallelExecution.DEFAULT_IDS_PER_CHUNK;
import static org.neo4j.consistency.checker.ParallelExecution.NOOP_EXCEPTION_HANDLER;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.common.EntityType;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.index.IndexAccessor;

class IndexSizesTest {
    private IndexSizes sizes;
    private final int highNodeId = 10000;
    private final int highRelationshipId = 50000;
    private List<IndexDescriptor> indexes;

    @BeforeEach
    void setUp() {
        ParallelExecution execution = new ParallelExecution(2, NOOP_EXCEPTION_HANDLER, DEFAULT_IDS_PER_CHUNK);

        indexes = new ArrayList<>();
        var indexAccessors = mock(IndexAccessors.class);
        when(indexAccessors.onlineRules(any())).then(invocation -> indexes.stream()
                .filter(index -> index.schema().entityType() == invocation.getArgument(0))
                .collect(Collectors.toList()));
        when(indexAccessors.accessorFor(any())).then(invocation -> {
            IndexAccessor mock = mock(IndexAccessor.class);
            when(mock.estimateNumberOfEntries(any(CursorContext.class)))
                    .thenReturn(invocation.getArgument(0, IndexDescriptor.class).getId());
            return mock;
        });

        sizes = new IndexSizes(
                execution,
                indexAccessors,
                highNodeId,
                highRelationshipId,
                new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER));
    }

    @Test
    void shouldDivideByEntityType() {
        // given
        createIndexes(3, 3, NODE);
        createIndexes(3, 3, RELATIONSHIP);
        // then
        List<IndexDescriptor> largeNodeIndexes = sizes.largeIndexes(NODE);
        List<IndexDescriptor> smallNodeIndexes = sizes.smallIndexes(NODE);
        List<IndexDescriptor> largeRelIndexes = sizes.largeIndexes(RELATIONSHIP);
        List<IndexDescriptor> smallRelIndexes = sizes.smallIndexes(RELATIONSHIP);
        assertEquals(NUM_INDEXES_IN_CACHE, largeNodeIndexes.size());
        assertEquals(6 - NUM_INDEXES_IN_CACHE, smallNodeIndexes.size());
        assertEquals(NUM_INDEXES_IN_CACHE, largeRelIndexes.size());
        assertEquals(6 - NUM_INDEXES_IN_CACHE, smallRelIndexes.size());
    }

    @ParameterizedTest
    @EnumSource(EntityType.class)
    void shouldSplitEvenly(EntityType entityType) {
        // given
        createIndexes(3, 3, entityType);
        // then
        assertEquals(NUM_INDEXES_IN_CACHE, sizes.largeIndexes(entityType).size());
        assertEquals(6 - NUM_INDEXES_IN_CACHE, sizes.smallIndexes(entityType).size());
    }

    @ParameterizedTest
    @EnumSource(EntityType.class)
    void shouldSplitEvenlyLarge(EntityType entityType) {
        // given
        createIndexes(151, 149, entityType);
        // then
        assertEquals(150, sizes.largeIndexes(entityType).size());
        assertEquals(150, sizes.smallIndexes(entityType).size());
    }

    @ParameterizedTest
    @EnumSource(EntityType.class)
    void shouldHandleAllSmall(EntityType entityType) {
        // given
        createIndexes(3, 0, entityType);
        // then
        assertEquals(3, sizes.smallIndexes(entityType).size());
        assertEquals(0, sizes.largeIndexes(entityType).size());
    }

    @ParameterizedTest
    @EnumSource(EntityType.class)
    void shouldHandleAllLarge(EntityType entityType) {
        // given
        createIndexes(0, 3, entityType);
        // then
        assertEquals(0, sizes.smallIndexes(entityType).size());
        assertEquals(3, sizes.largeIndexes(entityType).size());
    }

    @ParameterizedTest
    @EnumSource(EntityType.class)
    void shouldHandleEmpty(EntityType entityType) {
        // then
        createIndexes(0, 0, entityType);
        assertEquals(0, sizes.largeIndexes(entityType).size());
        assertEquals(0, sizes.smallIndexes(entityType).size());
    }

    @ParameterizedTest
    @EnumSource(EntityType.class)
    void shouldNotConsiderIndexWithoutValueCapabilityAsLarge(EntityType entityType) throws Exception {
        // given
        int highId = entityType == NODE ? highNodeId : highRelationshipId;
        indexes.add(prototype(entityType).materialise(highId / 2) /*w/o value capability*/);
        sizes.initialize();

        // when/then
        assertTrue(sizes.largeIndexes(entityType).isEmpty());
        assertEquals(indexes, sizes.smallIndexes(entityType));
    }

    @ParameterizedTest
    @EnumSource(EntityType.class)
    void shouldTreatFulltextAsLargeEvenThoughHasNoValueCapability(EntityType entityType) throws Exception {
        // given
        int highId = entityType == NODE ? highNodeId : highRelationshipId;
        indexes.add(IndexPrototype.forSchema(SchemaDescriptors.fulltext(entityType, new int[] {1}, new int[] {2}))
                .withName("foobar")
                .materialise(highId / 2) /*w/o value capability*/);
        sizes.initialize();

        // when/then
        assertEquals(indexes, sizes.largeIndexes(entityType));
        assertTrue(sizes.smallIndexes(entityType).isEmpty());
    }

    @ParameterizedTest
    @EnumSource(EntityType.class)
    void shouldTreatSmallFulltextAsSmall(EntityType entityType) throws Exception {
        // given
        indexes.add(IndexPrototype.forSchema(SchemaDescriptors.fulltext(entityType, new int[] {1}, new int[] {2}))
                .withName("foobar")
                .materialise(1) /*w/o value capability*/);
        sizes.initialize();

        // when/then
        assertEquals(indexes, sizes.smallIndexes(entityType));
        assertTrue(sizes.largeIndexes(entityType).isEmpty());
    }

    @ParameterizedTest
    @EnumSource(EntityType.class)
    void shouldConsiderLargeNodePointIndexesAsLarge(EntityType entityType) throws Exception {
        // given
        SchemaDescriptor schema =
                entityType == NODE ? SchemaDescriptors.forLabel(1, 2) : SchemaDescriptors.forRelType(1, 2);
        indexes.add(prototype(entityType)
                .withSchemaDescriptor(schema)
                .withIndexType(IndexType.POINT)
                .materialise(highNodeId)
                .withIndexCapability(yes()));
        sizes.initialize();

        // when/then
        assertThat(sizes.largeIndexes(entityType).size()).isEqualTo(1);
        assertThat(sizes.smallIndexes(entityType).size()).isEqualTo(0);
    }

    private void createIndexes(int numSmall, int numLarge, EntityType entityType) {
        IndexCapability capabilityWithValue = yes();
        IndexPrototype prototype = prototype(entityType);

        int highId = entityType == NODE ? highNodeId : highRelationshipId;
        for (int i = 0; i < numLarge; i++) {
            indexes.add(prototype
                    .materialise(highId / 2 + i)
                    .withIndexCapability(capabilityWithValue)); // using id as "size"
        }
        for (int i = 0; i < numSmall; i++) {
            indexes.add(prototype.materialise(i).withIndexCapability(capabilityWithValue)); // using id as "size"
        }

        try {
            sizes.initialize();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private IndexCapability yes() {
        IndexCapability capabilityWithValue = mock(IndexCapability.class);
        when(capabilityWithValue.supportsReturningValues()).thenReturn(true);
        return capabilityWithValue;
    }

    private static IndexPrototype prototype(EntityType entityType) {
        if (entityType == NODE) {
            return IndexPrototype.forSchema(SchemaDescriptors.forLabel(1, 1, 2)).withName("foo");
        }
        return IndexPrototype.forSchema(SchemaDescriptors.forRelType(1, 1, 2)).withName("bar");
    }
}
