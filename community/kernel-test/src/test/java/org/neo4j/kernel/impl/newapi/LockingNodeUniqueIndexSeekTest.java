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
package org.neo4j.kernel.impl.newapi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.exact;
import static org.neo4j.kernel.impl.locking.ResourceIds.indexEntryResourceId;
import static org.neo4j.lock.ResourceType.INDEX_ENTRY;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.kernel.impl.newapi.LockingNodeUniqueIndexSeek.UniqueNodeIndexSeeker;
import org.neo4j.lock.LockTracer;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class LockingNodeUniqueIndexSeekTest {
    private final int labelId = 1;
    private final int propertyKeyId = 2;
    private IndexDescriptor index = IndexPrototype.uniqueForSchema(SchemaDescriptors.forLabel(labelId, propertyKeyId))
            .withName("index_12")
            .materialise(12);

    private final Value value = Values.of("value");
    private final PropertyIndexQuery.ExactPredicate predicate = exact(propertyKeyId, value);
    private final long resourceId = indexEntryResourceId(labelId, predicate);
    private UniqueNodeIndexSeeker<NodeValueIndexCursor> uniqueNodeIndexSeeker = mock(UniqueNodeIndexSeeker.class);

    private final LockManager.Client locks = mock(LockManager.Client.class);
    private final Read read = mock(Read.class);
    private InOrder order;

    @BeforeEach
    void setup() {
        order = inOrder(locks);
    }

    @Test
    void shouldHoldSharedIndexLockIfNodeIsExists() throws Exception {
        // given
        NodeValueIndexCursor cursor = mock(NodeValueIndexCursor.class);
        when(cursor.next()).thenReturn(true);
        when(cursor.nodeReference()).thenReturn(42L);

        // when
        long nodeId = LockingNodeUniqueIndexSeek.apply(
                locks,
                LockTracer.NONE,
                cursor,
                uniqueNodeIndexSeeker,
                read,
                index,
                new PropertyIndexQuery.ExactPredicate[] {predicate});

        // then
        assertEquals(42L, nodeId);
        verify(locks).acquireShared(LockTracer.NONE, INDEX_ENTRY, resourceId);
        verifyNoMoreInteractions(locks);
    }

    @Test
    void shouldHoldSharedIndexLockIfNodeIsConcurrentlyCreated() throws Exception {
        // given
        NodeValueIndexCursor cursor = mock(NodeValueIndexCursor.class);
        when(cursor.next()).thenReturn(false, true);
        when(cursor.nodeReference()).thenReturn(42L);

        // when
        long nodeId = LockingNodeUniqueIndexSeek.apply(
                locks,
                LockTracer.NONE,
                cursor,
                uniqueNodeIndexSeeker,
                read,
                index,
                new PropertyIndexQuery.ExactPredicate[] {predicate});

        // then
        assertEquals(42L, nodeId);
        order.verify(locks).acquireShared(LockTracer.NONE, INDEX_ENTRY, resourceId);
        order.verify(locks).releaseShared(INDEX_ENTRY, resourceId);
        order.verify(locks).acquireExclusive(LockTracer.NONE, INDEX_ENTRY, resourceId);
        order.verify(locks).acquireShared(LockTracer.NONE, INDEX_ENTRY, resourceId);
        order.verify(locks).releaseExclusive(INDEX_ENTRY, resourceId);
        verifyNoMoreInteractions(locks);
    }

    @Test
    void shouldHoldExclusiveIndexLockIfNodeDoesNotExist() throws Exception {
        // given
        NodeValueIndexCursor cursor = mock(NodeValueIndexCursor.class);
        when(cursor.next()).thenReturn(false, false);
        when(cursor.nodeReference()).thenReturn(-1L);

        // when
        long nodeId = LockingNodeUniqueIndexSeek.apply(
                locks,
                LockTracer.NONE,
                cursor,
                uniqueNodeIndexSeeker,
                read,
                index,
                new PropertyIndexQuery.ExactPredicate[] {predicate});

        // then
        assertEquals(-1L, nodeId);
        order.verify(locks).acquireShared(LockTracer.NONE, INDEX_ENTRY, resourceId);
        order.verify(locks).releaseShared(INDEX_ENTRY, resourceId);
        order.verify(locks).acquireExclusive(LockTracer.NONE, INDEX_ENTRY, resourceId);
        verifyNoMoreInteractions(locks);
    }
}
