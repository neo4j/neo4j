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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.exact;
import static org.neo4j.kernel.impl.locking.ResourceIds.indexEntryResourceId;
import static org.neo4j.lock.ResourceType.INDEX_ENTRY;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.neo4j.internal.kernel.api.EntityLocks;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AccessMode.Static;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.lock.LockTracer;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.StorageLocks;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class LockingNodeUniqueIndexSeekTest {
    private final int labelId = 1;
    private final int propertyKeyId = 2;
    private final IndexDescriptor index = IndexPrototype.uniqueForSchema(
                    SchemaDescriptors.forLabel(labelId, propertyKeyId))
            .withName("index_12")
            .materialise(12);

    private final Value value = Values.of("value");
    private final PropertyIndexQuery.ExactPredicate predicate = exact(propertyKeyId, value);
    private final long resourceId = indexEntryResourceId(labelId, predicate);

    private final LockManager.Client locks = mock(LockManager.Client.class);
    private InOrder order;

    @BeforeEach
    void setup() {
        order = inOrder(locks);
    }

    @Test
    void shouldHoldSharedIndexLockIfNodeIsExists() throws Exception {
        var cursor = mock(DefaultNodeValueIndexCursor.class);
        when(cursor.next()).thenReturn(true);
        when(cursor.nodeReference()).thenReturn(42L);

        var allStoreHolder = createMockedRead();
        long nodeId = allStoreHolder.lockingNodeUniqueIndexSeek(index, cursor, predicate);

        assertEquals(42L, nodeId);
        verify(locks).acquireShared(LockTracer.NONE, INDEX_ENTRY, resourceId);
    }

    @Test
    void shouldHoldSharedIndexLockIfNodeIsConcurrentlyCreated() throws Exception {
        var cursor = mock(DefaultNodeValueIndexCursor.class);
        when(cursor.next()).thenReturn(false, true);
        when(cursor.nodeReference()).thenReturn(42L);

        var allStoreHolder = createMockedRead();
        long nodeId = allStoreHolder.lockingNodeUniqueIndexSeek(index, cursor, predicate);

        assertEquals(42L, nodeId);
        order.verify(locks).acquireShared(LockTracer.NONE, INDEX_ENTRY, resourceId);
        order.verify(locks).releaseShared(INDEX_ENTRY, resourceId);
        order.verify(locks).acquireExclusive(LockTracer.NONE, INDEX_ENTRY, resourceId);
        order.verify(locks).acquireShared(LockTracer.NONE, INDEX_ENTRY, resourceId);
        order.verify(locks).releaseExclusive(INDEX_ENTRY, resourceId);
    }

    @Test
    void shouldHoldExclusiveIndexLockIfNodeDoesNotExist() throws Exception {
        var cursor = mock(DefaultNodeValueIndexCursor.class);
        when(cursor.next()).thenReturn(false, false);
        when(cursor.nodeReference()).thenReturn(-1L);

        var allStoreHolder = createMockedRead();
        long nodeId = allStoreHolder.lockingNodeUniqueIndexSeek(index, cursor, predicate);

        assertEquals(-1L, nodeId);
        order.verify(locks).acquireShared(LockTracer.NONE, INDEX_ENTRY, resourceId);
        order.verify(locks).releaseShared(INDEX_ENTRY, resourceId);
        order.verify(locks).acquireExclusive(LockTracer.NONE, INDEX_ENTRY, resourceId);
    }

    private AllStoreHolder createMockedRead() throws IndexNotFoundKernelException {
        IndexingService indexingService = mock(IndexingService.class);
        IndexProxy indexProxy = mock(IndexProxy.class);
        when(indexProxy.getState()).thenReturn(InternalIndexState.ONLINE);
        when(indexProxy.newValueReader()).thenReturn(mock(ValueIndexReader.class));
        when(indexingService.getIndexProxy(any())).thenReturn(indexProxy);
        return new AllStoreHolder(
                mock(StorageReader.class),
                mock(TokenRead.class),
                mock(SchemaState.class),
                indexingService,
                mock(IndexStatisticsStore.class),
                EmptyMemoryTracker.INSTANCE,
                mock(DefaultPooledCursors.class),
                mock(StoreCursors.class),
                new EntityLocks(mock(StorageLocks.class), () -> LockTracer.NONE, locks, () -> {}),
                false,
                QueryContext.NULL_CONTEXT,
                mock(TxStateHolder.class)) {

            @Override
            void performCheckBeforeOperation() {}

            @Override
            AccessMode getAccessMode() {
                return Static.FULL;
            }
        };
    }
}
