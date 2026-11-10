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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.token.api.TokenConstants.ANY_LABEL;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.graphdb.TransactionTerminatedHelper;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.cursor.StoreCursors;

class EntityCounterTest {
    private static final int CHECK_PERIOD = 100_000;
    private static final int LABEL = 1;
    private static final int REL_TYPE = 2;

    private final StorageReader storageReader = mock(StorageReader.class);
    private final CursorFactory cursors = mock(CursorFactory.class);
    private final Read read = mock(Read.class);
    private final SchemaRead schemaRead = mock(SchemaRead.class);
    private final StoreCursors storeCursors = mock(StoreCursors.class);
    private final TxStateHolder txStateHolder = mock(TxStateHolder.class);

    @Test
    void shouldStopNodeScanWhenTransactionIsTerminated() {
        MutableInt scanned = new MutableInt();
        NodeCursor nodes = nodeCursor(scanned);
        when(cursors.allocateNodeCursor(any(), any())).thenReturn(nodes);

        EntityCounter counter = new EntityCounter(false, () -> {
            throw TransactionTerminatedHelper.transactionTerminated(Status.Transaction.Terminated);
        });

        assertThatThrownBy(() -> countsForNode(counter, mock(AccessMode.class)))
                .isInstanceOf(TransactionTerminatedException.class);

        assertThat(scanned.intValue()).isEqualTo(CHECK_PERIOD); // we should check for termination every 100_000 nodes
        verify(nodes).close();
    }

    @Test
    void shouldStopRelationshipScanWhenTransactionIsTerminated() {
        MutableInt scanned = new MutableInt();
        RelationshipScanCursor relationships = relationshipCursor(scanned);
        when(cursors.allocateRelationshipScanCursor(any(), any())).thenReturn(relationships);
        when(cursors.allocateFullAccessNodeCursor(any(), any())).thenAnswer(invocation -> nodeCursor(new MutableInt()));

        EntityCounter counter = new EntityCounter(false, () -> {
            throw TransactionTerminatedHelper.transactionTerminated(Status.Transaction.Terminated);
        });

        assertThatThrownBy(() -> countsForRelationship(counter, mock(AccessMode.class)))
                .isInstanceOf(TransactionTerminatedException.class);

        assertThat(scanned.intValue()).isEqualTo(CHECK_PERIOD); // we should check for termination every 100_000 rels
        verify(relationships).close();
    }

    @Test
    void shouldNotCheckForTerminationOnTheCountStorePath() {
        AccessMode accessMode = mock(AccessMode.class);
        when(accessMode.allowsTraverseAllNodesWithLabel(anyInt())).thenReturn(true);
        when(storageReader.countsForNode(anyInt(), any())).thenReturn(42L);

        MutableInt assertOpenCalls = new MutableInt();
        EntityCounter counter = new EntityCounter(false, assertOpenCalls::increment);

        countsForNode(counter, accessMode);
        verify(cursors, never()).allocateNodeCursor(any(), any());
        assertThat(assertOpenCalls.intValue()).isZero();
    }

    @Test
    void shouldUseCountStoreForRelationshipsWhenAllRelationshipsAndNodesCanBeTraversed() {
        AccessMode accessMode = mock(AccessMode.class);
        when(accessMode.allowsTraverseAllRelsWithType(REL_TYPE)).thenReturn(true);
        when(accessMode.allowsTraverseAllNodesWithLabel(LABEL)).thenReturn(true);
        when(accessMode.allowsTraverseAllLabels()).thenReturn(true);
        when(storageReader.countsForRelationship(anyInt(), anyInt(), anyInt(), any()))
                .thenReturn(42L);

        EntityCounter counter = new EntityCounter(false, () -> {});

        assertThat(countsForRelationship(counter, accessMode, LABEL, REL_TYPE, ANY_LABEL))
                .isEqualTo(42L);
        verify(storageReader).countsForRelationship(LABEL, REL_TYPE, ANY_LABEL, CursorContext.NULL_CONTEXT);
        verify(cursors, never()).allocateRelationshipScanCursor(any(), any());
    }

    @Test
    void shouldNotUseCountStoreForRelationshipsWhenOnlySomeRelationshipsWithTypeCanBeTraversed() {
        // allowsTraverseRelType/allowsTraverseNode only say that *some* entities may be traversable, e.g. a label
        // or type grant combined with a property-based deny, or a custom AccessMode that filters per entity.
        AccessMode accessMode = permissiveForSomeEntities();
        when(accessMode.allowsTraverseAllNodesWithLabel(anyInt())).thenReturn(true);
        when(accessMode.allowsTraverseAllLabels()).thenReturn(true);
        when(accessMode.allowsTraverseAllRelsWithType(REL_TYPE)).thenReturn(false);

        assertCountedByScan(accessMode, ANY_LABEL, REL_TYPE, ANY_LABEL);
    }

    @Test
    void shouldNotUseCountStoreForRelationshipsWhenOnlySomeNodesWithLabelCanBeTraversed() {
        AccessMode accessMode = permissiveForSomeEntities();
        when(accessMode.allowsTraverseAllRelsWithType(REL_TYPE)).thenReturn(true);
        when(accessMode.allowsTraverseAllLabels()).thenReturn(true);
        when(accessMode.allowsTraverseAllNodesWithLabel(LABEL)).thenReturn(false);

        assertCountedByScan(accessMode, LABEL, REL_TYPE, ANY_LABEL);
        assertCountedByScan(accessMode, ANY_LABEL, REL_TYPE, LABEL);
    }

    @Test
    void shouldNotUseCountStoreForAnyRelationshipTypeWhenNotAllRelationshipsCanBeTraversed() {
        AccessMode accessMode = permissiveForSomeEntities();
        when(accessMode.allowsTraverseAllRelsWithType(anyInt())).thenReturn(true);
        when(accessMode.allowsTraverseAllNodesWithLabel(anyInt())).thenReturn(true);
        when(accessMode.allowsTraverseAllLabels()).thenReturn(true);
        when(accessMode.allowsTraverseAllRelTypes()).thenReturn(false);

        assertCountedByScan(accessMode, ANY_LABEL, ANY_RELATIONSHIP_TYPE, ANY_LABEL);
    }

    @Test
    void shouldNotUseCountStoreForAnyLabelWhenNotAllNodesCanBeTraversed() {
        AccessMode accessMode = permissiveForSomeEntities();
        when(accessMode.allowsTraverseAllRelsWithType(anyInt())).thenReturn(true);
        when(accessMode.allowsTraverseAllNodesWithLabel(anyInt())).thenReturn(true);
        when(accessMode.allowsTraverseAllRelTypes()).thenReturn(true);
        when(accessMode.allowsTraverseAllLabels()).thenReturn(false);

        assertCountedByScan(accessMode, ANY_LABEL, REL_TYPE, ANY_LABEL);
    }

    /**
     * An access mode that says some (but not necessarily all) nodes and relationships with any token can be
     * traversed, and denies nothing outright.
     */
    private static AccessMode permissiveForSomeEntities() {
        AccessMode accessMode = mock(AccessMode.class);
        when(accessMode.allowsTraverseRelType(anyInt())).thenReturn(true);
        when(accessMode.allowsTraverseNode(anyInt())).thenReturn(true);
        return accessMode;
    }

    private void assertCountedByScan(AccessMode accessMode, int startLabel, int type, int endLabel) {
        when(cursors.allocateRelationshipScanCursor(any(), any())).thenReturn(mock(RelationshipScanCursor.class));
        when(cursors.allocateFullAccessNodeCursor(any(), any())).thenReturn(mock(NodeCursor.class));
        when(storageReader.countsForRelationship(anyInt(), anyInt(), anyInt(), any()))
                .thenReturn(42L);

        EntityCounter counter = new EntityCounter(false, () -> {});

        assertThat(countsForRelationship(counter, accessMode, startLabel, type, endLabel))
                .isZero();
        verify(storageReader, never()).countsForRelationship(anyInt(), anyInt(), anyInt(), any());
        verify(read, atLeastOnce()).allRelationshipsScan(any());
    }

    private void countsForNode(EntityCounter counter, AccessMode accessMode) {
        counter.countsForNode(
                ANY_LABEL,
                accessMode,
                storageReader,
                cursors,
                CursorContext.NULL_CONTEXT,
                EmptyMemoryTracker.INSTANCE,
                read,
                storeCursors,
                txStateHolder);
    }

    private void countsForRelationship(EntityCounter counter, AccessMode accessMode) {
        countsForRelationship(counter, accessMode, ANY_LABEL, ANY_RELATIONSHIP_TYPE, ANY_LABEL);
    }

    private long countsForRelationship(
            EntityCounter counter, AccessMode accessMode, int startLabel, int type, int endLabel) {
        return counter.countsForRelationship(
                startLabel,
                type,
                endLabel,
                accessMode,
                storageReader,
                cursors,
                read,
                CursorContext.NULL_CONTEXT,
                EmptyMemoryTracker.INSTANCE,
                storeCursors,
                schemaRead,
                txStateHolder);
    }

    private static NodeCursor nodeCursor(MutableInt scanned) {
        NodeCursor cursor = mock(NodeCursor.class);
        when(cursor.next()).thenAnswer(invocation -> {
            scanned.incrementAndGet();
            return true;
        });
        return cursor;
    }

    private static RelationshipScanCursor relationshipCursor(MutableInt scanned) {
        RelationshipScanCursor cursor = mock(RelationshipScanCursor.class);
        when(cursor.next()).thenAnswer(invocation -> {
            scanned.incrementAndGet();
            return true;
        });
        return cursor;
    }
}
