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
package org.neo4j.internal.recordstorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.store.record.Record.NO_LABELS_FIELD;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;

import org.junit.jupiter.api.Test;
import org.neo4j.internal.counts.RelationshipGroupDegreesStore;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.util.SingleDegree;

class RecordNodeCursorTest {
    @Test
    void shouldConsiderHighestPossibleIdInUseInScan() {
        // given
        NodeStore nodeStore = mock(NodeStore.class);
        IdGenerator idGenerator = mock(IdGenerator.class);
        when(nodeStore.getIdGenerator()).thenReturn(idGenerator);
        when(nodeStore.getHighestPossibleIdInUse(NULL_CONTEXT)).thenReturn(200L);
        when(idGenerator.getHighId()).thenReturn(20L);
        doAnswer(invocationOnMock -> {
                    long id = invocationOnMock.getArgument(0);
                    NodeRecord record = invocationOnMock.getArgument(1);
                    record.setId(id);
                    record.initialize(id == 200, 1L, false, 1L, NO_LABELS_FIELD.longValue());
                    return null;
                })
                .when(nodeStore)
                .getRecordByCursor(anyLong(), any(), any(), any(), any());
        doAnswer(invocationOnMock -> {
                    NodeRecord record = invocationOnMock.getArgument(0);
                    record.setId(record.getId() + 1);
                    record.initialize(record.getId() == 200, 1L, false, 1L, 0L);
                    return null;
                })
                .when(nodeStore)
                .nextRecordByCursor(any(), any(), any(), any());
        RecordNodeCursor cursor = new RecordNodeCursor(
                nodeStore, null, null, null, NULL_CONTEXT, StoreCursors.NULL, EmptyMemoryTracker.INSTANCE);

        // when
        cursor.scan();

        // then
        assertTrue(cursor.next());
        assertEquals(200, cursor.getId());
        assertFalse(cursor.next());
    }

    @Test
    void shouldChooseFastTotalDegreeLookupWhenPossible() {
        // given
        NodeStore nodeStore = mock(NodeStore.class);
        long relationshipId = 99;
        long nextRelationshipId = relationshipId + 1;
        long nodeId = 5;
        int degree = 123;
        when(nodeStore.getHighestPossibleIdInUse(NULL_CONTEXT)).thenReturn(nodeId + 1);
        doAnswer(invocationOnMock -> {
                    long id = invocationOnMock.getArgument(0);
                    NodeRecord record = invocationOnMock.getArgument(1);
                    record.setId(id);
                    record.initialize(
                            true, NULL_REFERENCE.longValue(), false, relationshipId, NO_LABELS_FIELD.longValue());
                    return null;
                })
                .when(nodeStore)
                .getRecordByCursor(eq(nodeId), any(), any(), any(), any());
        RelationshipStore relationshipStore = mock(RelationshipStore.class);
        doAnswer(invocationOnMock -> {
                    long id = invocationOnMock.getArgument(0);
                    RelationshipRecord record = invocationOnMock.getArgument(1);
                    record.setId(id);
                    record.initialize(
                            true,
                            NULL_REFERENCE.longValue(),
                            nodeId,
                            nodeId + 10,
                            1,
                            degree,
                            nextRelationshipId,
                            33,
                            44,
                            true,
                            false);
                    return null;
                })
                .when(relationshipStore)
                .getRecordByCursor(eq(relationshipId), any(), any(), any(), any());
        RelationshipGroupStore groupStore = mock(RelationshipGroupStore.class);
        RelationshipGroupDegreesStore groupDegreesStore = mock(RelationshipGroupDegreesStore.class);
        RecordNodeCursor nodeCursor = new RecordNodeCursor(
                nodeStore,
                relationshipStore,
                groupStore,
                groupDegreesStore,
                NULL_CONTEXT,
                StoreCursors.NULL,
                EmptyMemoryTracker.INSTANCE);

        // when
        nodeCursor.single(nodeId);
        assertThat(nodeCursor.next()).isTrue();
        SingleDegree mutator = new SingleDegree();
        nodeCursor.degrees(RelationshipSelection.ALL_RELATIONSHIPS, mutator);

        // then
        assertThat(mutator.getTotal()).isEqualTo(degree);
        verifyNoInteractions(groupStore);
        verify(relationshipStore).getRecordByCursor(eq(relationshipId), any(), any(), any(), any());
        verify(relationshipStore, never()).getRecordByCursor(eq(nextRelationshipId), any(), any(), any(), any());
    }
}
