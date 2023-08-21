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
package org.neo4j.kernel.impl.store;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.collection.Iterables.asList;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.store.DynamicNodeLabels.allocateRecordsForDynamicLabels;
import static org.neo4j.kernel.impl.store.DynamicNodeLabels.dynamicPointer;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.id.IdSequence;
import org.neo4j.kernel.impl.store.allocator.ReusableRecordsAllocator;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;

class NodeRecordTest {
    @Test
    void cloneShouldProduceExactCopy() {
        // Given
        long relId = 1337L;
        long propId = 1338L;
        long inlinedLabels = 12L;

        NodeRecord node = new NodeRecord(1L).initialize(false, propId, false, relId, 0);
        node.setLabelField(inlinedLabels, asList(new DynamicRecord(1L), new DynamicRecord(2L)));
        node.setInUse(true);

        // When
        NodeRecord clone = new NodeRecord(node);

        // Then
        assertEquals(node.inUse(), clone.inUse());
        assertEquals(node.getLabelField(), clone.getLabelField());
        assertEquals(node.getNextProp(), clone.getNextProp());
        assertEquals(node.getNextRel(), clone.getNextRel());

        assertThat(clone.getDynamicLabelRecords()).isEqualTo(node.getDynamicLabelRecords());
    }

    @Test
    void shouldListLabelRecordsInUse() {
        // Given
        NodeRecord node = new NodeRecord(1).initialize(false, -1, false, -1, 0);
        long inlinedLabels = 12L;
        DynamicRecord dynamic1 = new DynamicRecord(1L).initialize(true, true, Record.NO_NEXT_BLOCK.intValue(), -1);
        DynamicRecord dynamic2 = new DynamicRecord(2L).initialize(true, true, Record.NO_NEXT_BLOCK.intValue(), -1);
        DynamicRecord dynamic3 = new DynamicRecord(3L).initialize(true, true, Record.NO_NEXT_BLOCK.intValue(), -1);

        node.setLabelField(inlinedLabels, asList(dynamic1, dynamic2, dynamic3));

        dynamic3.setInUse(false);

        // When
        Iterable<DynamicRecord> usedRecords = node.getUsedDynamicLabelRecords();

        // Then
        assertThat(asList(usedRecords)).isEqualTo(asList(dynamic1, dynamic2));
    }

    @Test
    void shouldToStringBothUsedAndUnusedDynamicLabelRecords() {
        // GIVEN
        IdSequence ids = mock(IdSequence.class);
        when(ids.nextId(NULL_CONTEXT)).thenReturn(1L, 2L);
        ReusableRecordsAllocator recordAllocator =
                new ReusableRecordsAllocator(30, new DynamicRecord(1), new DynamicRecord(2));
        NodeRecord node = newUsedNodeRecord(0);
        long labelId = 10_123;
        // A dynamic label record
        List<DynamicRecord> existing = allocateRecordsForDynamicLabels(
                node.getId(), new long[] {labelId}, recordAllocator, NULL_CONTEXT, INSTANCE);
        // and a deleted one as well (simulating some deleted labels)
        DynamicRecord unused = newDeletedDynamicRecord(ids.nextId(NULL_CONTEXT));
        unused.setInUse(false);
        existing.add(unused);
        node.setLabelField(dynamicPointer(existing), existing);

        // WHEN
        String toString = node.toString();

        // THEN
        assertThat(toString).contains(String.valueOf(labelId));
        assertThat(toString).contains(unused.toString());
    }

    private static DynamicRecord newDeletedDynamicRecord(long id) {
        DynamicRecord record = new DynamicRecord(id);
        record.setInUse(false);
        return record;
    }

    private static NodeRecord newUsedNodeRecord(long id) {
        NodeRecord node = new NodeRecord(id);
        node.setInUse(true);
        return node;
    }
}
