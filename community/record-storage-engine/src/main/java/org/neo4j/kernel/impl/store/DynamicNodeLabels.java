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

import static java.lang.String.format;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.DYNAMIC_LABEL_STORE_CURSOR;
import static org.neo4j.kernel.impl.store.AbstractDynamicStore.readFullByteArrayFromHeavyRecords;
import static org.neo4j.kernel.impl.store.LabelIdArray.filter;
import static org.neo4j.kernel.impl.store.NodeLabelsField.fieldPointsToDynamicRecordOfLabels;
import static org.neo4j.kernel.impl.store.NodeLabelsField.firstDynamicLabelRecordId;
import static org.neo4j.kernel.impl.store.NodeLabelsField.parseLabelsBody;
import static org.neo4j.kernel.impl.store.PropertyType.ARRAY;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.allocator.ReusableRecordsCompositeAllocator;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;

public class DynamicNodeLabels implements NodeLabels {
    private final NodeRecord node;

    public DynamicNodeLabels(NodeRecord node) {
        this.node = node;
    }

    @Override
    public int[] get(NodeStore nodeStore, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        return get(node, nodeStore, storeCursors, memoryTracker);
    }

    public static int[] get(
            NodeRecord node, NodeStore nodeStore, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        nodeStore.ensureHeavy(node, firstDynamicLabelRecordId(node.getLabelField()), storeCursors, memoryTracker);
        var usedLabels = node.getUsedDynamicLabelRecords();
        if (usedLabels.isEmpty()) {
            return ArrayUtils.EMPTY_INT_ARRAY;
        }
        return getDynamicLabelsArray(usedLabels, nodeStore.getDynamicLabelStore(), storeCursors, memoryTracker);
    }

    public static boolean hasLabel(
            NodeRecord node, NodeStore nodeStore, StoreCursors storeCursors, int label, MemoryTracker memoryTracker) {
        DynamicArrayStore dynamicLabelStore = nodeStore.getDynamicLabelStore();
        HasLabelSubscriber subscriber = new HasLabelSubscriber(label, dynamicLabelStore, storeCursors, memoryTracker);
        if (node.isLight()) {
            // dynamic records not there, stream the result from the dynamic label store
            dynamicLabelStore.streamRecords(
                    firstDynamicLabelRecordId(node.getLabelField()),
                    RecordLoad.NORMAL,
                    false,
                    storeCursors.readCursor(DYNAMIC_LABEL_STORE_CURSOR),
                    subscriber,
                    memoryTracker);
        } else {
            // dynamic records are already here, lets use them
            for (DynamicRecord record : node.getUsedDynamicLabelRecords()) {
                if (!subscriber.onRecord(record)) {
                    break;
                }
            }
        }
        return subscriber.hasLabel();
    }

    @Override
    public Collection<DynamicRecord> put(
            int[] labelIds,
            NodeStore nodeStore,
            DynamicRecordAllocator allocator,
            CursorContext cursorContext,
            StoreCursors storeCursors,
            MemoryTracker memoryTracker) {
        Arrays.sort(labelIds);
        return putSorted(node, labelIds, nodeStore, allocator, cursorContext, storeCursors, memoryTracker);
    }

    static Collection<DynamicRecord> putSorted(
            NodeRecord node,
            int[] labelIds,
            NodeStore nodeStore,
            DynamicRecordAllocator allocator,
            CursorContext cursorContext,
            StoreCursors storeCursors,
            MemoryTracker memoryTracker) {
        long existingLabelsField = node.getLabelField();
        long existingLabelsBits = parseLabelsBody(existingLabelsField);

        List<DynamicRecord> changedDynamicRecords = node.getDynamicLabelRecords();

        long labelField = node.getLabelField();
        if (fieldPointsToDynamicRecordOfLabels(labelField)) {
            // There are existing dynamic label records, get them
            nodeStore.ensureHeavy(node, existingLabelsBits, storeCursors, memoryTracker);
            changedDynamicRecords = node.getDynamicLabelRecords();
            setNotInUse(changedDynamicRecords);
        }

        if (!InlineNodeLabels.tryInlineInNodeRecord(node, labelIds, changedDynamicRecords)) {
            Iterator<DynamicRecord> recycledRecords = changedDynamicRecords.iterator();
            List<DynamicRecord> allocatedRecords = allocateRecordsForDynamicLabels(
                    node.getId(),
                    labelIds,
                    new ReusableRecordsCompositeAllocator(recycledRecords, allocator),
                    cursorContext,
                    memoryTracker);
            // Set the rest of the previously set dynamic records as !inUse
            while (recycledRecords.hasNext()) {
                DynamicRecord removedRecord = recycledRecords.next();
                removedRecord.setInUse(false);
                allocatedRecords.add(removedRecord);
            }
            node.setLabelField(dynamicPointer(allocatedRecords), allocatedRecords);
            changedDynamicRecords = allocatedRecords;
        }

        return changedDynamicRecords;
    }

    @Override
    public Collection<DynamicRecord> add(
            int labelId,
            NodeStore nodeStore,
            DynamicRecordAllocator allocator,
            CursorContext cursorContext,
            StoreCursors storeCursors,
            MemoryTracker memoryTracker) {
        nodeStore.ensureHeavy(node, firstDynamicLabelRecordId(node.getLabelField()), storeCursors, memoryTracker);
        int[] existingLabelIds = getDynamicLabelsArray(
                node.getUsedDynamicLabelRecords(), nodeStore.getDynamicLabelStore(), storeCursors, memoryTracker);
        int[] newLabelIds = LabelIdArray.concatAndSort(existingLabelIds, labelId);
        Collection<DynamicRecord> existingRecords = node.getDynamicLabelRecords();
        List<DynamicRecord> changedDynamicRecords = allocateRecordsForDynamicLabels(
                node.getId(),
                newLabelIds,
                new ReusableRecordsCompositeAllocator(existingRecords, allocator),
                cursorContext,
                memoryTracker);
        node.setLabelField(dynamicPointer(changedDynamicRecords), changedDynamicRecords);
        return changedDynamicRecords;
    }

    @Override
    public Collection<DynamicRecord> remove(
            int labelId,
            NodeStore nodeStore,
            DynamicRecordAllocator allocator,
            CursorContext cursorContext,
            StoreCursors storeCursors,
            MemoryTracker memoryTracker) {
        nodeStore.ensureHeavy(node, firstDynamicLabelRecordId(node.getLabelField()), storeCursors, memoryTracker);
        int[] existingLabelIds = getDynamicLabelsArray(
                node.getUsedDynamicLabelRecords(), nodeStore.getDynamicLabelStore(), storeCursors, memoryTracker);
        int[] newLabelIds = filter(existingLabelIds, labelId);
        List<DynamicRecord> existingRecords = node.getDynamicLabelRecords();
        if (InlineNodeLabels.tryInlineInNodeRecord(node, newLabelIds, existingRecords)) {
            setNotInUse(existingRecords);
        } else {
            Collection<DynamicRecord> newRecords = allocateRecordsForDynamicLabels(
                    node.getId(),
                    newLabelIds,
                    new ReusableRecordsCompositeAllocator(existingRecords, allocator),
                    cursorContext,
                    memoryTracker);
            node.setLabelField(dynamicPointer(newRecords), existingRecords);
            if (!newRecords.equals(existingRecords)) { // One less dynamic record, mark that one as not in use
                for (DynamicRecord record : existingRecords) {
                    if (!newRecords.contains(record)) {
                        record.setInUse(false);
                    }
                }
            }
        }
        return existingRecords;
    }

    public static long dynamicPointer(Collection<DynamicRecord> newRecords) {
        return dynamicPointer(Iterables.first(newRecords).getId());
    }

    public static long dynamicPointer(long dynamicRecordId) {
        return 0x8000000000L | dynamicRecordId;
    }

    private static void setNotInUse(Collection<DynamicRecord> changedDynamicRecords) {
        for (DynamicRecord record : changedDynamicRecords) {
            record.setInUse(false);
        }
    }

    @Override
    public String toString() {
        if (node.isLight()) {
            return format("Dynamic(id:%d)", firstDynamicLabelRecordId(node.getLabelField()));
        }
        return format(
                "Dynamic(id:%d,[%s])",
                firstDynamicLabelRecordId(node.getLabelField()),
                Arrays.toString(parseHeavyRecords(node.getUsedDynamicLabelRecords())));
    }

    public static List<DynamicRecord> allocateRecordsForDynamicLabels(
            long nodeId,
            int[] labels,
            DynamicRecordAllocator allocator,
            CursorContext cursorContext,
            MemoryTracker memoryTracker) {
        long[] storedLongs = LabelIdArray.prependNodeId(nodeId, labels);
        List<DynamicRecord> records = new ArrayList<>();
        DynamicArrayStore.allocateRecords(records, storedLongs, allocator, cursorContext, memoryTracker);
        return records;
    }

    public static int[] getDynamicLabelsArray(
            Iterable<DynamicRecord> records,
            DynamicArrayStore dynamicLabelStore,
            StoreCursors storeCursors,
            MemoryTracker memoryTracker) {
        long[] storedLongs = (long[]) dynamicLabelStore
                .getArrayFor(records, storeCursors, memoryTracker)
                .asObject();
        return LabelIdArray.stripNodeId(storedLongs);
    }

    private static int[] parseHeavyRecords(Iterable<DynamicRecord> records) {
        var heavyRecordData = readFullByteArrayFromHeavyRecords(records, ARRAY);
        long[] storedLongs =
                (long[]) DynamicArrayStore.getNumbersArray(heavyRecordData.header(), heavyRecordData.data())
                        .asObject();
        return LabelIdArray.stripNodeId(storedLongs);
    }
}
