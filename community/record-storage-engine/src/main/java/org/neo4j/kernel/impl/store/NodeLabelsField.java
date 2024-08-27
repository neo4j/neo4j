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

import static org.neo4j.internal.recordstorage.RecordCursorTypes.DYNAMIC_LABEL_STORE_CURSOR;

import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;

/**
 * Logic for parsing and constructing {@link NodeRecord#getLabelField()} and dynamic label
 * records in {@link NodeRecord#getDynamicLabelRecords()} from label ids.
 * <p>
 * Each node has a label field of 5 bytes, where labels will be stored, if sufficient space
 * (max bits required for storing each label id is considered). If not then the field will
 * point to a dynamic record where the labels will be stored in the format of an array property.
 * <p>
 * [hhhh,bbbb][bbbb,bbbb][bbbb,bbbb][bbbb,bbbb][bbbb,bbbb]
 * h: header
 * - 0x0<=h<=0x7 (leaving high bit reserved): number of in-lined labels in the body
 * - 0x8: body will be a pointer to first dynamic record in node-labels dynamic store
 * b: body
 * - 0x0<=h<=0x7 (leaving high bit reserved): bits of this many in-lined label ids
 * - 0x8: pointer to node-labels store
 */
public class NodeLabelsField {
    private NodeLabelsField() {}

    public static NodeLabels parseLabelsField(NodeRecord node) {
        long labelField = node.getLabelField();
        return fieldPointsToDynamicRecordOfLabels(labelField)
                ? new DynamicNodeLabels(node)
                : new InlineNodeLabels(node);
    }

    /**
     * Get node labels without making node heavy
     */
    public static int[] getNoEnsureHeavy(
            NodeRecord node, NodeStore nodeStore, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        long labelField = node.getLabelField();
        if (!fieldPointsToDynamicRecordOfLabels(labelField)) {
            return InlineNodeLabels.parseInlined(labelField);
        }
        var dynamicLabelStore = nodeStore.getDynamicLabelStore();
        Iterable<DynamicRecord> dynamicLabelRecords;
        if (node.isLight()) {
            // labelField points to dynamic labels but records are not loaded, load them without updating node itself
            var firstDynamicLabelRecord = firstDynamicLabelRecordId(labelField);
            dynamicLabelRecords = dynamicLabelStore.getRecords(
                    firstDynamicLabelRecord,
                    RecordLoad.NORMAL,
                    false,
                    storeCursors.readCursor(DYNAMIC_LABEL_STORE_CURSOR),
                    memoryTracker);
        } else {
            dynamicLabelRecords = node.getUsedDynamicLabelRecords();
        }
        return DynamicNodeLabels.getDynamicLabelsArray(
                dynamicLabelRecords, dynamicLabelStore, storeCursors, memoryTracker);
    }

    public static int[] get(
            NodeRecord node, NodeStore nodeStore, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        return fieldPointsToDynamicRecordOfLabels(node.getLabelField())
                ? DynamicNodeLabels.get(node, nodeStore, storeCursors, memoryTracker)
                : InlineNodeLabels.get(node);
    }

    public static boolean hasLabel(
            NodeRecord node, NodeStore nodeStore, StoreCursors storeCursors, int label, MemoryTracker memoryTracker) {
        return fieldPointsToDynamicRecordOfLabels(node.getLabelField())
                ? DynamicNodeLabels.hasLabel(node, nodeStore, storeCursors, label, memoryTracker)
                : InlineNodeLabels.hasLabel(node, label);
    }

    public static boolean fieldPointsToDynamicRecordOfLabels(long labelField) {
        return (labelField & 0x8000000000L) != 0;
    }

    public static long parseLabelsBody(long labelField) {
        return labelField & 0xFFFFFFFFFL;
    }

    /**
     * @see NodeRecord
     *
     * @param labelField label field value from a node record
     * @return the id of the dynamic record this label field points to or null if it is an inline label field
     */
    public static long firstDynamicLabelRecordId(long labelField) {
        assert fieldPointsToDynamicRecordOfLabels(labelField);
        return parseLabelsBody(labelField);
    }
}
