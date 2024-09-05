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

import static java.lang.Long.highestOneBit;
import static java.lang.String.format;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.neo4j.kernel.impl.store.LabelIdArray.concatAndSort;
import static org.neo4j.kernel.impl.store.LabelIdArray.filter;
import static org.neo4j.kernel.impl.store.NodeLabelsField.parseLabelsBody;
import static org.neo4j.util.BitBuffer.bits;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.util.BitBuffer;

public class InlineNodeLabels implements NodeLabels {
    private static final int LABEL_BITS = 36;
    private final NodeRecord node;

    public InlineNodeLabels(NodeRecord node) {
        this.node = node;
    }

    @Override
    public int[] get(NodeStore nodeStore, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        return get(node);
    }

    public static int[] get(NodeRecord node) {
        return parseInlined(node.getLabelField());
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

    public static Collection<DynamicRecord> putSorted(
            NodeRecord node,
            int[] labelIds,
            NodeStore nodeStore,
            DynamicRecordAllocator allocator,
            CursorContext cursorContext,
            StoreCursors storeCursors,
            MemoryTracker memoryTracker) {
        if (tryInlineInNodeRecord(node, labelIds, node.getDynamicLabelRecords())) {
            return Collections.emptyList();
        }

        return DynamicNodeLabels.putSorted(
                node, labelIds, nodeStore, allocator, cursorContext, storeCursors, memoryTracker);
    }

    @Override
    public Collection<DynamicRecord> add(
            int labelId,
            NodeStore nodeStore,
            DynamicRecordAllocator allocator,
            CursorContext cursorContext,
            StoreCursors storeCursors,
            MemoryTracker memoryTracker) {
        int[] augmentedLabelIds = labelCount(node.getLabelField()) == 0
                ? new int[] {labelId}
                : concatAndSort(parseInlined(node.getLabelField()), labelId);

        return putSorted(node, augmentedLabelIds, nodeStore, allocator, cursorContext, storeCursors, memoryTracker);
    }

    @Override
    public Collection<DynamicRecord> remove(
            int labelId,
            NodeStore nodeStore,
            DynamicRecordAllocator allocator,
            CursorContext cursorContext,
            StoreCursors storeCursors,
            MemoryTracker memoryTracker) {
        int[] newLabelIds = filter(parseInlined(node.getLabelField()), labelId);
        boolean inlined = tryInlineInNodeRecord(node, newLabelIds, node.getDynamicLabelRecords());
        assert inlined;
        return Collections.emptyList();
    }

    static boolean tryInlineInNodeRecord(NodeRecord node, int[] ids, List<DynamicRecord> changedDynamicRecords) {
        // We reserve the high header bit for future extensions of the format of the in-lined label bits
        // i.e. the 0-valued high header bit can allow for 0-7 in-lined labels in the bit-packed format.
        if (ids.length > 7) {
            return false;
        }

        byte bitsPerLabel = (byte) (ids.length > 0 ? (LABEL_BITS / ids.length) : LABEL_BITS);
        BitBuffer bits = bits(5);
        if (!inlineValues(ids, bitsPerLabel, bits)) {
            return false;
        }
        node.setLabelField(
                combineLabelCountAndLabelStorage((byte) ids.length, bits.getLongs()[0]), changedDynamicRecords);
        return true;
    }

    private static boolean inlineValues(int[] values, int maxBitsPerLabel, BitBuffer target) {
        long limit = 1L << maxBitsPerLabel;
        for (long value : values) {
            if (highestOneBit(value) < limit) {
                target.put(value, maxBitsPerLabel);
            } else {
                return false;
            }
        }
        return true;
    }

    public static int[] parseInlined(long labelField) {
        byte numberOfLabels = labelCount(labelField);
        if (numberOfLabels == 0) {
            return EMPTY_INT_ARRAY;
        }

        long existingLabelsField = parseLabelsBody(labelField);
        byte bitsPerLabel = (byte) (LABEL_BITS / numberOfLabels);
        long mask = (1L << bitsPerLabel) - 1;
        int[] result = new int[numberOfLabels];
        for (int i = 0; i < numberOfLabels; i++) {
            result[i] = (int) (existingLabelsField & mask);
            existingLabelsField >>>= bitsPerLabel;
        }
        return result;
    }

    public static boolean hasLabel(NodeRecord node, int label) {
        long labelField = node.getLabelField();
        byte numberOfLabels = labelCount(labelField);
        if (numberOfLabels == 0) {
            return false;
        }

        long existingLabelsField = parseLabelsBody(labelField);
        byte bitsPerLabel = (byte) (LABEL_BITS / numberOfLabels);
        long mask = (1L << bitsPerLabel) - 1;
        for (int i = 0; i < numberOfLabels; i++) {
            if ((existingLabelsField & mask) == label) {
                return true;
            }
            existingLabelsField >>>= bitsPerLabel;
        }
        return false;
    }

    private static long combineLabelCountAndLabelStorage(byte labelCount, long labelBits) {
        return ((long) labelCount << 36) | labelBits;
    }

    private static byte labelCount(long labelField) {
        return (byte) ((labelField & 0xF000000000L) >>> 36);
    }

    @Override
    public String toString() {
        return format("Inline(0x%x:%s)", node.getLabelField(), Arrays.toString(parseInlined(node.getLabelField())));
    }
}
