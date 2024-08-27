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

import static org.eclipse.collections.impl.set.mutable.primitive.IntHashSet.newSetWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.storageengine.api.PropertySelection.ALL_PROPERTIES;

import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.junit.jupiter.api.Test;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;

/**
 * Test read access to committed label data.
 */
class RecordStorageReaderLabelTest extends RecordStorageReaderTestBase {
    @Test
    void shouldBeAbleToListLabelsForNode() throws Exception {
        // GIVEN
        long nodeId;
        nodeId = createNode(map(), label1, label2);
        int labelId1 = labelId(label1);
        int labelId2 = labelId(label2);

        // THEN
        StorageNodeCursor nodeCursor =
                storageReader.allocateNodeCursor(NULL_CONTEXT, storageCursors, EmptyMemoryTracker.INSTANCE);
        nodeCursor.single(nodeId);
        assertTrue(nodeCursor.next());
        assertEquals(newSetWith(labelId1, labelId2), IntSets.immutable.of(nodeCursor.labels()));
    }

    @Test
    void labelsShouldNotLeakOutAsProperties() throws Exception {
        // GIVEN
        long nodeId = createNode(map("name", "Node"), label1);
        int namePropertyKeyId = propertyKeyId("name");

        // WHEN THEN
        StorageNodeCursor nodeCursor =
                storageReader.allocateNodeCursor(NULL_CONTEXT, storageCursors, EmptyMemoryTracker.INSTANCE);
        StoragePropertyCursor propertyCursor =
                storageReader.allocatePropertyCursor(NULL_CONTEXT, storageCursors, EmptyMemoryTracker.INSTANCE);
        nodeCursor.single(nodeId);
        assertTrue(nodeCursor.next());
        nodeCursor.properties(propertyCursor, ALL_PROPERTIES);
        assertTrue(propertyCursor.next());
        assertEquals(namePropertyKeyId, propertyCursor.propertyKey());
        assertFalse(propertyCursor.next());
    }

    @Test
    void shouldCountAllNodes() throws Exception {
        // given
        int nodeCountPerLabel = 5;
        long[] label2Nodes = new long[nodeCountPerLabel];
        for (int i = 0; i < nodeCountPerLabel; i++) {
            createNode(map(), label1);
            label2Nodes[i] = createNode(map(), label2);
        }
        for (long label2Node : label2Nodes) {
            deleteNode(label2Node);
        }

        // when
        long count = storageReader.nodesGetCount(NULL_CONTEXT);

        // then
        assertEquals(nodeCountPerLabel, count);
    }
}
