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

import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.RelationshipSelection.ALL_RELATIONSHIPS;

import org.eclipse.collections.api.factory.primitive.IntObjectMaps;
import org.eclipse.collections.api.factory.primitive.IntSets;
import org.eclipse.collections.api.factory.primitive.LongObjectMaps;
import org.eclipse.collections.api.factory.primitive.LongSets;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.internal.kernel.api.EntityCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.exceptions.EntityAlreadyExistsException;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.values.storable.Value;

@ExtendWith(RandomExtension.class)
class CreateWithSpecificIdKernelWriteTest extends KernelAPIWriteTestBase<WriteTestSupport> {
    @Inject
    private RandomSupport random;

    @Override
    public WriteTestSupport newTestSupport() {
        return new WriteTestSupport();
    }

    @Test
    void shouldCreateNodesAndRelationshipsWithSpecificIds() throws Exception {
        // given
        MutableLongObjectMap<NodeData> nodes = LongObjectMaps.mutable.empty();
        MutableLongObjectMap<RelationshipData> relationships = LongObjectMaps.mutable.empty();
        try (var tx = beginTransaction()) {
            var write = tx.dataWrite();
            for (int i = 0; i < 10; i++) {
                int[] labels = (i % 2 == 0) ? EMPTY_INT_ARRAY : new int[] {i % 3};
                long nodeId = labels.length == 0 ? write.nodeCreate() : write.nodeCreateWithLabels(labels);
                int keyId = i % 4;
                var value = random.nextValue();
                write.nodeSetProperty(nodeId, keyId, value);
                nodes.put(
                        nodeId,
                        new NodeData(
                                IntSets.immutable.of(labels),
                                IntObjectMaps.immutable.with(keyId, value),
                                LongSets.mutable.empty()));
            }

            var nodeIds = nodes.keySet().toArray();
            for (int i = 0; i < 20; i++) {
                long startNodeId = random.among(nodeIds);
                int type = i % 3;
                long endNodeId = random.among(nodeIds);
                long relationshipId = write.relationshipCreate(startNodeId, type, endNodeId);
                int keyId = i % 4;
                var value = random.nextValue();
                write.relationshipSetProperty(relationshipId, keyId, value);
                var data =
                        new RelationshipData(startNodeId, type, endNodeId, IntObjectMaps.immutable.with(keyId, value));
                relationships.put(relationshipId, data);
                nodes.get(startNodeId).relationships.add(relationshipId);
                nodes.get(endNodeId).relationships.add(relationshipId);
            }
            tx.commit();
        }

        // when
        clearGraph();
        try (var tx = beginTransaction()) {
            var write = tx.dataWrite();
            for (var node : nodes.keyValuesView()) {
                long nodeId = node.getOne();
                var data = node.getTwo();
                if (data.labels.isEmpty()) {
                    write.nodeWithSpecificIdCreate(nodeId);
                } else {
                    write.nodeWithSpecificIdCreateWithLabels(nodeId, data.labels.toSortedArray());
                }
                for (var property : data.properties.keyValuesView()) {
                    write.nodeSetProperty(nodeId, property.getOne(), property.getTwo());
                }
            }
            for (var relationship : relationships.keyValuesView()) {
                long relationshipId = relationship.getOne();
                var data = relationship.getTwo();
                write.relationshipWithSpecificIdCreate(relationshipId, data.startNodeId, data.type, data.endNodeId);
                for (var property : data.properties.keyValuesView()) {
                    write.relationshipSetProperty(relationshipId, property.getOne(), property.getTwo());
                }
            }
            tx.commit();
        }

        // then, is all data there after we created if with specific IDs?
        assertThat(readNodes()).isEqualTo(nodes);
        assertThat(readRelationships()).isEqualTo(relationships);
    }

    @Test
    void shouldFailCreateNodeWithSpecificIdIfAlreadyExists() throws Exception {
        // given
        long nodeId;
        try (var tx = beginTransaction()) {
            nodeId = tx.dataWrite().nodeCreate();
            tx.commit();
        }

        // when/then
        try (var tx = beginTransaction()) {
            assertThatThrownBy(() -> tx.dataWrite().nodeWithSpecificIdCreate(nodeId))
                    .isInstanceOf(EntityAlreadyExistsException.class);
            assertThatThrownBy(() -> tx.dataWrite().nodeWithSpecificIdCreateWithLabels(nodeId, new int[] {1}))
                    .isInstanceOf(EntityAlreadyExistsException.class);
            tx.commit();
        }
    }

    @Test
    void shouldFailCreateRelationshipWithSpecificIdIfAlreadyExists() throws Exception {
        // given
        long relationshipId;
        try (var tx = beginTransaction()) {
            var write = tx.dataWrite();
            var nodeId1 = write.nodeCreate();
            var nodeId2 = write.nodeCreate();
            relationshipId = write.relationshipCreate(nodeId1, 1, nodeId2);
            tx.commit();
        }

        // when/then
        try (var tx = beginTransaction()) {
            var write = tx.dataWrite();
            var nodeId1 = write.nodeCreate();
            var nodeId2 = write.nodeCreate();
            assertThatThrownBy(() -> write.relationshipWithSpecificIdCreate(relationshipId, nodeId1, 1, nodeId2))
                    .isInstanceOf(EntityAlreadyExistsException.class);
            tx.commit();
        }
    }

    private MutableLongObjectMap<NodeData> readNodes() throws Exception {
        MutableLongObjectMap<NodeData> nodes = LongObjectMaps.mutable.empty();
        try (var tx = beginTransaction();
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var propertyCursor = tx.cursors().allocatePropertyCursor(NULL_CONTEXT, INSTANCE);
                var traversalCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            var read = tx.dataRead();
            read.allNodesScan(nodeCursor);
            while (nodeCursor.next()) {
                var relationships = LongSets.mutable.empty();
                nodeCursor.relationships(traversalCursor, ALL_RELATIONSHIPS);
                while (traversalCursor.next()) {
                    relationships.add(traversalCursor.reference());
                }
                nodes.put(
                        nodeCursor.reference(),
                        new NodeData(
                                IntSets.immutable.of(nodeCursor.labels().all()),
                                properties(nodeCursor, propertyCursor),
                                relationships));
            }
        }
        return nodes;
    }

    private IntObjectMap<Value> properties(EntityCursor entityCursor, PropertyCursor propertyCursor) {
        MutableIntObjectMap<Value> properties = IntObjectMaps.mutable.empty();
        entityCursor.properties(propertyCursor);
        while (propertyCursor.next()) {
            properties.put(propertyCursor.propertyKey(), propertyCursor.propertyValue());
        }
        return properties.toImmutable();
    }

    private MutableLongObjectMap<RelationshipData> readRelationships() throws Exception {
        MutableLongObjectMap<RelationshipData> relationships = LongObjectMaps.mutable.empty();
        try (var tx = beginTransaction();
                var relationshipCursor = tx.cursors().allocateRelationshipScanCursor(NULL_CONTEXT);
                var propertyCursor = tx.cursors().allocatePropertyCursor(NULL_CONTEXT, INSTANCE)) {
            var read = tx.dataRead();
            read.allRelationshipsScan(relationshipCursor);
            while (relationshipCursor.next()) {
                relationships.put(
                        relationshipCursor.reference(),
                        new RelationshipData(
                                relationshipCursor.sourceNodeReference(),
                                relationshipCursor.type(),
                                relationshipCursor.targetNodeReference(),
                                properties(relationshipCursor, propertyCursor)));
            }
        }
        return relationships;
    }

    private record NodeData(IntSet labels, IntObjectMap<Value> properties, MutableLongSet relationships) {}

    private record RelationshipData(long startNodeId, int type, long endNodeId, IntObjectMap<Value> properties) {}
}
