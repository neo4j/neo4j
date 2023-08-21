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
package org.neo4j.kernel.impl.api;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.neo4j.storageengine.api.RelationshipDirection.INCOMING;
import static org.neo4j.storageengine.api.RelationshipDirection.LOOP;
import static org.neo4j.storageengine.api.RelationshipDirection.OUTGOING;
import static org.neo4j.util.Preconditions.checkState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.RelationshipVisitorWithProperties;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.RelationshipModifications;

public class FlatRelationshipModifications implements RelationshipModifications {
    private final SortedMap<Long, NodeData> data = new TreeMap<>();

    public FlatRelationshipModifications(RelationshipData... creations) {
        mapData(creations, NodeData::creations);
    }

    public FlatRelationshipModifications(RelationshipData[] creations, RelationshipData[] deletions) {
        mapData(creations, NodeData::creations);
        mapData(deletions, NodeData::deletions);
    }

    private void mapData(
            RelationshipData[] relationships,
            Function<NodeData, SortedMap<Integer, List<RelationshipData>>> mapFunction) {
        for (RelationshipData relationship : relationships) {
            mapNode(relationship, relationship.startNode, mapFunction);
            if (relationship.startNode != relationship.endNode) {
                mapNode(relationship, relationship.endNode, mapFunction);
            }
        }
    }

    private void mapNode(
            RelationshipData relationship,
            long node,
            Function<NodeData, SortedMap<Integer, List<RelationshipData>>> mapFunction) {
        NodeData nodeData = data.computeIfAbsent(node, n -> new NodeData());
        mapFunction
                .apply(nodeData)
                .computeIfAbsent(relationship.type, type -> new ArrayList<>())
                .add(relationship);
    }

    @Override
    public RelationshipBatch creations() {
        return allAsBatch(NodeData::creations);
    }

    @Override
    public RelationshipBatch deletions() {
        return allAsBatch(NodeData::deletions);
    }

    private FlatRelationshipBatch allAsBatch(
            Function<NodeData, SortedMap<Integer, List<RelationshipData>>> mapFunction) {
        return new FlatRelationshipBatch(data.values().stream()
                .flatMap(n -> mapFunction.apply(n).values().stream())
                .flatMap(Collection::stream)
                .collect(toSet()));
    }

    @Override
    public void forEachSplit(IdsVisitor visitor) {
        data.forEach((nodeId, nodeData) -> {
            visitor.accept(new NodeRelationshipIds() {
                @Override
                public long nodeId() {
                    return nodeId;
                }

                @Override
                public boolean hasCreations() {
                    return !nodeData.creations.isEmpty();
                }

                @Override
                public boolean hasCreations(int type) {
                    return nodeData.creations.containsKey(type);
                }

                @Override
                public boolean hasDeletions() {
                    return !nodeData.deletions.isEmpty();
                }

                @Override
                public RelationshipBatch creations() {
                    return new FlatRelationshipBatch(nodeData.creations.values().stream()
                            .flatMap(Collection::stream)
                            .collect(toList()));
                }

                @Override
                public RelationshipBatch deletions() {
                    return new FlatRelationshipBatch(nodeData.deletions.values().stream()
                            .flatMap(Collection::stream)
                            .collect(toList()));
                }

                @Override
                public void forEachCreationSplitInterruptible(InterruptibleTypeIdsVisitor visitor) {
                    for (Map.Entry<Integer, List<RelationshipData>> entry : nodeData.creations.entrySet()) {
                        if (visitor.test(new FlatNodeRelationshipTypeIds(entry.getKey(), entry.getValue(), nodeId))) {
                            break;
                        }
                    }
                }

                @Override
                public void forEachDeletionSplitInterruptible(InterruptibleTypeIdsVisitor visitor) {
                    for (Map.Entry<Integer, List<RelationshipData>> entry : nodeData.deletions.entrySet()) {
                        if (visitor.test(new FlatNodeRelationshipTypeIds(entry.getKey(), entry.getValue(), nodeId))) {
                            break;
                        }
                    }
                }
            });
        });
    }

    public record RelationshipData(
            long id, int type, long startNode, long endNode, Collection<StorageProperty> properties) {
        public RelationshipData(long id, int type, long startNode, long endNode) {
            this(id, type, startNode, endNode, Collections.emptyList());
        }

        public RelationshipDirection direction(long fromNodePov) {
            checkState(
                    fromNodePov == startNode || fromNodePov == endNode,
                    fromNodePov + " is neither node " + startNode + " nor " + endNode);
            return fromNodePov == startNode ? startNode == endNode ? LOOP : OUTGOING : INCOMING;
        }

        public long neighbourNode(long fromNodeIdPov) {
            return startNode == fromNodeIdPov ? endNode : startNode;
        }
    }

    public static RelationshipModifications singleCreate(long id, int type, long startNode, long endNode) {
        return new FlatRelationshipModifications(relationship(id, type, startNode, endNode));
    }

    public static RelationshipModifications singleCreate(
            long id, int type, long startNode, long endNode, Collection<StorageProperty> properties) {
        return new FlatRelationshipModifications(relationship(id, type, startNode, endNode, properties));
    }

    public static RelationshipModifications singleCreate(RelationshipData relationship) {
        return new FlatRelationshipModifications(relationship);
    }

    public static RelationshipModifications modifications(RelationshipData[] creations, RelationshipData[] deletions) {
        return new FlatRelationshipModifications(creations, deletions);
    }

    public static RelationshipModifications creations(RelationshipData... creations) {
        return new FlatRelationshipModifications(creations);
    }

    public static RelationshipModifications deletions(RelationshipData... deletions) {
        return new FlatRelationshipModifications(relationships(), deletions);
    }

    public static RelationshipData relationship(long id, int type, long startNode, long endNode) {
        return new RelationshipData(id, type, startNode, endNode);
    }

    public static RelationshipData relationship(
            long id, int type, long startNode, long endNode, Collection<StorageProperty> properties) {
        return new RelationshipData(id, type, startNode, endNode, properties);
    }

    public static RelationshipModifications singleDelete(long id, int type, long startNode, long endNode) {
        return new FlatRelationshipModifications(
                relationships(), relationships(relationship(id, type, startNode, endNode)));
    }

    public static RelationshipModifications singleDelete(RelationshipData relationship) {
        return new FlatRelationshipModifications(relationships(), relationships(relationship));
    }

    public static RelationshipData[] relationships(RelationshipData... relationships) {
        return relationships;
    }

    private static class FlatRelationshipBatch implements RelationshipBatch {
        private final Collection<RelationshipData> relationships;

        FlatRelationshipBatch(Collection<RelationshipData> relationships) {
            this.relationships = relationships;
        }

        @Override
        public int size() {
            return relationships.size();
        }

        @Override
        public <E extends Exception> void forEach(RelationshipVisitorWithProperties<E> relationship) throws E {
            for (RelationshipData rel : relationships) {
                relationship.visit(rel.id, rel.type, rel.startNode, rel.endNode, rel.properties);
            }
        }
    }

    private static class NodeData {
        final SortedMap<Integer, List<RelationshipData>> creations = new TreeMap<>();
        final SortedMap<Integer, List<RelationshipData>> deletions = new TreeMap<>();

        SortedMap<Integer, List<RelationshipData>> creations() {
            return creations;
        }

        SortedMap<Integer, List<RelationshipData>> deletions() {
            return deletions;
        }
    }

    private static class FlatNodeRelationshipTypeIds implements NodeRelationshipTypeIds {
        private final int type;
        private final List<RelationshipData> relationships;
        private final Long nodeId;

        FlatNodeRelationshipTypeIds(int type, List<RelationshipData> relationships, Long nodeId) {
            this.type = type;
            this.relationships = relationships;
            this.nodeId = nodeId;
        }

        @Override
        public int type() {
            return type;
        }

        @Override
        public boolean hasOut() {
            return relationships.stream().anyMatch(r -> r.direction(nodeId) == OUTGOING);
        }

        @Override
        public boolean hasIn() {
            return relationships.stream().anyMatch(r -> r.direction(nodeId) == INCOMING);
        }

        @Override
        public boolean hasLoop() {
            return relationships.stream().anyMatch(r -> r.direction(nodeId) == LOOP);
        }

        @Override
        public RelationshipBatch out() {
            return new FlatRelationshipBatch(relationships.stream()
                    .filter(r -> r.direction(nodeId) == OUTGOING)
                    .collect(toList()));
        }

        @Override
        public RelationshipBatch in() {
            return new FlatRelationshipBatch(relationships.stream()
                    .filter(r -> r.direction(nodeId) == INCOMING)
                    .collect(toList()));
        }

        @Override
        public RelationshipBatch loop() {
            return new FlatRelationshipBatch(relationships.stream()
                    .filter(r -> r.direction(nodeId) == LOOP)
                    .collect(toList()));
        }
    }
}
