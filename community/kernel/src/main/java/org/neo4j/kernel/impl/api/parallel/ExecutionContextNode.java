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
package org.neo4j.kernel.impl.api.parallel;

import static java.lang.String.format;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.allIterator;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.incomingIterator;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.outgoingIterator;

import java.util.Map;
import org.neo4j.common.EntityType;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.collection.AbstractResourceIterable;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.helpers.RelationshipFactory;
import org.neo4j.kernel.api.ExecutionContext;
import org.neo4j.kernel.impl.core.AbstractNodeEntity;

public class ExecutionContextNode extends AbstractNodeEntity {

    private final long nodeId;
    private final ExecutionContext executionContext;

    public ExecutionContextNode(long nodeId, ExecutionContext executionContext) {
        this.nodeId = nodeId;
        this.executionContext = executionContext;
    }

    @Override
    public long getId() {
        return nodeId;
    }

    @Override
    public String getElementId() {
        return executionContext.elementIdMapper().nodeElementId(nodeId);
    }

    @Override
    public boolean hasProperty(String key) {
        var cursors = executionContext.cursors();
        try (NodeCursor nodes =
                        cursors.allocateNodeCursor(executionContext.cursorContext(), executionContext.memoryTracker());
                PropertyCursor properties = cursors.allocatePropertyCursor(
                        executionContext.cursorContext(), executionContext.memoryTracker())) {
            singleNode(nodes);
            return hasProperty(key, nodes, properties);
        }
    }

    @Override
    public Object getProperty(String key) {
        var cursors = executionContext.cursors();
        try (NodeCursor nodes =
                        cursors.allocateNodeCursor(executionContext.cursorContext(), executionContext.memoryTracker());
                PropertyCursor properties = cursors.allocatePropertyCursor(
                        executionContext.cursorContext(), executionContext.memoryTracker())) {
            singleNode(nodes);
            return getProperty(key, nodes, properties);
        }
    }

    @Override
    public Object getProperty(String key, Object defaultValue) {
        var cursors = executionContext.cursors();
        try (NodeCursor nodes =
                        cursors.allocateNodeCursor(executionContext.cursorContext(), executionContext.memoryTracker());
                PropertyCursor properties = cursors.allocatePropertyCursor(
                        executionContext.cursorContext(), executionContext.memoryTracker())) {
            singleNode(nodes);
            return getProperty(key, defaultValue, nodes, properties);
        }
    }

    @Override
    public Iterable<String> getPropertyKeys() {
        var cursors = executionContext.cursors();
        try (NodeCursor nodes =
                        cursors.allocateNodeCursor(executionContext.cursorContext(), executionContext.memoryTracker());
                PropertyCursor properties = cursors.allocatePropertyCursor(
                        executionContext.cursorContext(), executionContext.memoryTracker())) {
            singleNode(nodes);
            return getPropertyKeys(nodes, properties);
        }
    }

    @Override
    public Map<String, Object> getProperties(String... keys) {
        var cursors = executionContext.cursors();
        try (NodeCursor nodes =
                        cursors.allocateNodeCursor(executionContext.cursorContext(), executionContext.memoryTracker());
                PropertyCursor properties = cursors.allocatePropertyCursor(
                        executionContext.cursorContext(), executionContext.memoryTracker())) {
            singleNode(nodes);
            return getProperties(nodes, properties, keys);
        }
    }

    @Override
    public Map<String, Object> getAllProperties() {
        var cursors = executionContext.cursors();
        try (NodeCursor nodes =
                        cursors.allocateNodeCursor(executionContext.cursorContext(), executionContext.memoryTracker());
                PropertyCursor properties = cursors.allocatePropertyCursor(
                        executionContext.cursorContext(), executionContext.memoryTracker())) {
            singleNode(nodes);
            return getAllProperties(nodes, properties);
        }
    }

    @Override
    public ResourceIterable<Relationship> getRelationships(Direction direction, RelationshipType... types) {
        int[] typeIds = relTypeIds(executionContext.tokenRead(), types);
        return new RelationshipsIterable(executionContext, getId(), direction, typeIds, this::relationship);
    }

    @Override
    public boolean hasRelationship(Direction direction, RelationshipType... types) {
        int[] typeIds = relTypeIds(executionContext.tokenRead(), types);
        return innerHasRelationships(direction, typeIds);
    }

    private boolean innerHasRelationships(Direction direction, int[] typeIds) {
        try (ResourceIterator<Relationship> iterator =
                getRelationshipSelectionIterator(executionContext, getId(), direction, typeIds, this::relationship)) {
            return iterator.hasNext();
        }
    }

    @Override
    public ResourceIterable<Relationship> getRelationships(Direction direction) {
        return new RelationshipsIterable(executionContext, getId(), direction, null, this::relationship);
    }

    @Override
    public boolean hasRelationship(Direction direction) {
        return innerHasRelationships(direction, null);
    }

    @Override
    public Relationship getSingleRelationship(RelationshipType type, Direction dir) {
        int[] typeIds = relTypeIds(executionContext.tokenRead(), type);
        try (ResourceIterator<Relationship> relationships =
                getRelationshipSelectionIterator(executionContext, getId(), dir, typeIds, this::relationship)) {
            return getSingleRelationship(relationships, type, dir);
        }
    }

    @Override
    public Iterable<RelationshipType> getRelationshipTypes() {
        try (NodeCursor nodes = executionContext
                .cursors()
                .allocateNodeCursor(executionContext.cursorContext(), executionContext.memoryTracker())) {
            return getRelationshipTypes(nodes);
        }
    }

    @Override
    public int getDegree() {
        try (NodeCursor nodes = executionContext
                .cursors()
                .allocateNodeCursor(executionContext.cursorContext(), executionContext.memoryTracker())) {
            return getDegree(nodes);
        }
    }

    @Override
    public int getDegree(RelationshipType type) {
        try (NodeCursor nodes = executionContext
                .cursors()
                .allocateNodeCursor(executionContext.cursorContext(), executionContext.memoryTracker())) {
            return getDegree(type, nodes);
        }
    }

    @Override
    public int getDegree(Direction direction) {
        try (NodeCursor nodes = executionContext
                .cursors()
                .allocateNodeCursor(executionContext.cursorContext(), executionContext.memoryTracker())) {
            return getDegree(direction, nodes);
        }
    }

    @Override
    public int getDegree(RelationshipType type, Direction direction) {
        try (NodeCursor nodes = executionContext
                .cursors()
                .allocateNodeCursor(executionContext.cursorContext(), executionContext.memoryTracker())) {
            return getDegree(type, direction, nodes);
        }
    }

    @Override
    public boolean hasLabel(Label label) {
        try (NodeCursor nodes = executionContext
                .cursors()
                .allocateNodeCursor(executionContext.cursorContext(), executionContext.memoryTracker())) {
            return hasLabel(label, nodes);
        }
    }

    @Override
    public Iterable<Label> getLabels() {
        try (NodeCursor nodes = executionContext
                .cursors()
                .allocateNodeCursor(executionContext.cursorContext(), executionContext.memoryTracker())) {
            return getLabels(nodes);
        }
    }

    @Override
    public void delete() {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public void setProperty(String key, Object value) {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public Object removeProperty(String key) {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public Relationship createRelationshipTo(Node otherNode, RelationshipType type) {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public void addLabel(Label label) {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public void removeLabel(Label label) {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    private Relationship relationship(RelationshipTraversalCursor cursor) {
        return new ExecutionContextRelationship(cursor.relationshipReference(), executionContext);
    }

    @Override
    protected TokenRead tokenRead() {
        return executionContext.tokenRead();
    }

    @Override
    protected void singleNode(NodeCursor nodes) {
        executionContext.dataRead().singleNode(nodeId, nodes);
        if (!nodes.next()) {
            throw new NotFoundException(new EntityNotFoundException(EntityType.NODE, getElementId()));
        }
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Node && this.getId() == ((Node) o).getId();
    }

    @Override
    public int hashCode() {
        return (int) ((nodeId >>> 32) ^ nodeId);
    }

    @Override
    public String toString() {
        return "Node[" + this.getId() + "]";
    }

    private static ResourceIterator<Relationship> getRelationshipSelectionIterator(
            ExecutionContext executionContext,
            long nodeId,
            Direction direction,
            int[] typeIds,
            RelationshipFactory<Relationship> factory) {
        try (NodeCursor node = executionContext
                .cursors()
                .allocateNodeCursor(executionContext.cursorContext(), executionContext.memoryTracker())) {
            executionContext.dataRead().singleNode(nodeId, node);
            if (!node.next()) {
                throw new NotFoundException(format("Node %d not found", nodeId));
            }

            var cursorContext = executionContext.cursorContext();
            var cursors = executionContext.cursors();
            return switch (direction) {
                case OUTGOING -> outgoingIterator(cursors, node, typeIds, factory, cursorContext);
                case INCOMING -> incomingIterator(cursors, node, typeIds, factory, cursorContext);
                case BOTH -> allIterator(cursors, node, typeIds, factory, cursorContext);
            };
        }
    }

    private static class RelationshipsIterable extends AbstractResourceIterable<Relationship> {
        private final ExecutionContext executionContext;
        private final long nodeId;
        private final Direction direction;
        private final int[] typeIds;
        private final RelationshipFactory<Relationship> factory;

        private RelationshipsIterable(
                ExecutionContext executionContext,
                long nodeId,
                Direction direction,
                int[] typeIds,
                RelationshipFactory<Relationship> factory) {
            this.executionContext = executionContext;
            this.nodeId = nodeId;
            this.direction = direction;
            this.typeIds = typeIds;
            this.factory = factory;
        }

        @Override
        protected ResourceIterator<Relationship> newIterator() {
            return getRelationshipSelectionIterator(executionContext, nodeId, direction, typeIds, factory);
        }
    }
}
