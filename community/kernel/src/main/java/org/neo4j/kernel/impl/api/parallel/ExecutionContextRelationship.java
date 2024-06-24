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

import java.util.Map;
import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.ExecutionContext;
import org.neo4j.kernel.impl.core.AbstractEntity;
import org.neo4j.storageengine.api.LongReference;

public class ExecutionContextRelationship extends AbstractEntity implements Relationship {

    private final ExecutionContext executionContext;
    private final long id;

    public ExecutionContextRelationship(long relationshipId, ExecutionContext executionContext) {
        this.id = relationshipId;
        this.executionContext = executionContext;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public String getElementId() {
        return executionContext.elementIdMapper().relationshipElementId(id);
    }

    @Override
    public boolean hasProperty(String key) {
        var cursors = executionContext.cursors();
        try (RelationshipScanCursor relationships = cursors.allocateRelationshipScanCursor(
                        executionContext.cursorContext(), executionContext.memoryTracker());
                PropertyCursor properties = cursors.allocatePropertyCursor(
                        executionContext.cursorContext(), executionContext.memoryTracker())) {
            singleRelationship(relationships);
            return hasProperty(key, relationships, properties);
        }
    }

    @Override
    public Object getProperty(String key) {
        var cursors = executionContext.cursors();
        try (RelationshipScanCursor relationships = cursors.allocateRelationshipScanCursor(
                        executionContext.cursorContext(), executionContext.memoryTracker());
                PropertyCursor properties = cursors.allocatePropertyCursor(
                        executionContext.cursorContext(), executionContext.memoryTracker())) {
            singleRelationship(relationships);
            return getProperty(key, relationships, properties);
        }
    }

    @Override
    public Object getProperty(String key, Object defaultValue) {
        var cursors = executionContext.cursors();
        try (RelationshipScanCursor relationships = cursors.allocateRelationshipScanCursor(
                        executionContext.cursorContext(), executionContext.memoryTracker());
                PropertyCursor properties = cursors.allocatePropertyCursor(
                        executionContext.cursorContext(), executionContext.memoryTracker())) {
            singleRelationship(relationships);

            return getProperty(key, defaultValue, relationships, properties);
        }
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
    public Iterable<String> getPropertyKeys() {
        var cursors = executionContext.cursors();
        try (RelationshipScanCursor relationships = cursors.allocateRelationshipScanCursor(
                        executionContext.cursorContext(), executionContext.memoryTracker());
                PropertyCursor properties = cursors.allocatePropertyCursor(
                        executionContext.cursorContext(), executionContext.memoryTracker())) {
            singleRelationship(relationships);
            return getPropertyKeys(relationships, properties);
        }
    }

    @Override
    public Map<String, Object> getProperties(String... keys) {
        var cursors = executionContext.cursors();
        try (RelationshipScanCursor relationships = cursors.allocateRelationshipScanCursor(
                        executionContext.cursorContext(), executionContext.memoryTracker());
                PropertyCursor properties = cursors.allocatePropertyCursor(
                        executionContext.cursorContext(), executionContext.memoryTracker())) {
            singleRelationship(relationships);
            return getProperties(relationships, properties, keys);
        }
    }

    @Override
    public Map<String, Object> getAllProperties() {
        var cursors = executionContext.cursors();
        try (RelationshipScanCursor relationships = cursors.allocateRelationshipScanCursor(
                        executionContext.cursorContext(), executionContext.memoryTracker());
                PropertyCursor properties = cursors.allocatePropertyCursor(
                        executionContext.cursorContext(), executionContext.memoryTracker())) {
            singleRelationship(relationships);
            return getAllProperties(relationships, properties);
        }
    }

    @Override
    public void delete() {
        throw new UnsupportedOperationException("Operation unsupported during parallel query execution");
    }

    @Override
    public Node getStartNode() {
        try (RelationshipScanCursor relationships = executionContext
                .cursors()
                .allocateRelationshipScanCursor(executionContext.cursorContext(), executionContext.memoryTracker())) {
            singleRelationship(relationships);
            return new ExecutionContextNode(relationships.sourceNodeReference(), executionContext);
        }
    }

    @Override
    public Node getEndNode() {
        try (RelationshipScanCursor relationships = executionContext
                .cursors()
                .allocateRelationshipScanCursor(executionContext.cursorContext(), executionContext.memoryTracker())) {
            singleRelationship(relationships);
            return new ExecutionContextNode(relationships.targetNodeReference(), executionContext);
        }
    }

    @Override
    public Node getOtherNode(Node node) {
        return new ExecutionContextNode(getOtherNodeId(node.getId()), executionContext);
    }

    @Override
    public Node[] getNodes() {
        return new Node[] {getStartNode(), getEndNode()};
    }

    @Override
    public RelationshipType getType() {
        try (RelationshipScanCursor relationships = executionContext
                .cursors()
                .allocateRelationshipScanCursor(executionContext.cursorContext(), executionContext.memoryTracker())) {
            singleRelationship(relationships);
            int type = relationships.type();
            if (type == LongReference.NULL) {
                throw new NotFoundException(new EntityNotFoundException(EntityType.RELATIONSHIP, getElementId()));
            }

            try {
                String name = executionContext.tokenRead().relationshipTypeName(type);
                return RelationshipType.withName(name);
            } catch (KernelException e) {
                throw new IllegalStateException("Kernel API returned non-existent relationship type: " + type, e);
            }
        }
    }

    @Override
    public boolean isType(RelationshipType type) {
        return getType().equals(type);
    }

    private void singleRelationship(RelationshipScanCursor relationships) {
        executionContext.dataRead().singleRelationship(id, relationships);
        if (!relationships.next()) {
            throw new NotFoundException(new EntityNotFoundException(EntityType.RELATIONSHIP, getElementId()));
        }
    }

    @Override
    protected TokenRead tokenRead() {
        return executionContext.tokenRead();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Relationship && this.getId() == ((Relationship) o).getId();
    }

    @Override
    public int hashCode() {
        return (int) ((getId() >>> 32) ^ getId());
    }

    @Override
    public String toString() {
        try {
            var relType = getType().name();
            return format(
                    "(%d)-[%s,%d]->(%d)",
                    getStartNode().getId(), relType, getId(), getEndNode().getId());
        } catch (NotInTransactionException | DatabaseShutdownException e) {
            // We don't keep the rel-name lookup if the database is shut down. Source ID and target ID also requires
            // database access in a transaction. However, failing on toString would be uncomfortably evil, so we fall
            // back to noting the relationship id.
        }
        return format("(?)-[%d]->(?)", getId());
    }
}
