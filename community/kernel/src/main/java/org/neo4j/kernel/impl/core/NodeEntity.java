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
package org.neo4j.kernel.impl.core;

import static java.lang.String.format;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.allIterator;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.incomingIterator;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.outgoingIterator;
import static org.neo4j.kernel.impl.coreapi.DefaultTransactionExceptionMapper.mapStatusException;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.values.storable.Values.NO_VALUE;

import java.util.Map;
import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.internal.helpers.collection.AbstractResourceIterable;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.internal.kernel.api.exceptions.schema.TokenCapacityExceededKernelException;
import org.neo4j.internal.kernel.api.helpers.RelationshipFactory;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.values.storable.Values;

public class NodeEntity extends AbstractNodeEntity implements RelationshipFactory<Relationship> {
    public static final long SHALLOW_SIZE = shallowSizeOfInstance(NodeEntity.class);

    private final InternalTransaction internalTransaction;
    private final long nodeId;

    public NodeEntity(InternalTransaction internalTransaction, long nodeId) {
        this.internalTransaction = internalTransaction;
        this.nodeId = nodeId;
    }

    public static boolean isDeletedInCurrentTransaction(Node node) {
        if (node instanceof NodeEntity proxy) {
            KernelTransaction ktx = proxy.internalTransaction.kernelTransaction();
            return ktx.dataRead().nodeDeletedInTransaction(proxy.nodeId);
        }
        return false;
    }

    @Override
    public long getId() {
        return nodeId;
    }

    @Override
    public String getElementId() {
        return internalTransaction.elementIdMapper().nodeElementId(nodeId);
    }

    @Override
    public void delete() {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        try {
            boolean deleted = transaction.dataWrite().nodeDelete(getId());
            if (!deleted) {
                throw new NotFoundException("Unable to delete Node[" + nodeId + "] since it has already been deleted.");
            }
        } catch (InvalidTransactionTypeKernelException e) {
            throw new ConstraintViolationException(e.getMessage(), e);
        }
    }

    @Override
    public ResourceIterable<Relationship> getRelationships(final Direction direction) {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        return innerGetRelationships(transaction, direction, null);
    }

    @Override
    public ResourceIterable<Relationship> getRelationships(final Direction direction, RelationshipType... types) {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        int[] typeIds = relTypeIds(transaction.tokenRead(), types);
        return innerGetRelationships(transaction, direction, typeIds);
    }

    private ResourceIterable<Relationship> innerGetRelationships(
            KernelTransaction transaction, final Direction direction, int[] typeIds) {
        return new RelationshipsIterable(transaction, getId(), direction, typeIds, this);
    }

    @Override
    public boolean hasRelationship(Direction direction) {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        return innerHasRelationships(transaction, direction, null);
    }

    @Override
    public boolean hasRelationship(Direction direction, RelationshipType... types) {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        int[] typeIds = relTypeIds(transaction.tokenRead(), types);
        return innerHasRelationships(transaction, direction, typeIds);
    }

    private boolean innerHasRelationships(
            final KernelTransaction transaction, final Direction direction, int[] typeIds) {
        try (ResourceIterator<Relationship> iterator =
                getRelationshipSelectionIterator(transaction, getId(), direction, typeIds, this)) {
            return iterator.hasNext();
        }
    }

    @Override
    public Relationship getSingleRelationship(RelationshipType type, Direction dir) {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        int[] typeIds = relTypeIds(transaction.tokenRead(), type);
        try (ResourceIterator<Relationship> relationships =
                getRelationshipSelectionIterator(transaction, getId(), dir, typeIds, this)) {
            return getSingleRelationship(relationships, type, dir);
        }
    }

    @Override
    public void setProperty(String key, Object value) {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        int propertyKeyId;
        try {
            propertyKeyId = transaction.tokenWrite().propertyKeyGetOrCreateForName(key);
        } catch (IllegalTokenNameException e) {
            throw new IllegalArgumentException(format("Invalid property key '%s'.", key), e);
        } catch (TokenCapacityExceededKernelException e) {
            throw new ConstraintViolationException(e.getMessage(), e);
        } catch (KernelException e) {
            throw mapStatusException("Unknown error trying to create property key token", e.status(), e);
        }

        try {
            transaction.dataWrite().nodeSetProperty(nodeId, propertyKeyId, Values.of(value, false));
        } catch (ConstraintValidationException e) {
            throw new ConstraintViolationException(e.getUserMessage(transaction.tokenRead()), e);
        } catch (IllegalArgumentException e) {
            try {
                transaction.rollback();
            } catch (org.neo4j.internal.kernel.api.exceptions.TransactionFailureException ex) {
                ex.addSuppressed(e);
                throw new TransactionFailureException(
                        "Fail to rollback transaction.", ex, Status.Transaction.TransactionRollbackFailed);
            }
            throw e;
        } catch (EntityNotFoundException e) {
            throw new NotFoundException(e);
        } catch (KernelException e) {
            throw new ConstraintViolationException(e.getMessage(), e);
        }
    }

    @Override
    public Object removeProperty(String key) throws NotFoundException {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        int propertyKeyId = transaction.tokenRead().propertyKey(key);
        if (propertyKeyId == TokenRead.NO_TOKEN) {
            return NO_VALUE.asObjectCopy();
        }
        try {
            return transaction
                    .dataWrite()
                    .nodeRemoveProperty(nodeId, propertyKeyId)
                    .asObjectCopy();
        } catch (EntityNotFoundException e) {
            throw new NotFoundException(e);
        } catch (InvalidTransactionTypeKernelException e) {
            throw new ConstraintViolationException(e.getMessage(), e);
        }
    }

    @Override
    public Object getProperty(String key, Object defaultValue) {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        NodeCursor nodes = transaction.ambientNodeCursor();
        singleNode(nodes);
        return getProperty(key, defaultValue, nodes, transaction.ambientPropertyCursor());
    }

    @Override
    public Iterable<String> getPropertyKeys() {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        NodeCursor nodes = transaction.ambientNodeCursor();
        singleNode(nodes);
        return getPropertyKeys(nodes, transaction.ambientPropertyCursor());
    }

    @Override
    public Map<String, Object> getProperties(String... keys) {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        NodeCursor nodes = transaction.ambientNodeCursor();
        singleNode(nodes);
        return getProperties(transaction.ambientNodeCursor(), transaction.ambientPropertyCursor(), keys);
    }

    @Override
    public Map<String, Object> getAllProperties() {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        NodeCursor nodes = transaction.ambientNodeCursor();
        singleNode(nodes);
        return getAllProperties(transaction.ambientNodeCursor(), transaction.ambientPropertyCursor());
    }

    @Override
    public Object getProperty(String key) throws NotFoundException {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        NodeCursor nodes = transaction.ambientNodeCursor();
        singleNode(nodes);
        return getProperty(key, nodes, transaction.ambientPropertyCursor());
    }

    @Override
    public boolean hasProperty(String key) {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        NodeCursor nodes = transaction.ambientNodeCursor();
        singleNode(nodes);
        return hasProperty(key, nodes, transaction.ambientPropertyCursor());
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

    @Override
    public Relationship createRelationshipTo(Node otherNode, RelationshipType type) {
        if (otherNode == null) {
            throw new IllegalArgumentException("Other node is null.");
        }

        KernelTransaction transaction = internalTransaction.kernelTransaction();
        int relationshipTypeId;
        try {
            relationshipTypeId = transaction.tokenWrite().relationshipTypeGetOrCreateForName(type.name());
        } catch (IllegalTokenNameException e) {
            throw new IllegalArgumentException(format("Invalid type name '%s'.", type.name()), e);
        } catch (TokenCapacityExceededKernelException e) {
            throw new ConstraintViolationException(e.getMessage(), e);
        } catch (KernelException e) {
            throw mapStatusException("Unknown error trying to create relationship type token", e.status(), e);
        }

        try {
            long relationshipId =
                    transaction.dataWrite().relationshipCreate(nodeId, relationshipTypeId, otherNode.getId());
            return internalTransaction.newRelationshipEntity(
                    relationshipId, nodeId, relationshipTypeId, otherNode.getId());
        } catch (EntityNotFoundException e) {
            throw new NotFoundException(
                    "Node[" + e.entityId() + "] is deleted and cannot be used to create a relationship");
        } catch (InvalidTransactionTypeKernelException e) {
            throw new ConstraintViolationException(e.getMessage(), e);
        }
    }

    @Override
    public void addLabel(Label label) {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        int labelId;
        try {
            labelId = transaction.tokenWrite().labelGetOrCreateForName(label.name());
        } catch (IllegalTokenNameException e) {
            throw new IllegalArgumentException(format("Invalid label name '%s'.", label.name()), e);
        } catch (TokenCapacityExceededKernelException e) {
            throw new ConstraintViolationException(e.getMessage(), e);
        } catch (KernelException e) {
            throw mapStatusException("Unknown error trying to create label token", e.status(), e);
        }

        try {
            transaction.dataWrite().nodeAddLabel(getId(), labelId);
        } catch (ConstraintValidationException e) {
            throw new ConstraintViolationException(e.getUserMessage(transaction.tokenRead()), e);
        } catch (EntityNotFoundException e) {
            throw new NotFoundException("No node with id " + getId() + " found.", e);
        } catch (KernelException e) {
            throw new ConstraintViolationException(e.getMessage(), e);
        }
    }

    @Override
    public void removeLabel(Label label) {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        try {
            int labelId = transaction.tokenRead().nodeLabel(label.name());
            if (labelId != TokenRead.NO_TOKEN) {
                transaction.dataWrite().nodeRemoveLabel(getId(), labelId);
            }
        } catch (EntityNotFoundException e) {
            throw new NotFoundException("No node with id " + getId() + " found.", e);
        } catch (KernelException e) {
            throw new ConstraintViolationException(e.getMessage(), e);
        }
    }

    @Override
    public boolean hasLabel(Label label) {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        NodeCursor nodes = transaction.ambientNodeCursor();
        return hasLabel(label, nodes);
    }

    @Override
    public Iterable<Label> getLabels() {
        NodeCursor nodes = internalTransaction.kernelTransaction().ambientNodeCursor();
        return getLabels(nodes);
    }

    public InternalTransaction getTransaction() {
        return internalTransaction;
    }

    @Override
    public int getDegree() {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        return getDegree(transaction.ambientNodeCursor());
    }

    @Override
    public int getDegree(RelationshipType type) {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        return getDegree(type, transaction.ambientNodeCursor());
    }

    @Override
    public int getDegree(Direction direction) {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        return getDegree(direction, transaction.ambientNodeCursor());
    }

    @Override
    public int getDegree(RelationshipType type, Direction direction) {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        return getDegree(type, direction, transaction.ambientNodeCursor());
    }

    @Override
    public Iterable<RelationshipType> getRelationshipTypes() {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        NodeCursor nodes = transaction.ambientNodeCursor();
        return getRelationshipTypes(nodes);
    }

    @Override
    protected TokenRead tokenRead() {
        return internalTransaction.kernelTransaction().tokenRead();
    }

    @Override
    protected void singleNode(NodeCursor nodes) {
        singleNode(internalTransaction.kernelTransaction(), nodes);
    }

    private static class RelationshipsIterable extends AbstractResourceIterable<Relationship> {
        private final KernelTransaction transaction;
        private final long nodeId;
        private final Direction direction;
        private final int[] typeIds;
        private final RelationshipFactory<Relationship> factory;

        private RelationshipsIterable(
                KernelTransaction transaction,
                long nodeId,
                Direction direction,
                int[] typeIds,
                RelationshipFactory<Relationship> factory) {
            this.transaction = transaction;
            this.nodeId = nodeId;
            this.direction = direction;
            this.typeIds = typeIds;
            this.factory = factory;
        }

        @Override
        protected ResourceIterator<Relationship> newIterator() {
            return getRelationshipSelectionIterator(transaction, nodeId, direction, typeIds, factory);
        }
    }

    private static ResourceIterator<Relationship> getRelationshipSelectionIterator(
            KernelTransaction transaction,
            long nodeId,
            Direction direction,
            int[] typeIds,
            RelationshipFactory<Relationship> factory) {
        NodeCursor node = transaction.ambientNodeCursor();
        transaction.dataRead().singleNode(nodeId, node);
        if (!node.next()) {
            throw new NotFoundException(format("Node %d not found", nodeId));
        }

        var cursorContext = transaction.cursorContext();
        var cursors = transaction.cursors();
        return switch (direction) {
            case OUTGOING -> outgoingIterator(cursors, node, typeIds, factory, cursorContext);
            case INCOMING -> incomingIterator(cursors, node, typeIds, factory, cursorContext);
            case BOTH -> allIterator(cursors, node, typeIds, factory, cursorContext);
        };
    }

    private void singleNode(KernelTransaction transaction, NodeCursor nodes) {
        transaction.dataRead().singleNode(nodeId, nodes);
        if (!nodes.next()) {
            throw new NotFoundException(new EntityNotFoundException(EntityType.NODE, getElementId()));
        }
    }

    @Override
    public Relationship relationship(RelationshipTraversalCursor cursor) {
        return internalTransaction.newRelationshipEntity(cursor);
    }
}
