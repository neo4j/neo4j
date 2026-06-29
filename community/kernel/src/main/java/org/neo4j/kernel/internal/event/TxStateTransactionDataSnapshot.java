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
package org.neo4j.kernel.internal.event;

import static org.neo4j.collection.trackable.HeapTrackingCollections.newArrayList;
import static org.neo4j.collection.trackable.HeapTrackingCollections.newLongObjectMap;
import static org.neo4j.storageengine.api.PropertySelection.ALL_PROPERTIES;
import static org.neo4j.storageengine.api.PropertySelection.ALL_PROPERTY_KEYS;
import static org.neo4j.storageengine.api.PropertySelection.onlyKeysSelection;
import static org.neo4j.storageengine.api.PropertySelection.selection;
import static org.neo4j.values.storable.Values.NO_VALUE;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.neo4j.collection.diffset.IntDiffSets;
import org.neo4j.collection.trackable.HeapTrackingArrayList;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.core.NodeEntity;
import org.neo4j.kernel.impl.core.RelationshipEntity;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageEntityCursor;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;
import org.neo4j.storageengine.api.txstate.NodeState;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.RelationshipState;
import org.neo4j.values.storable.Value;

/**
 * Transform for {@link org.neo4j.storageengine.api.txstate.ReadableTransactionState} to make it accessible as {@link TransactionData}.
 */
public class TxStateTransactionDataSnapshot implements TransactionData, AutoCloseable {
    private final ReadableTransactionState state;
    private final StorageReader store;
    private final KernelTransaction transaction;

    private final HeapTrackingArrayList<PropertyEntry<Node>> assignedNodeProperties;
    private final HeapTrackingArrayList<PropertyEntry<Relationship>> assignedRelationshipProperties;
    private final HeapTrackingArrayList<LabelEntry> assignedLabels;

    private final HeapTrackingArrayList<PropertyEntry<Node>> removedNodeProperties;
    private final HeapTrackingArrayList<PropertyEntry<Relationship>> removedRelationshipProperties;
    private final HeapTrackingArrayList<LabelEntry> removedLabels;
    private final MutableLongObjectMap<RelationshipEntity> relationshipsReadFromStore;
    private final StorageRelationshipScanCursor relationship;
    private final InternalTransaction internalTransaction;
    private final MemoryTracker memoryTracker;
    private final Set<DataSelection> dataSelection;
    private final boolean isLast;

    /**
     * Creates a new snapshot of the given transaction state.
     * @param state transaction state to snapshot.
     * @param storageReader storage reader for reading additional data from storage, if required.
     * @param transaction the actual transaction.
     * @param isLast whether it's the last part of the transaction.
     * @param dataSelection specific set of data items to include for this snapshot,
     * or {@code null} if all data should be included.
     */
    TxStateTransactionDataSnapshot(
            ReadableTransactionState state,
            StorageReader storageReader,
            KernelTransaction transaction,
            boolean isLast,
            Set<DataSelection> dataSelection) {
        this.state = state;
        this.store = storageReader;
        this.transaction = transaction;
        this.isLast = isLast;
        this.internalTransaction = transaction.internalTransaction();
        this.memoryTracker = transaction.memoryTracker();
        this.dataSelection = dataSelection;
        this.relationship = storageReader.allocateRelationshipScanCursor(
                transaction.cursorContext(), transaction.storeCursors(), memoryTracker);
        this.relationshipsReadFromStore = newLongObjectMap(memoryTracker);
        this.removedLabels = newArrayList(memoryTracker);
        this.removedRelationshipProperties = newArrayList(memoryTracker);
        this.removedNodeProperties = newArrayList(memoryTracker);

        this.assignedLabels = newArrayList(memoryTracker);
        this.assignedRelationshipProperties = newArrayList(memoryTracker);
        this.assignedNodeProperties = newArrayList(memoryTracker);

        // Load changes that require store access eagerly, because we won't have access to the after-state
        // after the tx has been committed.
        takeSnapshot(memoryTracker);
    }

    @Override
    public Iterable<Node> createdNodes() {
        return map2Nodes(state.addedAndRemovedNodes().getAdded());
    }

    @Override
    public Iterable<Node> deletedNodes() {
        return map2Nodes(state.addedAndRemovedNodes().getRemoved());
    }

    @Override
    public Iterable<Relationship> createdRelationships() {
        return map2Rels(state.addedAndRemovedRelationships().getAdded());
    }

    @Override
    public Iterable<Relationship> deletedRelationships() {
        return map2Rels(state.addedAndRemovedRelationships().getRemoved());
    }

    @Override
    public boolean isDeleted(Node node) {
        return state.nodeIsDeletedInThisBatch(node.getId());
    }

    @Override
    public boolean isDeleted(Relationship relationship) {
        return state.relationshipIsDeletedInThisBatch(relationship.getId());
    }

    @Override
    public Iterable<PropertyEntry<Node>> assignedNodeProperties() {
        return assignedNodeProperties;
    }

    @Override
    public Iterable<PropertyEntry<Node>> removedNodeProperties() {
        return removedNodeProperties;
    }

    @Override
    public Iterable<PropertyEntry<Relationship>> assignedRelationshipProperties() {
        return assignedRelationshipProperties;
    }

    @Override
    public Iterable<PropertyEntry<Relationship>> removedRelationshipProperties() {
        return removedRelationshipProperties;
    }

    @Override
    public String username() {
        return transaction.securityContext().subject().executingUser();
    }

    @Override
    public Map<String, Object> metaData() {
        return transaction.getMetaData();
    }

    @Override
    public Iterable<LabelEntry> removedLabels() {
        return removedLabels;
    }

    @Override
    public Iterable<LabelEntry> assignedLabels() {
        return assignedLabels;
    }

    @Override
    public long getTransactionId() {
        return transaction.getTransactionId();
    }

    @Override
    public long getCommitTime() {
        return transaction.getCommitTime();
    }

    @Override
    public long transactionIdentityNumber() {
        return transaction.getTransactionSequenceNumber();
    }

    @Override
    public boolean isLast() {
        return isLast;
    }

    @Override
    public boolean hasDataChanges() {
        return state.hasDataChanges();
    }

    private void takeSnapshot(MemoryTracker memoryTracker) {
        var cursorContext = transaction.cursorContext();
        var storeCursors = transaction.storeCursors();
        try (StorageNodeCursor node = store.allocateNodeCursor(cursorContext, storeCursors, memoryTracker);
                StoragePropertyCursor properties =
                        store.allocatePropertyCursor(cursorContext, storeCursors, memoryTracker)) {
            TokenRead tokenRead = transaction.tokenRead();
            snapshotRemovedNodes(memoryTracker, node, properties, tokenRead);
            snapshotRemovedRelationships(memoryTracker, properties, tokenRead);
            snapshotModifiedNodes(memoryTracker, node, properties, tokenRead);
            snapshotModifiedRelationships(memoryTracker, properties, tokenRead);
        } catch (PropertyKeyIdNotFoundKernelException e) {
            throw new IllegalStateException("An entity that does not exist was modified.", e);
        }
    }

    private boolean shouldInclude(DataSelection selection) {
        return dataSelection == null || dataSelection.contains(selection);
    }

    private void snapshotModifiedRelationships(
            MemoryTracker memoryTracker, StoragePropertyCursor properties, TokenRead tokenRead)
            throws PropertyKeyIdNotFoundKernelException {
        boolean includeRemovedPropertyValues = shouldInclude(DataSelection.removedPropertyValues);
        boolean includeReplacedPropertyValues = shouldInclude(DataSelection.replacedPropertyValues);
        boolean includeRemovedPropertyKeys = includeRemovedPropertyValues
                || includeReplacedPropertyValues
                || shouldInclude(DataSelection.removedPropertKeys);
        for (RelationshipState relState : state.modifiedRelationships()) {
            Relationship relationship = relationship(relState.getId());
            Iterator<StorageProperty> added = relState.addedProperties().iterator();
            while (added.hasNext()) {
                StorageProperty property = added.next();
                assignedRelationshipProperties.add(createRelationshipPropertyEntryView(
                        memoryTracker,
                        tokenRead,
                        relationship,
                        property.propertyKeyId(),
                        property.value(),
                        committedValue(
                                relState,
                                property.propertyKeyId(),
                                this.relationship,
                                properties,
                                includeRemovedPropertyKeys,
                                includeReplacedPropertyValues)));
            }
            relState.removedProperties().each(id -> {
                try {
                    var entryView = createRelationshipPropertyEntryView(
                            memoryTracker,
                            tokenRead,
                            relationship,
                            id,
                            null,
                            committedValue(
                                    relState,
                                    id,
                                    this.relationship,
                                    properties,
                                    includeRemovedPropertyKeys,
                                    includeRemovedPropertyValues));
                    removedRelationshipProperties.add(entryView);
                } catch (PropertyKeyIdNotFoundKernelException e) {
                    throw new IllegalStateException(
                            "Not existing properties was modified for relationship " + relState.getId(), e);
                }
            });
        }
    }

    private void snapshotModifiedNodes(
            MemoryTracker memoryTracker, StorageNodeCursor node, StoragePropertyCursor properties, TokenRead tokenRead)
            throws PropertyKeyIdNotFoundKernelException {
        boolean includeRemovedPropertyValues = shouldInclude(DataSelection.removedPropertyValues);
        boolean includeReplacedPropertyValues = shouldInclude(DataSelection.replacedPropertyValues);
        boolean includeRemovedPropertyKeys = includeRemovedPropertyValues
                || includeReplacedPropertyValues
                || shouldInclude(DataSelection.removedPropertKeys);
        for (NodeState nodeState : state.modifiedNodes()) {
            Iterator<StorageProperty> added = nodeState.addedProperties().iterator();
            long nodeId = nodeState.getId();
            while (added.hasNext()) {
                StorageProperty property = added.next();
                var entryView = createNodePropertyEntryView(
                        memoryTracker,
                        tokenRead,
                        nodeId,
                        property.propertyKeyId(),
                        property.value(),
                        committedValue(
                                nodeState,
                                property.propertyKeyId(),
                                node,
                                properties,
                                includeRemovedPropertyKeys,
                                includeReplacedPropertyValues));
                assignedNodeProperties.add(entryView);
            }
            nodeState.removedProperties().each(id -> {
                try {
                    removedNodeProperties.add(createNodePropertyEntryView(
                            memoryTracker,
                            tokenRead,
                            nodeId,
                            id,
                            null,
                            committedValue(
                                    nodeState,
                                    id,
                                    node,
                                    properties,
                                    includeRemovedPropertyKeys,
                                    includeRemovedPropertyValues)));
                } catch (PropertyKeyIdNotFoundKernelException e) {
                    throw new IllegalStateException("Not existing node properties was modified for node " + nodeId, e);
                }
            });

            final IntDiffSets labels = nodeState.labelDiffSets();
            addLabelEntriesTo(nodeId, labels.getAdded(), assignedLabels);
            addLabelEntriesTo(nodeId, labels.getRemoved(), removedLabels);
        }
    }

    private void snapshotRemovedRelationships(
            MemoryTracker memoryTracker, StoragePropertyCursor properties, TokenRead tokenRead) {
        boolean includeRemovedPropertyValues = shouldInclude(DataSelection.removedPropertyValues);
        boolean includeRemovedPropertyKeys =
                includeRemovedPropertyValues || shouldInclude(DataSelection.removedPropertKeys);
        state.addedAndRemovedRelationships().getRemoved().each(relId -> {
            if (!includeRemovedPropertyValues && !includeRemovedPropertyKeys) {
                // We don't need to include the properties for this deleted relationship. There's a chance
                // we can get the other meta-data directly from the tx-state - try that first.
                boolean foundInTxState = state.getRelationshipStateEvenDeleted(relId)
                        .accept((relationshipId, typeId, startNodeId, endNodeId) -> relationshipsReadFromStore.put(
                                relationshipId, (RelationshipEntity) internalTransaction.newRelationshipEntity(
                                        relationshipId, startNodeId, typeId, endNodeId)));
                if (foundInTxState) {
                    return;
                }
            }

            Relationship relationship = relationship(relId);
            this.relationship.single(relId);
            if (this.relationship.next()) {
                if (includeRemovedPropertyKeys) {
                    this.relationship.properties(
                            properties, includeRemovedPropertyValues ? ALL_PROPERTIES : ALL_PROPERTY_KEYS);
                    while (properties.next()) {
                        try {
                            removedRelationshipProperties.add(createRelationshipPropertyEntryView(
                                    memoryTracker,
                                    tokenRead,
                                    relationship,
                                    properties.propertyKey(),
                                    null,
                                    includeRemovedPropertyValues ? properties.propertyValue() : NO_VALUE));
                        } catch (PropertyKeyIdNotFoundKernelException e) {
                            throw new IllegalStateException(
                                    "Not existing node properties was modified for relationship " + relId, e);
                        }
                    }
                }
            }
        });
    }

    private void snapshotRemovedNodes(
            MemoryTracker memoryTracker,
            StorageNodeCursor node,
            StoragePropertyCursor properties,
            TokenRead tokenRead) {
        boolean includeDeletedNodeLabels = shouldInclude(DataSelection.deletedNodeLabels);
        boolean includeRemovedPropertyValues = shouldInclude(DataSelection.removedPropertyValues);
        boolean includeRemovedPropertyKeys =
                includeRemovedPropertyValues || shouldInclude(DataSelection.removedPropertKeys);
        if (!includeDeletedNodeLabels && !includeRemovedPropertyKeys) {
            return;
        }

        state.addedAndRemovedNodes().getRemoved().each(nodeId -> {
            node.single(nodeId);
            if (node.next()) {
                if (includeRemovedPropertyKeys) {
                    node.properties(properties, includeRemovedPropertyValues ? ALL_PROPERTIES : ALL_PROPERTY_KEYS);
                    while (properties.next()) {
                        try {
                            removedNodeProperties.add(createNodePropertyEntryView(
                                    memoryTracker,
                                    tokenRead,
                                    nodeId,
                                    properties.propertyKey(),
                                    null,
                                    includeRemovedPropertyValues ? properties.propertyValue() : NO_VALUE));
                        } catch (PropertyKeyIdNotFoundKernelException e) {
                            throw new IllegalStateException(
                                    "Not existing properties was modified for node " + nodeId, e);
                        }
                    }
                }

                if (includeDeletedNodeLabels) {
                    for (int labelId : node.labels()) {
                        try {
                            removedLabels.add(createLabelView(memoryTracker, tokenRead, nodeId, labelId));
                        } catch (LabelNotFoundKernelException e) {
                            throw new IllegalStateException("Not existing label was modified for node " + nodeId, e);
                        }
                    }
                }
            }
        });
    }

    @Override
    public void close() {
        relationship.close();
    }

    private void addLabelEntriesTo(long nodeId, IntSet labelIds, HeapTrackingArrayList<LabelEntry> target) {
        labelIds.each(labelId -> {
            try {
                target.add(createLabelView(memoryTracker, transaction.tokenRead(), nodeId, labelId));
            } catch (LabelNotFoundKernelException e) {
                throw new IllegalStateException("Not existing label was modified for node " + nodeId, e);
            }
        });
    }

    private static RelationshipPropertyEntryView createRelationshipPropertyEntryView(
            MemoryTracker memoryTracker,
            TokenRead tokenRead,
            Relationship relationship,
            int key,
            Value newValue,
            Value oldValue)
            throws PropertyKeyIdNotFoundKernelException {
        var entryView =
                new RelationshipPropertyEntryView(relationship, tokenRead.propertyKeyName(key), newValue, oldValue);
        memoryTracker.allocateHeap(RelationshipPropertyEntryView.SHALLOW_SIZE);
        if (oldValue != null) {
            memoryTracker.allocateHeap(oldValue.estimatedHeapUsage());
        }
        return entryView;
    }

    private NodePropertyEntryView createNodePropertyEntryView(
            MemoryTracker memoryTracker, TokenRead tokenRead, long nodeId, int key, Value newValue, Value oldValue)
            throws PropertyKeyIdNotFoundKernelException {
        memoryTracker.allocateHeap(NodePropertyEntryView.SHALLOW_SIZE);
        var entryView = new NodePropertyEntryView(
                internalTransaction, nodeId, tokenRead.propertyKeyName(key), newValue, oldValue);
        if (oldValue != null) {
            memoryTracker.allocateHeap(oldValue.estimatedHeapUsage());
        }
        return entryView;
    }

    private LabelEntryView createLabelView(MemoryTracker memoryTracker, TokenRead tokenRead, long nodeId, int labelId)
            throws LabelNotFoundKernelException {
        memoryTracker.allocateHeap(LabelEntryView.SHALLOW_SIZE);
        return new LabelEntryView(internalTransaction, nodeId, tokenRead.nodeLabelName(labelId));
    }

    private Relationship relationship(long relId) {
        RelationshipEntity relationship = (RelationshipEntity) internalTransaction.newRelationshipEntity(relId);
        if (!state.relationshipVisit(
                relId, relationship)) { // This relationship has been created or changed in this transaction
            RelationshipEntity cached = relationshipsReadFromStore.get(relId);
            if (cached != null) {
                return cached;
            }

            // Get this relationship data from the store
            this.relationship.single(relId);
            if (!this.relationship.next()) {
                throw new IllegalStateException(
                        "Getting deleted relationship data should have been covered by the tx state");
            }
            relationship.visit(
                    relId,
                    this.relationship.type(),
                    this.relationship.sourceNodeReference(),
                    this.relationship.targetNodeReference());
            memoryTracker.allocateHeap(RelationshipEntity.SHALLOW_SIZE);
            relationshipsReadFromStore.put(relId, relationship);
        }
        return relationship;
    }

    private Iterable<Node> map2Nodes(LongIterable ids) {
        return ids.asLazy().collect(id -> new NodeEntity(internalTransaction, id));
    }

    private Iterable<Relationship> map2Rels(LongIterable ids) {
        return ids.asLazy().collect(this::relationship);
    }

    private Value committedValue(
            NodeState nodeState,
            int property,
            StorageNodeCursor node,
            StoragePropertyCursor properties,
            boolean readPropertyKey,
            boolean readPropertyValue) {
        if (!readPropertyKey || state.nodeIsAddedInThisBatch(nodeState.getId())) {
            return NO_VALUE;
        }

        node.single(nodeState.getId());
        if (!node.next()) {
            return NO_VALUE;
        }
        return committedValue(properties, node, property, readPropertyValue);
    }

    private static Value committedValue(
            StoragePropertyCursor properties, StorageEntityCursor cursor, int propertyKey, boolean readPropertyValue) {
        if (readPropertyValue) {
            cursor.properties(properties, selection(propertyKey));
            return properties.next() ? properties.propertyValue() : null;
        }
        cursor.properties(properties, onlyKeysSelection(propertyKey));
        return properties.next() ? NO_VALUE : null;
    }

    private Value committedValue(
            RelationshipState relState,
            int property,
            StorageRelationshipScanCursor relationship,
            StoragePropertyCursor properties,
            boolean readPropertyKey,
            boolean readPropertyValue) {
        if (!readPropertyKey || state.relationshipIsAddedInThisBatch(relState.getId())) {
            return NO_VALUE;
        }

        relationship.single(relState.getId());
        if (!relationship.next()) {
            return NO_VALUE;
        }

        return committedValue(properties, relationship, property, readPropertyValue);
    }
}
