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
package org.neo4j.kernel.api.database.enrichment;

import static org.neo4j.util.Preconditions.checkState;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.api.factory.primitive.IntObjectMaps;
import org.eclipse.collections.api.factory.primitive.IntSets;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.neo4j.collection.trackable.HeapTrackingArrayList;
import org.neo4j.collection.trackable.HeapTrackingCollections;
import org.neo4j.collection.trackable.HeapTrackingLongIntHashMap;
import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.StorageEntityCursor;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.api.enrichment.CaptureMode;
import org.neo4j.storageengine.api.enrichment.Enrichment;
import org.neo4j.storageengine.api.enrichment.EnrichmentCommand;
import org.neo4j.storageengine.api.enrichment.EnrichmentCommandFactory;
import org.neo4j.storageengine.api.enrichment.EnrichmentTxStateVisitor;
import org.neo4j.storageengine.api.enrichment.TxMetadata;
import org.neo4j.storageengine.api.enrichment.WriteEnrichmentChannel;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.RelationshipModifications;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.values.storable.Value;

/**
 * Captures the changes to node and relationship entities during a transaction and ensures that they can be in the
 * following iterator order:
 * <ul>
 *  <li>+ node
 *  <li>+ rel
 *  <li>+- node labels/props
 *  <li>+- rel props
 *  <li>- node
 *  <li>- rel
 * </ul>
 * This is different to the order from the visitor calls, which is as follows:
 * <ul>
 *  <li>+ node
 *  <li>+ rel
 *  <li>- rel
 *  <li>- node
 *  <li>+- node props
 *  <li>+- node labels
 *  <li>+- rel props
 * </ul>
 * The visitor builds up the changes within 4 distinct buffers:
 * <ul>
 *  <li>the participants / entities channel: contains the pointers to any nodes/relationships featured during the transaction
 *  <li>the details channel: contains the details around the entity changes and pointers to those changes
 *  <p>For nodes, the layout is iiiiiiii e d cccc xxxx pppp yyyy llll</p>
 *  <p>For relationships, the layout is: iiiiiiii e d cccc xxxx pppp rrrr ssss tttt</p>
 *  <p> Where i = ID, e = entity type, d = delta type, c = constraints, x = property state, p = property changes,
 *  y = label state, l = label changes, r = relationship type, s = node source pointer, t = node target pointer.
 *  <p> The length of each identifier signifies the length in bytes
 *  <li>the changes channel: contains the change details around logical key constraints, label and property changes
 *  (and their value pointers)
 *  <li>the values channel: contains the values updated in the changes channel
 * </ul>
 */
public class TxEnrichmentVisitor extends TxStateVisitor.Delegator implements EnrichmentTxStateVisitor {

    public static final int NO_MORE_PROPERTIES = Integer.MIN_VALUE;

    public static final byte ADDED_MARKER = 0b001;
    public static final byte MODIFIED_MARKER = 0b010;
    public static final byte DELETED_MARKER = 0b100;

    public static final int UNKNOWN_POSITION = -1;

    // offset from the entity id in the entitiesBuffer
    private static final int TYPE_OFFSET = Long.BYTES;
    private static final int DELTA_OFFSET = TYPE_OFFSET + Byte.BYTES;
    private static final int CONSTRAINTS_OFFSET = DELTA_OFFSET + Byte.BYTES;
    private static final int PROPERTIES_STATE_OFFSET = CONSTRAINTS_OFFSET + Integer.BYTES;
    private static final int PROPERTIES_CHANGE_OFFSET = PROPERTIES_STATE_OFFSET + Integer.BYTES;
    private static final int LABELS_STATE_OFFSET = PROPERTIES_CHANGE_OFFSET + Integer.BYTES;
    private static final int LABELS_CHANGE_OFFSET = LABELS_STATE_OFFSET + Integer.BYTES;

    public static final long NODE_SIZE = LABELS_CHANGE_OFFSET + Integer.BYTES;

    private final CaptureMode captureMode;
    private final String serverId;
    private final KernelVersion kernelVersion;
    private final EnrichmentCommandFactory enrichmentCommandFactory;
    private final ReadableTransactionState txState;
    private final long lastTransactionIdWhenStarted;
    private final StorageReader store;
    private final MemoryTracker memoryTracker;

    private final WriteEnrichmentChannel participantsChannel;
    private final WriteEnrichmentChannel detailsChannel;
    private final WriteEnrichmentChannel changesChannel;
    private final ValuesChannel valuesChannel;
    private final ValuesChannel metadataChannel;

    private final HeapTrackingArrayList<Participant> participants;
    private final HeapTrackingLongIntHashMap nodePositions;
    private final HeapTrackingLongIntHashMap relationshipPositions;
    private final StorageNodeCursor nodeCursor;
    private final StorageRelationshipScanCursor relCursor;
    private final StoragePropertyCursor propertiesCursor;

    public TxEnrichmentVisitor(
            TxStateVisitor parent,
            CaptureMode captureMode,
            String serverId,
            KernelVersionProvider kernelVersionProvider,
            EnrichmentCommandFactory enrichmentCommandFactory,
            ReadableTransactionState txState,
            Map<String, Object> userMetadata,
            long lastTransactionIdWhenStarted,
            StorageReader store,
            CursorContext cursorContext,
            StoreCursors storeCursors,
            MemoryTracker memoryTracker) {
        super(parent);
        this.captureMode = captureMode;
        this.serverId = serverId;
        this.kernelVersion = kernelVersionProvider.kernelVersion();
        this.enrichmentCommandFactory = enrichmentCommandFactory;
        this.txState = txState;
        this.lastTransactionIdWhenStarted = lastTransactionIdWhenStarted;
        this.store = store;
        this.memoryTracker = memoryTracker;

        this.participantsChannel = new WriteEnrichmentChannel(memoryTracker);
        this.detailsChannel = new WriteEnrichmentChannel(memoryTracker);
        this.changesChannel = new WriteEnrichmentChannel(memoryTracker);
        this.valuesChannel = new ValuesChannel(memoryTracker);

        this.participants = HeapTrackingCollections.newArrayList(memoryTracker);
        this.nodePositions = HeapTrackingCollections.newLongIntMap(memoryTracker);
        this.relationshipPositions = HeapTrackingCollections.newLongIntMap(memoryTracker);

        this.nodeCursor = store.allocateNodeCursor(cursorContext, storeCursors, memoryTracker);
        this.relCursor = store.allocateRelationshipScanCursor(cursorContext, storeCursors, memoryTracker);
        this.propertiesCursor = store.allocatePropertyCursor(cursorContext, storeCursors, memoryTracker);

        if (kernelVersion.isAtLeast(KernelVersion.VERSION_CDC_USER_METADATA_INTRODUCED)) {
            this.metadataChannel = new ValuesChannel(memoryTracker);
            if (!userMetadata.isEmpty()) {
                metadataChannel.writer.writeInteger(userMetadata.size());
                userMetadata.forEach((key, value) -> {
                    metadataChannel.writer.writeString(key);
                    metadataChannel.writer.write(ValueUtils.of(value));
                });
            }
        } else {
            this.metadataChannel = null;
        }
    }

    @Override
    public void visitCreatedNode(long id) {
        super.visitCreatedNode(id);
        setNodeChangeType(id, DeltaType.ADDED);
    }

    @Override
    public void visitDeletedNode(long id) {
        super.visitDeletedNode(id);
        captureNodeState(id, DeltaType.DELETED, true);
    }

    @Override
    public void visitRelationshipModifications(RelationshipModifications modifications)
            throws ConstraintValidationException {
        super.visitRelationshipModifications(modifications);
        modifications.creations().forEach((relationshipId, typeId, startNodeId, endNodeId, addedProperties) -> {
            final var startPos =
                    captureNodeState(startNodeId, DeltaType.STATE, txState.nodeIsModifiedInThisBatch(startNodeId));
            final var endPos =
                    captureNodeState(endNodeId, DeltaType.STATE, txState.nodeIsModifiedInThisBatch(endNodeId));
            setRelationshipChangeType(relationshipId, DeltaType.ADDED, typeId, startPos, endPos);
            captureRelTypeConstraints(relationshipId, typeId);
            setRelationshipChangeDelta(
                    relationshipId, ChangeType.PROPERTIES_STATE, captureRelationshipState(addedProperties));
        });

        modifications.deletions().forEach((relationshipId, typeId, startNodeId, endNodeId, addedProperties) -> {
            final var startPos =
                    captureNodeState(startNodeId, DeltaType.STATE, txState.nodeIsModifiedInThisBatch(startNodeId));
            final var endPos =
                    captureNodeState(endNodeId, DeltaType.STATE, txState.nodeIsModifiedInThisBatch(endNodeId));
            setRelationshipChangeType(relationshipId, DeltaType.DELETED, typeId, startPos, endPos);
            captureRelTypeConstraints(relationshipId, typeId);
            captureRelationshipState(relationshipId, typeId, startNodeId, endNodeId, PropertySelection.ALL_PROPERTIES);
        });
    }

    @Override
    public void visitNodeLabelChanges(long id, LongSet added, LongSet removed) throws ConstraintValidationException {
        super.visitNodeLabelChanges(id, added, removed);
        captureNodeState(id, DeltaType.MODIFIED, true);

        final var position = changesChannel.size();
        final var addedInThisBatch = txState.nodeIsAddedInThisBatch(id);
        if (addedInThisBatch) {
            final var labels = toIntArray(added);
            addLabels(labels);
            captureLabelConstraints(id, labels);
        } else {
            addLabels(toIntArray(added));
            addLabels(toIntArray(removed));
        }

        setNodeChangeDelta(id, addedInThisBatch ? ChangeType.LABELS_STATE : ChangeType.LABELS_CHANGE, position);
    }

    @Override
    public void visitNodePropertyChanges(
            long id, Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed)
            throws ConstraintValidationException {
        super.visitNodePropertyChanges(id, added, changed, removed);
        captureNodeState(id, DeltaType.MODIFIED, true);

        if (txState.nodeIsAddedInThisBatch(id)) {
            setNodeChangeDelta(id, ChangeType.PROPERTIES_STATE, addNewNodeProperties(added));
        } else {
            final var position = changesChannel.size();
            nodeCursor.single(id);

            final var changesFlag = entityProperties(nodeCursor, added, changed, removed);
            if (changesFlag == 0) {
                setNodeChangeDelta(id, ChangeType.PROPERTIES_CHANGE, UNKNOWN_POSITION);
            } else {
                changesChannel.put(position, changesFlag);
                setNodeChangeDelta(id, ChangeType.PROPERTIES_CHANGE, position);
            }
        }
    }

    @Override
    public void visitRelPropertyChanges(
            long id,
            int type,
            long startNode,
            long endNode,
            Iterable<StorageProperty> added,
            Iterable<StorageProperty> changed,
            IntIterable removed)
            throws ConstraintValidationException {
        super.visitRelPropertyChanges(id, type, startNode, endNode, added, changed, removed);

        checkState(!relationshipPositions.containsKey(id), "Already tracking the relationship: " + id);

        final var startPos = captureNodeState(startNode, DeltaType.STATE, txState.nodeIsModifiedInThisBatch(startNode));
        final var endPos = captureNodeState(endNode, DeltaType.STATE, txState.nodeIsModifiedInThisBatch(endNode));
        setRelationshipChangeType(id, DeltaType.MODIFIED, type, startPos, endPos);

        // always capture constraints - so don't inline this
        final var constraintProps = captureRelTypeConstraints(id, type);
        final var selection =
                (captureMode == CaptureMode.FULL) ? PropertySelection.ALL_PROPERTIES : selection(constraintProps);
        captureRelationshipState(id, type, startNode, endNode, selection);

        final var position = changesChannel.size();
        relCursor.single(id, startNode, type, endNode);

        final var changesFlag = entityProperties(relCursor, added, changed, removed);
        if (changesFlag == 0) {
            setRelationshipChangeDelta(id, ChangeType.PROPERTIES_CHANGE, UNKNOWN_POSITION);
        } else {
            changesChannel.put(position, changesFlag);
            setRelationshipChangeDelta(id, ChangeType.PROPERTIES_CHANGE, position);
        }
    }

    private byte entityProperties(
            StorageEntityCursor entityCursor,
            Iterable<StorageProperty> added,
            Iterable<StorageProperty> changed,
            IntIterable removed) {
        var changesFlag = (byte) 0;
        if (entityCursor.next()) {
            changesFlag |= entityPropertyAdditions(added) ? ADDED_MARKER : 0;
            changesFlag |= entityPropertyChanges(entityCursor, changed, changesFlag > 0) ? MODIFIED_MARKER : 0;
            changesFlag |= entityPropertyDeletes(entityCursor, removed, changesFlag > 0) ? DELETED_MARKER : 0;
        }

        return changesFlag;
    }

    @Override
    public EnrichmentCommand command(SecurityContext securityContext) {
        if (ensureParticipantsWritten()) {
            final var metadata =
                    TxMetadata.create(captureMode, serverId, securityContext, lastTransactionIdWhenStarted);

            final Enrichment.Write enrichment;
            if (metadataChannel == null) {
                enrichment = Enrichment.Write.createV5_8(
                        metadata, participantsChannel, detailsChannel, changesChannel, valuesChannel.channel);
            } else {
                enrichment = Enrichment.Write.createV5_12(
                        metadata,
                        participantsChannel,
                        detailsChannel,
                        changesChannel,
                        valuesChannel.channel,
                        metadataChannel.channel);
            }

            return enrichmentCommandFactory.create(kernelVersion, enrichment);
        }

        return null;
    }

    @Override
    public void close() throws KernelException {
        IOUtils.closeAllUnchecked(
                this::ensureParticipantsWritten,
                TxEnrichmentVisitor.super::close,
                nodeCursor,
                relCursor,
                propertiesCursor,
                participants,
                nodePositions,
                relationshipPositions);
    }

    private boolean ensureParticipantsWritten() {
        if (!participants.isEmpty() && participantsChannel.isEmpty()) {
            Collections.sort(participants);
            for (var participant : participants) {
                participantsChannel.putInt(participant.position);
            }

            // and clear so we don't re-enter
            participants.clear();
            // also flip all the buffers ready for the command creation
            participantsChannel.flip();
            detailsChannel.flip();
            changesChannel.flip();
            valuesChannel.flip();
            if (metadataChannel != null) {
                metadataChannel.flip();
            }
            return true;
        }

        return !participantsChannel.isEmpty();
    }

    private boolean setNodeChangeType(long id, DeltaType deltaType) {
        final var beforePos = detailsChannel.size();
        final var position = nodePositions.getIfAbsentPut(id, beforePos);
        if (position == beforePos) {
            // new entry at end of the channel
            if (deltaType != DeltaType.STATE) {
                participants.add(createParticipant(EntityType.NODE, deltaType, id, position));
            }

            detailsChannel
                    .putLong(id)
                    .put(EntityType.NODE.id())
                    .put(deltaType.id())
                    .putInt(UNKNOWN_POSITION)
                    .putInt(UNKNOWN_POSITION)
                    .putInt(UNKNOWN_POSITION)
                    .putInt(UNKNOWN_POSITION)
                    .putInt(UNKNOWN_POSITION);
            return true;
        } else {
            // already tracked this node but as a different change type, ex. node state for a deleted relationship
            final var currentType = deltaType(detailsChannel.peek(position + DELTA_OFFSET));
            if (deltaType.id() < currentType.id()) {
                // this ensures that STATE is replaced (ex to MODIFIED) but not a true change (ex ADDED to MODIFIED)
                participants.add(createParticipant(EntityType.NODE, deltaType, id, position));

                detailsChannel.put(position + DELTA_OFFSET, deltaType.id());
            }
            return false;
        }
    }

    private void setNodeChangeDelta(long id, ChangeType changeType, int changePosition) {
        final var position = nodePositions.getIfAbsent(id, UNKNOWN_POSITION);
        checkState(position != UNKNOWN_POSITION, "Not yet tracking the node: " + id);

        final var offset =
                switch (changeType) {
                    case CONSTRAINTS -> CONSTRAINTS_OFFSET;
                    case PROPERTIES_STATE -> PROPERTIES_STATE_OFFSET;
                    case PROPERTIES_CHANGE -> PROPERTIES_CHANGE_OFFSET;
                    case LABELS_STATE -> LABELS_STATE_OFFSET;
                    case LABELS_CHANGE -> LABELS_CHANGE_OFFSET;
                };
        detailsChannel.putInt(position + offset, changePosition);
    }

    private void setRelationshipChangeType(long id, DeltaType deltaType, int type, int sourcePos, int targetPos) {
        // relationships entries are always appended at end of the channel
        participants.add(createParticipant(EntityType.RELATIONSHIP, deltaType, id, detailsChannel.size()));

        relationshipPositions.put(id, detailsChannel.size());
        detailsChannel
                .putLong(id)
                .put(EntityType.RELATIONSHIP.id())
                .put(deltaType.id())
                .putInt(UNKNOWN_POSITION)
                .putInt(UNKNOWN_POSITION)
                .putInt(UNKNOWN_POSITION)
                .putInt(type)
                .putInt(sourcePos)
                .putInt(targetPos);
    }

    private void setRelationshipChangeDelta(long id, ChangeType changeType, int changePosition) {
        final var position = relationshipPositions.getIfAbsent(id, UNKNOWN_POSITION);
        checkState(position != UNKNOWN_POSITION, "Not yet tracking the entity: " + id);

        final var offset =
                switch (changeType) {
                    case CONSTRAINTS -> CONSTRAINTS_OFFSET;
                    case PROPERTIES_STATE -> PROPERTIES_STATE_OFFSET;
                    case PROPERTIES_CHANGE -> PROPERTIES_CHANGE_OFFSET;
                    default -> throw new IllegalStateException("Relationships do not have labels");
                };
        detailsChannel.putInt(position + offset, changePosition);
    }

    private PropertySelection selection(IntSet constraintProps) {
        return constraintProps.isEmpty() ? null : PropertySelection.selection(constraintProps.toArray());
    }

    private int captureNodeState(long id, DeltaType deltaType, boolean asPartOfNodeChange) {
        if (setNodeChangeType(id, deltaType) && !txState.nodeIsAddedInThisBatch(id)) {
            nodeCursor.single(id);
            if (nodeCursor.next()) {
                final var labels = toSortedIntArray(nodeCursor.labels());
                final var constraintProps = captureLabelConstraints(id, labels);
                setNodeChangeDelta(id, ChangeType.LABELS_STATE, addLabels(labels));

                final PropertySelection selection;
                if (txState.nodeIsDeletedInThisBatch(id) || (asPartOfNodeChange && captureMode == CaptureMode.FULL)) {
                    selection = PropertySelection.ALL_PROPERTIES;
                } else {
                    selection = selection(constraintProps);
                }

                setNodeChangeDelta(
                        id, ChangeType.PROPERTIES_STATE, addPropertiesFromCursor(EntityType.NODE, selection));
            }
        }

        return nodePositions.get(id);
    }

    private IntSet captureLabelConstraints(long id, int... labels) {
        if (labels.length == 0) {
            return IntSets.immutable.empty();
        }

        var constraintGroupsAdded = 0;
        final var constraintProps = IntSets.mutable.empty();
        final var constraintsPosition = changesChannel.size();
        for (var label : labels) {
            final var logicalPropsArray = store.constraintsGetPropertyTokensForLogicalKey(label, EntityType.NODE);
            if (logicalPropsArray.length > 0) {
                if (constraintGroupsAdded == 0) {
                    // write a dummy constraints count now - will be updated when all have been added
                    changesChannel.putInt(0);
                }

                changesChannel.putInt(label).putInt(logicalPropsArray.length);
                for (var logicalProps : logicalPropsArray) {
                    writeConstraints(constraintProps, logicalProps);
                }

                constraintGroupsAdded++;
            }
        }

        if (constraintGroupsAdded > 0) {
            changesChannel.putInt(constraintsPosition, constraintGroupsAdded);
            setNodeChangeDelta(id, ChangeType.CONSTRAINTS, constraintsPosition);
        }

        return constraintProps;
    }

    private void captureRelationshipState(
            long id, int type, long startNode, long endNode, PropertySelection selection) {
        relCursor.single(id, startNode, type, endNode);
        if (relCursor.next()) {
            setRelationshipChangeDelta(
                    id, ChangeType.PROPERTIES_STATE, addPropertiesFromCursor(EntityType.RELATIONSHIP, selection));
        }
    }

    private IntSet captureRelTypeConstraints(long id, int relType) {
        final var constraintProps = IntSets.mutable.empty();
        final var logicalPropsArrays =
                store.constraintsGetPropertyTokensForLogicalKey(relType, EntityType.RELATIONSHIP);
        if (logicalPropsArrays.length > 0) {
            final var constraintsPosition = changesChannel.size();
            changesChannel.putInt(relType).putInt(logicalPropsArrays.length);
            for (var logicalProps : logicalPropsArrays) {
                writeConstraints(constraintProps, logicalProps);
            }

            setRelationshipChangeDelta(id, ChangeType.CONSTRAINTS, constraintsPosition);
        }

        return constraintProps;
    }

    private int captureRelationshipState(Iterable<StorageProperty> properties) {
        final var iterator = properties.iterator();
        if (!iterator.hasNext()) {
            return UNKNOWN_POSITION;
        }

        final var position = changesChannel.size();
        while (iterator.hasNext()) {
            final var property = iterator.next();
            final var propertyKeyId = property.propertyKeyId();
            captureProperty(propertyKeyId, property.value());
        }

        changesChannel.putInt(NO_MORE_PROPERTIES);
        return position;
    }

    private void writeConstraints(MutableIntSet allConstraintProps, IntSet logicalProps) {
        allConstraintProps.addAll(logicalProps);
        changesChannel.putInt(logicalProps.size());
        logicalProps.forEach(changesChannel::putInt);
    }

    private int addNewNodeProperties(Iterable<StorageProperty> properties) {
        final var iterator = properties.iterator();
        if (!iterator.hasNext()) {
            return UNKNOWN_POSITION;
        }

        final var position = changesChannel.size();
        while (iterator.hasNext()) {
            final var property = iterator.next();
            captureProperty(property.propertyKeyId(), property.value());
        }

        changesChannel.putInt(NO_MORE_PROPERTIES);
        return position;
    }

    private int addPropertiesFromCursor(EntityType entityType, PropertySelection selection) {
        if (selection == null) {
            return UNKNOWN_POSITION;
        }

        if (entityType == EntityType.NODE) {
            propertiesCursor.initNodeProperties(nodeCursor, selection);
        } else {
            propertiesCursor.initRelationshipProperties(relCursor, selection);
        }

        final var position = changesChannel.size();
        var captured = 0;
        while (propertiesCursor.next()) {
            final var property = propertiesCursor.propertyKey();
            captureProperty(property, propertiesCursor.propertyValue());
            captured++;
        }

        if (captured == 0) {
            return UNKNOWN_POSITION;
        }

        changesChannel.putInt(NO_MORE_PROPERTIES);
        return position;
    }

    private void captureProperty(int property, Value value) {
        changesChannel.putInt(property);
        changesChannel.putInt(valuesChannel.write(value));
    }

    private int addLabels(int... labels) {
        final var position = changesChannel.size();
        changesChannel.putInt(labels.length);
        for (var label : labels) {
            changesChannel.putInt(label);
        }

        return position;
    }

    private boolean entityPropertyAdditions(Iterable<StorageProperty> properties) {
        var captured = 0;
        for (var property : properties) {
            if (captured == 0) {
                changesChannel.put((byte) 0);
            }

            changesChannel.putInt(property.propertyKeyId());
            changesChannel.putInt(valuesChannel.write(property.value()));
            captured++;
        }

        if (captured > 0) {
            changesChannel.putInt(NO_MORE_PROPERTIES);
            return true;
        }

        return false;
    }

    private boolean entityPropertyChanges(
            StorageEntityCursor cursor, Iterable<StorageProperty> properties, boolean addedChangesMarker) {
        var captured = 0;
        final var propertyValues = IntObjectMaps.mutable.<Value>empty();
        for (var property : properties) {
            propertyValues.put(property.propertyKeyId(), property.value());
        }

        if (propertyValues.isEmpty()) {
            return false;
        }

        cursor.properties(
                propertiesCursor,
                PropertySelection.selection(propertyValues.keySet().toArray()));
        while (propertiesCursor.next()) {
            if (captured == 0 && !addedChangesMarker) {
                changesChannel.put((byte) 0);
            }

            final var propertyId = propertiesCursor.propertyKey();
            changesChannel.putInt(propertyId);
            changesChannel.putInt(valuesChannel.write(propertiesCursor.propertyValue()));
            changesChannel.putInt(valuesChannel.write(propertyValues.get(propertyId)));
            captured++;
        }

        if (captured > 0) {
            changesChannel.putInt(NO_MORE_PROPERTIES);
            return true;
        }

        return false;
    }

    private boolean entityPropertyDeletes(
            StorageEntityCursor cursor, IntIterable properties, boolean addedChangesMarker) {
        final var propertyIds = properties.toArray();
        if (propertyIds.length == 0) {
            return false;
        }

        var captured = 0;
        cursor.properties(propertiesCursor, PropertySelection.selection(propertyIds));
        while (propertiesCursor.next()) {
            if (captured == 0 && !addedChangesMarker) {
                changesChannel.put((byte) 0);
            }

            changesChannel.putInt(propertiesCursor.propertyKey());
            changesChannel.putInt(valuesChannel.write(propertiesCursor.propertyValue()));
            captured++;
        }

        if (captured > 0) {
            changesChannel.putInt(NO_MORE_PROPERTIES);
            return true;
        }

        return false;
    }

    private Participant createParticipant(EntityType entityType, DeltaType deltaType, long id, int position) {
        // we want to order based on delta type then on entity type (fallback to id after that)
        // however, we need to reverse the order for deletes to make detach-delete operations easier
        // i.e. rel first rather than node first
        var orderCode = (short) (deltaType.id() << Byte.SIZE);
        if (deltaType == DeltaType.DELETED) {
            orderCode |= (short) ((entityType == EntityType.NODE) ? 1 : 0);
        } else {
            orderCode |= (short) ((entityType == EntityType.NODE) ? 0 : 1);
        }

        memoryTracker.allocateHeap(Participant.SIZE);
        return new Participant(orderCode, id, position);
    }

    private static DeltaType deltaType(byte flag) {
        return DeltaType.BY_ID.get(flag);
    }

    private static int[] toIntArray(LongSet ids) {
        return toIntArray(ids.toSortedArray());
    }

    private static int[] toSortedIntArray(int[] data) {
        Arrays.sort(data);
        return data;
    }

    private static int[] toIntArray(long[] sorted) {
        final var tokens = new int[sorted.length];
        for (var i = 0; i < tokens.length; i++) {
            tokens[i] = (int) sorted[i];
        }
        return tokens;
    }

    private static class ValuesChannel {

        private final WriteEnrichmentChannel channel;
        private final ValuesWriter writer;

        private ValuesChannel(MemoryTracker memoryTracker) {
            this.channel = new WriteEnrichmentChannel(memoryTracker);
            this.writer = new ValuesWriter(channel);
        }

        private void flip() {
            channel.flip();
        }

        private int write(Value value) {
            return writer.write(value);
        }
    }

    @SuppressWarnings("ClassCanBeRecord") // HeapEstimator doesn't work on records
    private static class Participant implements Comparable<Participant> {
        private static final long SIZE = HeapEstimator.shallowSizeOfInstance(Participant.class);

        private final short orderCode;
        private final long id;
        private final int position;

        private Participant(short orderCode, long id, int position) {
            this.orderCode = orderCode;
            this.id = id;
            this.position = position;
        }

        @Override
        public int compareTo(Participant other) {
            final var c = Short.compare(orderCode, other.orderCode);
            return c == 0 ? Long.compare(id, other.id) : c;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Participant that = (Participant) o;
            return orderCode == that.orderCode && id == that.id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(orderCode, id);
        }
    }
}
