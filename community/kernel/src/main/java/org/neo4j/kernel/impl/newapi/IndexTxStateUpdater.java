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

import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;
import static org.neo4j.token.api.TokenConstants.ANY_PROPERTY_KEY;
import static org.neo4j.values.storable.Values.NO_VALUE;

import java.util.Arrays;
import java.util.Collection;
import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.factory.primitive.IntLists;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.neo4j.common.EntityType;
import org.neo4j.internal.kernel.api.EntityCursor;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;

/**
 * Utility class that performs necessary updates for the transaction state.
 */
public class IndexTxStateUpdater {
    private final StorageReader storageReader;
    private final TxStateHolder txStateHolder;
    private final IndexingService indexingService;

    // We can use the StorageReader directly instead of the SchemaReadOps, because we know that in transactions
    // where this class is needed we will never have index changes.
    public IndexTxStateUpdater(
            StorageReader storageReader, IndexingService indexingService, TxStateHolder txStateHolder) {
        this.storageReader = storageReader;
        this.txStateHolder = txStateHolder;
        this.indexingService = indexingService;
    }

    // LABEL CHANGES

    public enum LabelChangeType {
        ADDED_LABEL,
        REMOVED_LABEL
    }

    /**
     * A label has been changed, figure out what updates are needed to tx state.
     *
     * @param node cursor to the node where the change was applied
     * @param propertyCursor cursor to the properties of node
     * @param changeType The type of change event
     * @param indexes the indexes related to the node
     */
    void onLabelChange(
            NodeCursor node,
            PropertyCursor propertyCursor,
            LabelChangeType changeType,
            Collection<IndexDescriptor> indexes) {
        assert noSchemaChangedInTx();

        // Check all indexes of the changed label
        if (!indexes.isEmpty()) {
            MutableIntObjectMap<Value> materializedProperties = IntObjectMaps.mutable.empty();
            for (IndexDescriptor index : indexes) {
                MemoryTracker memoryTracker = txStateHolder.txState().memoryTracker();
                int[] indexPropertyIds = index.schema().getPropertyIds();
                Value[] values = getValueTuple(
                        node,
                        propertyCursor,
                        NO_SUCH_PROPERTY_KEY,
                        NO_VALUE,
                        indexPropertyIds,
                        materializedProperties,
                        memoryTracker);
                ValueTuple valueTuple = ValueTuple.of(values);
                memoryTracker.allocateHeap(valueTuple.getShallowSize());
                switch (changeType) {
                    case ADDED_LABEL -> {
                        indexingService.validateBeforeCommit(index, values, node.nodeReference());
                        txStateHolder
                                .txState()
                                .indexDoUpdateEntry(index.schema(), node.nodeReference(), null, valueTuple);
                    }
                    case REMOVED_LABEL -> txStateHolder
                            .txState()
                            .indexDoUpdateEntry(index.schema(), node.nodeReference(), valueTuple, null);
                }
            }
        }
    }

    void onPropertyAdd(
            NodeCursor node,
            PropertyCursor propertyCursor,
            int[] labels,
            int propertyKeyId,
            int[] existingPropertyKeyIds,
            Value value) {
        onPropertyAdd(node, NODE, propertyCursor, labels, propertyKeyId, existingPropertyKeyIds, value);
    }

    void onPropertyRemove(
            NodeCursor node,
            PropertyCursor propertyCursor,
            int[] labels,
            int propertyKeyId,
            int[] existingPropertyKeyIds,
            Value value) {
        onPropertyRemove(node, NODE, propertyCursor, labels, propertyKeyId, existingPropertyKeyIds, value);
    }

    void onPropertyChange(
            NodeCursor node,
            PropertyCursor propertyCursor,
            int[] labels,
            int propertyKeyId,
            int[] existingPropertyKeyIds,
            Value beforeValue,
            Value afterValue) {
        onPropertyChange(
                node, NODE, propertyCursor, labels, propertyKeyId, existingPropertyKeyIds, beforeValue, afterValue);
    }

    void onPropertyAdd(
            RelationshipScanCursor relationship,
            PropertyCursor propertyCursor,
            int type,
            int propertyKeyId,
            int[] existingPropertyKeyIds,
            Value value) {
        onPropertyAdd(
                relationship,
                RELATIONSHIP,
                propertyCursor,
                new int[] {type},
                propertyKeyId,
                existingPropertyKeyIds,
                value);
    }

    void onPropertyRemove(
            RelationshipScanCursor relationship,
            PropertyCursor propertyCursor,
            int type,
            int propertyKeyId,
            int[] existingPropertyKeyIds,
            Value value) {
        onPropertyRemove(
                relationship,
                RELATIONSHIP,
                propertyCursor,
                new int[] {type},
                propertyKeyId,
                existingPropertyKeyIds,
                value);
    }

    void onPropertyChange(
            RelationshipScanCursor relationship,
            PropertyCursor propertyCursor,
            int type,
            int propertyKeyId,
            int[] existingPropertyKeyIds,
            Value beforeValue,
            Value afterValue) {
        onPropertyChange(
                relationship,
                RELATIONSHIP,
                propertyCursor,
                new int[] {type},
                propertyKeyId,
                existingPropertyKeyIds,
                beforeValue,
                afterValue);
    }

    void onDeleteUncreated(NodeCursor node, PropertyCursor propertyCursor) {
        onDeleteUncreated(node, NODE, propertyCursor, node.labels().all());
    }

    void onDeleteUncreated(RelationshipScanCursor relationship, PropertyCursor propertyCursor) {
        onDeleteUncreated(relationship, RELATIONSHIP, propertyCursor, new int[] {relationship.type()});
    }

    private boolean noSchemaChangedInTx() {
        var txState = txStateHolder.txState();
        return !(txState.hasChanges() && !txState.hasDataChanges());
    }

    // PROPERTY CHANGES

    /**
     * Creating an entity with its data in a transaction adds also adds that state to index transaction state (for matching indexes).
     * When deleting an entity this method will delete this state from the index transaction state.
     *
     * @param entity entity that was deleted.
     * @param propertyCursor property cursor for accessing the properties of the entity.
     * @param tokens the entity tokens this entity has.
     */
    private void onDeleteUncreated(
            EntityCursor entity, EntityType entityType, PropertyCursor propertyCursor, int[] tokens) {
        assert noSchemaChangedInTx();
        entity.properties(propertyCursor, PropertySelection.ALL_PROPERTY_KEYS);
        MutableIntList propertyKeyList = IntLists.mutable.empty();
        while (propertyCursor.next()) {
            propertyKeyList.add(propertyCursor.propertyKey());
        }
        // Make sure to sort the propertyKeyIds since SchemaMatcher.onMatchingSchema requires it.
        int[] propertyKeyIds = propertyKeyList.toSortedArray();
        Collection<IndexDescriptor> indexes = storageReader.valueIndexesGetRelated(tokens, propertyKeyIds, entityType);
        if (!indexes.isEmpty()) {
            MutableIntObjectMap<Value> materializedProperties = IntObjectMaps.mutable.empty();
            SchemaMatcher.onMatchingSchema(indexes.iterator(), ANY_PROPERTY_KEY, propertyKeyIds, index -> {
                MemoryTracker memoryTracker = txStateHolder.txState().memoryTracker();
                SchemaDescriptor schema = index.schema();
                Value[] values = getValueTuple(
                        entity,
                        propertyCursor,
                        ANY_PROPERTY_KEY,
                        NO_VALUE,
                        schema.getPropertyIds(),
                        materializedProperties,
                        memoryTracker);
                ValueTuple valueTuple = ValueTuple.of(values);
                memoryTracker.allocateHeap(valueTuple.getShallowSize());
                txStateHolder.txState().indexDoUpdateEntry(schema, entity.reference(), valueTuple, null);
            });
        }
    }

    private void onPropertyAdd(
            EntityCursor entity,
            EntityType entityType,
            PropertyCursor propertyCursor,
            int[] tokens,
            int propertyKeyId,
            int[] existingPropertyKeyIds,
            Value value) {
        assert noSchemaChangedInTx();
        Collection<IndexDescriptor> indexes = storageReader.valueIndexesGetRelated(tokens, propertyKeyId, entityType);
        if (!indexes.isEmpty()) {
            MutableIntObjectMap<Value> materializedProperties = IntObjectMaps.mutable.empty();
            SchemaMatcher.onMatchingSchema(indexes.iterator(), propertyKeyId, existingPropertyKeyIds, index -> {
                MemoryTracker memoryTracker = txStateHolder.txState().memoryTracker();
                SchemaDescriptor schema = index.schema();
                Value[] values = getValueTuple(
                        entity,
                        propertyCursor,
                        propertyKeyId,
                        value,
                        schema.getPropertyIds(),
                        materializedProperties,
                        memoryTracker);
                indexingService.validateBeforeCommit(index, values, entity.reference());
                ValueTuple valueTuple = ValueTuple.of(values);
                memoryTracker.allocateHeap(valueTuple.getShallowSize());
                txStateHolder.txState().indexDoUpdateEntry(schema, entity.reference(), null, valueTuple);
            });
        }
    }

    private void onPropertyRemove(
            EntityCursor entity,
            EntityType entityType,
            PropertyCursor propertyCursor,
            int[] tokens,
            int propertyKeyId,
            int[] existingPropertyKeyIds,
            Value value) {
        assert noSchemaChangedInTx();
        Collection<IndexDescriptor> indexes = storageReader.valueIndexesGetRelated(tokens, propertyKeyId, entityType);
        if (!indexes.isEmpty()) {
            MutableIntObjectMap<Value> materializedProperties = IntObjectMaps.mutable.empty();
            SchemaMatcher.onMatchingSchema(indexes.iterator(), propertyKeyId, existingPropertyKeyIds, index -> {
                MemoryTracker memoryTracker = txStateHolder.txState().memoryTracker();
                SchemaDescriptor schema = index.schema();
                Value[] values = getValueTuple(
                        entity,
                        propertyCursor,
                        propertyKeyId,
                        value,
                        schema.getPropertyIds(),
                        materializedProperties,
                        memoryTracker);
                ValueTuple valueTuple = ValueTuple.of(values);
                memoryTracker.allocateHeap(valueTuple.getShallowSize());
                txStateHolder.txState().indexDoUpdateEntry(schema, entity.reference(), valueTuple, null);
            });
        }
    }

    private void onPropertyChange(
            EntityCursor entity,
            EntityType entityType,
            PropertyCursor propertyCursor,
            int[] tokens,
            int propertyKeyId,
            int[] existingPropertyKeyIds,
            Value beforeValue,
            Value afterValue) {
        assert noSchemaChangedInTx();
        Collection<IndexDescriptor> indexes = storageReader.valueIndexesGetRelated(tokens, propertyKeyId, entityType);
        if (!indexes.isEmpty()) {
            MutableIntObjectMap<Value> materializedProperties = IntObjectMaps.mutable.empty();
            SchemaMatcher.onMatchingSchema(indexes.iterator(), propertyKeyId, existingPropertyKeyIds, index -> {
                MemoryTracker memoryTracker = txStateHolder.txState().memoryTracker();
                SchemaDescriptor schema = index.schema();
                int[] propertyIds = schema.getPropertyIds();
                Value[] valuesAfter = getValueTuple(
                        entity,
                        propertyCursor,
                        propertyKeyId,
                        afterValue,
                        propertyIds,
                        materializedProperties,
                        memoryTracker);

                // The valuesBefore tuple is just like valuesAfter, except is has the afterValue instead of the
                // beforeValue
                Value[] valuesBefore = Arrays.copyOf(valuesAfter, valuesAfter.length);
                int k = ArrayUtils.indexOf(propertyIds, propertyKeyId);
                valuesBefore[k] = beforeValue;

                indexingService.validateBeforeCommit(index, valuesAfter, entity.reference());
                ValueTuple valuesTupleBefore = ValueTuple.of(valuesBefore);
                ValueTuple valuesTupleAfter = ValueTuple.of(valuesAfter);
                memoryTracker.allocateHeap(
                        valuesTupleBefore.getShallowSize() * 2); // They are copies and same shallow size
                txStateHolder
                        .txState()
                        .indexDoUpdateEntry(schema, entity.reference(), valuesTupleBefore, valuesTupleAfter);
            });
        }
    }

    private static Value[] getValueTuple(
            EntityCursor entity,
            PropertyCursor propertyCursor,
            int changedPropertyKeyId,
            Value changedValue,
            int[] indexPropertyIds,
            MutableIntObjectMap<Value> materializedValues,
            MemoryTracker memoryTracker) {
        Value[] values = new Value[indexPropertyIds.length];
        int missing = 0;

        // First get whatever values we already have on the stack, like the value change that provoked this update in
        // the first place
        // and already loaded values that we can get from the map of materialized values.
        for (int k = 0; k < indexPropertyIds.length; k++) {
            values[k] = indexPropertyIds[k] == changedPropertyKeyId
                    ? changedValue
                    : materializedValues.getIfAbsent(indexPropertyIds[k], () -> NO_VALUE);
            if (values[k] == NO_VALUE) {
                missing++;
            }
        }

        // If we couldn't get all values that we wanted we need to load from the entity. While we're loading values
        // we'll place those values in the map so that other index updates from this change can just used them.
        if (missing > 0) {
            entity.properties(propertyCursor, PropertySelection.selection(indexPropertyIds));
            while (missing > 0 && propertyCursor.next()) {
                int k = ArrayUtils.indexOf(indexPropertyIds, propertyCursor.propertyKey());
                assert k >= 0;
                if (values[k] == NO_VALUE) {
                    int propertyKeyId = indexPropertyIds[k];
                    boolean thisIsTheChangedProperty = propertyKeyId == changedPropertyKeyId;
                    values[k] = thisIsTheChangedProperty ? changedValue : propertyCursor.propertyValue();
                    if (!thisIsTheChangedProperty) {
                        materializedValues.put(propertyKeyId, values[k]);
                        memoryTracker.allocateHeap(values[k].estimatedHeapUsage());
                    }
                    missing--;
                }
            }
        }

        return values;
    }
}
