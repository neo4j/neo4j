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

import static java.lang.StrictMath.toIntExact;
import static java.lang.String.format;
import static org.neo4j.internal.recordstorage.InconsistentDataReadException.CYCLE_DETECTION_THRESHOLD;
import static org.neo4j.storageengine.api.LongReference.longReference;
import static org.neo4j.storageengine.api.PropertySelection.ALL_PROPERTIES;

import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.internal.recordstorage.RecordAccess.RecordProxy;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.values.storable.Value;

public class PropertyDeleter {
    private final PropertyTraverser traverser;
    private final NeoStores neoStores;
    private final TokenNameLookup tokenNameLookup;
    private final InternalLogProvider logProvider;
    private final Config config;
    private final CursorContext cursorContext;
    private final StoreCursors storeCursors;

    public PropertyDeleter(
            PropertyTraverser traverser,
            NeoStores neoStores,
            TokenNameLookup tokenNameLookup,
            InternalLogProvider logProvider,
            Config config,
            CursorContext cursorContext,
            StoreCursors storeCursors) {
        this.traverser = traverser;
        this.neoStores = neoStores;
        this.tokenNameLookup = tokenNameLookup;
        this.logProvider = logProvider;
        this.config = config;
        this.cursorContext = cursorContext;
        this.storeCursors = storeCursors;
    }

    public void deletePropertyChain(
            PrimitiveRecord primitive,
            RecordAccess<PropertyRecord, PrimitiveRecord> propertyRecords,
            MemoryTracker memoryTracker) {
        long nextProp = primitive.getNextProp();
        MutableLongSet seenPropertyIds = null;
        int count = 0;
        try {
            while (nextProp != Record.NO_NEXT_PROPERTY.longValue()) {
                RecordProxy<PropertyRecord, PrimitiveRecord> propertyChange =
                        propertyRecords.getOrLoad(nextProp, primitive);
                PropertyRecord propRecord = propertyChange.forChangingData();
                deletePropertyRecordIncludingValueRecords(propRecord);

                // set created flag in before state for the dynamic records that will be removed, so reverse recovery
                // can re-create them
                var before = propertyChange.getBefore();
                markValueRecordsAsCreated(before);

                if (++count >= CYCLE_DETECTION_THRESHOLD) {
                    if (seenPropertyIds == null) {
                        seenPropertyIds = LongSets.mutable.empty();
                    }
                    if (!seenPropertyIds.add(nextProp)) {
                        throw new InconsistentDataReadException("Cycle detected in property chain for %s", primitive);
                    }
                }
                nextProp = propRecord.getNextProp();
                propRecord.setChanged(primitive);
            }
        } catch (InvalidRecordException e) {
            // This property chain, or a dynamic value record chain contains a record which is not in use, so it's
            // somewhat broken.
            // Abort reading the chain, but don't fail the deletion of this property record chain.
            logInconsistentPropertyChain(primitive, memoryTracker, "unused record", e);
        } catch (InconsistentDataReadException e) {
            // This property chain, or a dynamic value record chain contains a cycle.
            // Abort reading the chain, but don't fail the deletion of this property record chain.
            logInconsistentPropertyChain(primitive, memoryTracker, "cycle", e);
        }
        primitive.setNextProp(Record.NO_NEXT_PROPERTY.intValue());
    }

    private void markValueRecordsAsCreated(PropertyRecord beforeRecord) {
        for (var beforeBlock : beforeRecord) {
            markValueRecordsAsCreated(beforeBlock);
        }
    }

    private void markValueRecordsAsCreated(PropertyBlock beforeBlock) {
        assert beforeBlock != null;
        for (DynamicRecord beforeDynamicRecord : beforeBlock.getValueRecords()) {
            assert beforeDynamicRecord.inUse();
            beforeDynamicRecord.setCreated();
        }
    }

    private void logInconsistentPropertyChain(
            PrimitiveRecord primitive, MemoryTracker memoryTracker, String causeMessage, Throwable cause) {
        if (!config.get(GraphDatabaseInternalSettings.log_inconsistent_data_deletion)) {
            return;
        }

        StringBuilder message = new StringBuilder(
                format("Deleted inconsistent property chain with %s for %s", causeMessage, primitive));
        try (RecordPropertyCursor propertyCursor =
                new RecordPropertyCursor(neoStores.getPropertyStore(), cursorContext, storeCursors, memoryTracker)) {
            if (primitive instanceof NodeRecord node) {
                message.append(" with labels: ");
                int[] labelIds = NodeLabelsField.parseLabelsField(node)
                        .get(neoStores.getNodeStore(), storeCursors, memoryTracker);
                message.append(IntStream.of(labelIds)
                        .mapToObj(labelId -> tokenNameLookup.labelGetName(toIntExact(labelId)))
                        .collect(Collectors.toList()));
                propertyCursor.initNodeProperties(longReference(node.getNextProp()), ALL_PROPERTIES, node.getId());
            } else if (primitive instanceof RelationshipRecord relationship) {
                message.append(format(
                        " with relationship type: %s",
                        tokenNameLookup.relationshipTypeGetName(relationship.getType())));
                propertyCursor.initRelationshipProperties(
                        longReference(relationship.getNextProp()), ALL_PROPERTIES, relationship.getId());
            }

            // Use the cursor to read property values, because it's more flexible in reading data
            MutableIntSet seenKeyIds = IntSets.mutable.empty();
            while (propertyCursor.next()) {
                int keyId = propertyCursor.propertyKey();
                if (!seenKeyIds.add(keyId)) {
                    continue;
                }

                String key = tokenNameLookup.propertyKeyGetName(keyId);
                Value value;
                try {
                    value = propertyCursor.propertyValue();
                } catch (Exception e) {
                    value = null;
                }
                String valueToString = value != null ? value.toString() : "<value could not be read>";
                message.append(format("%n  %s = %s", key, valueToString));
            }
        } catch (InconsistentDataReadException e) {
            // Expected to occur on chain cycles, that's what we're here for
        }
        logProvider.getLog(InconsistentDataDeletion.class).error(message.toString(), cause);
    }

    static void deletePropertyRecordIncludingValueRecords(PropertyRecord record) {
        for (PropertyBlock block : record) {
            deleteValueRecords(record, block);
        }
        record.clearPropertyBlocks();
        record.setInUse(false);
    }

    /**
     * Removes property with given {@code propertyKey} from property chain owner by the primitive found in
     * {@code primitiveProxy}.
     *
     * @param primitiveProxy access to the primitive record pointing to the start of the property chain.
     * @param propertyKey the property key token id to look for and remove.
     * @param propertyRecords access to records.
     * @throws IllegalStateException if property key was not found in the property chain.
     */
    public <P extends PrimitiveRecord> void removeProperty(
            RecordProxy<P, Void> primitiveProxy,
            int propertyKey,
            RecordAccess<PropertyRecord, PrimitiveRecord> propertyRecords) {
        PrimitiveRecord primitive = primitiveProxy.forReadingData();
        long propertyId = traverser.findPropertyRecordContaining(primitive, propertyKey, propertyRecords, true);
        removeProperty(primitiveProxy, propertyKey, propertyRecords, primitive, propertyId);
    }

    private <P extends PrimitiveRecord> void removeProperty(
            RecordProxy<P, Void> primitiveProxy,
            int propertyKey,
            RecordAccess<PropertyRecord, PrimitiveRecord> propertyRecords,
            PrimitiveRecord primitive,
            long propertyId) {
        RecordProxy<PropertyRecord, PrimitiveRecord> recordChange = propertyRecords.getOrLoad(propertyId, primitive);
        PropertyRecord propRecord = recordChange.forChangingData();
        if (!propRecord.inUse()) {
            throw new IllegalStateException(
                    "Unable to delete property[" + propertyId + "] since it is already deleted.");
        }

        PropertyBlock block = propRecord.removePropertyBlock(propertyKey);
        if (block == null) {
            throw new IllegalStateException(
                    "Property with index[" + propertyKey + "] is not present in property[" + propertyId + "]");
        }

        deleteValueRecords(propRecord, block);

        // set created flag in before state for the dynamic records that will be removed, so reverse recovery can
        // re-create them
        var before = recordChange.getBefore();
        markValueRecordsAsCreated(before.getPropertyBlock(propertyKey));

        if (propRecord.size() > 0) {
            /*
             * There are remaining blocks in the record. We do not unlink yet.
             */
            propRecord.setChanged(primitive);
            assert traverser.assertPropertyChain(primitive, propertyRecords);
        } else {
            unlinkPropertyRecord(propRecord, propertyRecords, primitiveProxy);
        }
    }

    private static void deleteValueRecords(PropertyRecord propRecord, PropertyBlock block) {
        for (var valueRecord : block.getValueRecords()) {
            assert valueRecord.inUse();
            valueRecord.setInUse(false, block.getType().intValue());
            propRecord.addDeletedRecord(valueRecord);
        }
    }

    private <P extends PrimitiveRecord> void unlinkPropertyRecord(
            PropertyRecord propRecord,
            RecordAccess<PropertyRecord, PrimitiveRecord> propertyRecords,
            RecordProxy<P, Void> primitiveRecordChange) {
        P primitive = primitiveRecordChange.forReadingLinkage();
        assert traverser.assertPropertyChain(primitive, propertyRecords);
        assert propRecord.size() == 0;
        long prevProp = propRecord.getPrevProp();
        long nextProp = propRecord.getNextProp();
        if (primitive.getNextProp() == propRecord.getId()) {
            assert propRecord.getPrevProp() == Record.NO_PREVIOUS_PROPERTY.intValue()
                    : propRecord + " for " + primitive;
            primitiveRecordChange.forChangingLinkage().setNextProp(nextProp);
        }
        if (prevProp != Record.NO_PREVIOUS_PROPERTY.intValue()) {
            PropertyRecord prevPropRecord =
                    propertyRecords.getOrLoad(prevProp, primitive).forChangingLinkage();
            assert prevPropRecord.inUse() : prevPropRecord + "->" + propRecord + " for " + primitive;
            prevPropRecord.setNextProp(nextProp);
            prevPropRecord.setChanged(primitive);
        }
        if (nextProp != Record.NO_NEXT_PROPERTY.intValue()) {
            PropertyRecord nextPropRecord =
                    propertyRecords.getOrLoad(nextProp, primitive).forChangingLinkage();
            assert nextPropRecord.inUse() : propRecord + "->" + nextPropRecord + " for " + primitive;
            nextPropRecord.setPrevProp(prevProp);
            nextPropRecord.setChanged(primitive);
        }
        propRecord.setInUse(false);
        /*
         *  The following two are not needed - the above line does all the work (PropertyStore
         *  does not write out the prev/next for !inUse records). It is nice to set this
         *  however to check for consistency when assertPropertyChain().
         */
        propRecord.setPrevProp(Record.NO_PREVIOUS_PROPERTY.intValue());
        propRecord.setNextProp(Record.NO_NEXT_PROPERTY.intValue());
        propRecord.setChanged(primitive);
        assert traverser.assertPropertyChain(primitive, propertyRecords);
    }
}
