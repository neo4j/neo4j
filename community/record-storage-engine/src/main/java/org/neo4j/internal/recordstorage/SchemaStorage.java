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

import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.SCHEMA_CURSOR;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY_ARRAY;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY_STRING;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.eclipse.collections.api.block.procedure.primitive.IntObjectProcedure;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.DuplicateSchemaRuleException;
import org.neo4j.internal.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.DynamicAllocatorProvider;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.util.IdUpdateListener;
import org.neo4j.token.TokenHolders;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class SchemaStorage implements SchemaRuleAccess {
    private final SchemaStore schemaStore;
    private final TokenHolders tokenHolders;

    public SchemaStorage(SchemaStore schemaStore, TokenHolders tokenHolders) {
        this.schemaStore = schemaStore;
        this.tokenHolders = tokenHolders;
    }

    @Override
    public long newRuleId(CursorContext cursorContext) {
        return schemaStore.getIdGenerator().nextId(cursorContext);
    }

    @Override
    public Iterable<SchemaRule> getAll(StoreCursors storeCursors, MemoryTracker memoryTracker) {
        return streamAllSchemaRules(false, storeCursors, memoryTracker)::iterator;
    }

    @Override
    public Iterable<SchemaRule> getAllIgnoreMalformed(StoreCursors storeCursors, MemoryTracker memoryTracker) {
        return streamAllSchemaRules(true, storeCursors, memoryTracker)::iterator;
    }

    @Override
    public SchemaRule loadSingleSchemaRule(long ruleId, StoreCursors storeCursors, MemoryTracker memoryTracker)
            throws MalformedSchemaRuleException {
        SchemaRecord record = loadSchemaRecord(ruleId, storeCursors, memoryTracker);
        return readSchemaRule(record, storeCursors, memoryTracker);
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll(StoreCursors storeCursors, MemoryTracker memoryTracker) {
        return indexRules(streamAllSchemaRules(false, storeCursors, memoryTracker))
                .iterator();
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAllIgnoreMalformed(
            StoreCursors storeCursors, MemoryTracker memoryTracker) {
        return indexRules(streamAllSchemaRules(true, storeCursors, memoryTracker))
                .iterator();
    }

    @Override
    public IndexDescriptor[] indexGetForSchema(
            SchemaDescriptorSupplier supplier, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        SchemaDescriptor schema = supplier.schema();
        return indexRules(streamAllSchemaRules(false, storeCursors, memoryTracker))
                .filter(rule -> rule.schema().equals(schema))
                .toArray(IndexDescriptor[]::new);
    }

    @Override
    public IndexDescriptor indexGetForName(String indexName, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        return indexRules(streamAllSchemaRules(false, storeCursors, memoryTracker))
                .filter(idx -> idx.getName().equals(indexName))
                .findAny()
                .orElse(null);
    }

    @Override
    public ConstraintDescriptor constraintsGetSingle(
            ConstraintDescriptor descriptor, StoreCursors storeCursors, MemoryTracker memoryTracker)
            throws SchemaRuleNotFoundException, DuplicateSchemaRuleException {
        ConstraintDescriptor[] rules = constraintRules(streamAllSchemaRules(false, storeCursors, memoryTracker))
                .filter(descriptor::equals)
                .toArray(ConstraintDescriptor[]::new);
        if (rules.length == 0) {
            throw new SchemaRuleNotFoundException(descriptor, tokenHolders);
        }
        if (rules.length > 1) {
            throw new DuplicateSchemaRuleException(descriptor, tokenHolders);
        }
        return rules[0];
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetAllIgnoreMalformed(
            StoreCursors storeCursors, MemoryTracker memoryTracker) {
        return constraintRules(streamAllSchemaRules(true, storeCursors, memoryTracker))
                .iterator();
    }

    @Override
    public SchemaRecordChangeTranslator getSchemaRecordChangeTranslator() {
        return new PropertyBasedSchemaRecordChangeTranslator() {
            @Override
            protected IntObjectMap<Value> asMap(SchemaRule rule) throws KernelException {
                return SchemaStore.convertSchemaRuleToMap(rule, tokenHolders);
            }

            @Override
            protected void setConstraintIndexOwnerProperty(long constraintId, IntObjectProcedure<Value> proc)
                    throws KernelException {
                int propertyId = SchemaStore.getOwningConstraintPropertyKeyId(tokenHolders);
                proc.value(propertyId, Values.longValue(constraintId));
            }
        };
    }

    @Override
    public void writeSchemaRule(
            SchemaRule rule,
            IdUpdateListener idUpdateListener,
            DynamicAllocatorProvider allocationProvider,
            CursorContext cursorContext,
            MemoryTracker memoryTracker,
            StoreCursors storeCursors)
            throws KernelException {
        IntObjectMap<Value> protoProperties = SchemaStore.convertSchemaRuleToMap(rule, tokenHolders);
        PropertyStore propertyStore = schemaStore.propertyStore();
        Collection<PropertyBlock> blocks = new ArrayList<>();
        protoProperties.forEachKeyValue((keyId, value) -> {
            PropertyBlock block = new PropertyBlock();
            PropertyStore.encodeValue(
                    block,
                    keyId,
                    value,
                    allocationProvider.allocator(PROPERTY_STRING),
                    allocationProvider.allocator(PROPERTY_ARRAY),
                    cursorContext,
                    memoryTracker);
            blocks.add(block);
        });

        assert !blocks.isEmpty() : "Property blocks should have been produced for schema rule: " + rule;

        long nextPropId = NO_NEXT_PROPERTY.longValue();
        PropertyRecord currRecord = newInitialisedPropertyRecord(propertyStore, rule, cursorContext);

        try (var propertyCursor = storeCursors.writeCursor(PROPERTY_CURSOR);
                var schemaCursor = storeCursors.writeCursor(SCHEMA_CURSOR)) {
            for (PropertyBlock block : blocks) {
                if (!currRecord.hasSpaceFor(block)) {
                    PropertyRecord nextRecord = newInitialisedPropertyRecord(propertyStore, rule, cursorContext);
                    linkAndWritePropertyRecord(
                            propertyStore,
                            currRecord,
                            idUpdateListener,
                            nextRecord.getId(),
                            nextPropId,
                            propertyCursor,
                            cursorContext,
                            storeCursors);
                    nextPropId = currRecord.getId();
                    currRecord = nextRecord;
                }
                currRecord.addPropertyBlock(block);
            }

            linkAndWritePropertyRecord(
                    propertyStore,
                    currRecord,
                    idUpdateListener,
                    Record.NO_PREVIOUS_PROPERTY.longValue(),
                    nextPropId,
                    propertyCursor,
                    cursorContext,
                    storeCursors);
            nextPropId = currRecord.getId();

            SchemaRecord schemaRecord = schemaStore.newRecord();
            schemaRecord.initialize(true, nextPropId);
            schemaRecord.setId(rule.getId());
            schemaRecord.setCreated();
            schemaStore.updateRecord(schemaRecord, idUpdateListener, schemaCursor, cursorContext, storeCursors);
            long highId = rule.getId();
            schemaStore.getIdGenerator().setHighestPossibleIdInUse(highId);
        }
    }

    @Override
    public void deleteSchemaRule(
            long ruleId,
            IdUpdateListener idUpdateListener,
            CursorContext cursorContext,
            MemoryTracker memoryTracker,
            StoreCursors storeCursors)
            throws KernelException {
        var schemaRecord = loadSchemaRecord(ruleId, storeCursors, memoryTracker);
        var propertyStore = schemaStore.propertyStore();

        // Delete property records
        PropertyRecord propRecord = propertyStore.newRecord();
        var nextProp = schemaRecord.getNextProp();
        try (var propertyCursor = storeCursors.writeCursor(PROPERTY_CURSOR)) {
            while (nextProp != NO_NEXT_PROPERTY.longValue()) {
                try {
                    // Ensure all linked dynamic records are also loaded to property record by ensureHeavy
                    propertyStore.getRecordByCursor(
                            nextProp, propRecord, RecordLoad.NORMAL, propertyCursor, memoryTracker);
                    propertyStore.ensureHeavy(propRecord, storeCursors, memoryTracker);
                } catch (InvalidRecordException e) {
                    throw new MalformedSchemaRuleException(
                            "Cannot read schema rule because it is referencing a property record (id " + nextProp
                                    + ") that is invalid: " + propRecord,
                            e);
                }
                nextProp = propRecord.getNextProp();
                deletePropertyRecord(
                        propertyStore, propRecord, idUpdateListener, propertyCursor, cursorContext, storeCursors);
            }
        }

        // Delete schema record
        try (var schemaCursor = storeCursors.writeCursor(SCHEMA_CURSOR)) {
            schemaRecord.setId(ruleId);
            schemaRecord.initialize(false, NO_NEXT_PROPERTY.longValue());
            schemaStore.updateRecord(schemaRecord, idUpdateListener, schemaCursor, cursorContext, storeCursors);
        }
    }

    private static PropertyRecord newInitialisedPropertyRecord(
            PropertyStore propertyStore, SchemaRule rule, CursorContext cursorContext) {
        PropertyRecord record = propertyStore.newRecord();
        record.setId(propertyStore.getIdGenerator().nextId(cursorContext));
        record.setSchemaRuleId(rule.getId());
        record.setCreated();
        return record;
    }

    private static void linkAndWritePropertyRecord(
            PropertyStore propertyStore,
            PropertyRecord record,
            IdUpdateListener idUpdateListener,
            long prevPropId,
            long nextProp,
            PageCursor propertyCursor,
            CursorContext cursorContext,
            StoreCursors storeCursors) {
        record.setInUse(true);
        record.setPrevProp(prevPropId);
        record.setNextProp(nextProp);
        propertyStore.updateRecord(record, idUpdateListener, propertyCursor, cursorContext, storeCursors);
        long highId = record.getId();
        propertyStore.getIdGenerator().setHighestPossibleIdInUse(highId);
    }

    private static void deletePropertyRecord(
            PropertyStore propertyStore,
            PropertyRecord record,
            IdUpdateListener idUpdateListener,
            PageCursor propertyCursor,
            CursorContext cursorContext,
            StoreCursors storeCursors) {
        PropertyDeleter.deletePropertyRecordIncludingValueRecords(record);
        record.setPrevProp(NO_NEXT_PROPERTY.longValue());
        record.setNextProp(NO_NEXT_PROPERTY.longValue());
        propertyStore.updateRecord(record, idUpdateListener, propertyCursor, cursorContext, storeCursors);
    }

    private SchemaRecord loadSchemaRecord(long ruleId, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        return schemaStore.getRecordByCursor(
                ruleId,
                schemaStore.newRecord(),
                RecordLoad.NORMAL,
                storeCursors.readCursor(SCHEMA_CURSOR),
                memoryTracker);
    }

    @VisibleForTesting
    Stream<SchemaRule> streamAllSchemaRules(
            boolean ignoreMalformed, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        long startId = schemaStore.getNumberOfReservedLowIds();
        long endId = schemaStore.getIdGenerator().getHighId();

        return LongStream.range(startId, endId)
                .mapToObj(id -> schemaStore.getRecordByCursor(
                        id,
                        schemaStore.newRecord(),
                        RecordLoad.LENIENT_ALWAYS,
                        storeCursors.readCursor(SCHEMA_CURSOR),
                        memoryTracker))
                .filter(AbstractBaseRecord::inUse)
                .flatMap(record ->
                        readSchemaRuleThrowingRuntimeException(record, ignoreMalformed, storeCursors, memoryTracker));
    }

    private static Stream<IndexDescriptor> indexRules(Stream<SchemaRule> stream) {
        return stream.filter(rule -> rule instanceof IndexDescriptor).map(rule -> (IndexDescriptor) rule);
    }

    private static Stream<ConstraintDescriptor> constraintRules(Stream<SchemaRule> stream) {
        return stream.filter(rule -> rule instanceof ConstraintDescriptor).map(rule -> (ConstraintDescriptor) rule);
    }

    private Stream<SchemaRule> readSchemaRuleThrowingRuntimeException(
            SchemaRecord record, boolean ignoreMalformed, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        try {
            return Stream.of(readSchemaRule(record, storeCursors, memoryTracker));
        } catch (MalformedSchemaRuleException e) {
            // In case we've raced with a record deletion, ignore malformed records that no longer appear to be in use.
            if (!ignoreMalformed && schemaStore.isInUse(record.getId(), storeCursors.readCursor(SCHEMA_CURSOR))) {
                throw new RuntimeException(e);
            }
        }
        return Stream.empty();
    }

    private SchemaRule readSchemaRule(SchemaRecord record, StoreCursors storeCursors, MemoryTracker memoryTracker)
            throws MalformedSchemaRuleException {
        return SchemaStore.readSchemaRule(
                record, schemaStore.propertyStore(), tokenHolders, storeCursors, memoryTracker);
    }
}
