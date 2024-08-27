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
package org.neo4j.consistency.checker;

import static org.neo4j.consistency.checker.RecordLoading.NO_DYNAMIC_HANDLER;
import static org.neo4j.consistency.checker.RecordLoading.checkValidInternalToken;
import static org.neo4j.consistency.checker.RecordLoading.checkValidToken;
import static org.neo4j.consistency.checker.RecordLoading.lightReplace;
import static org.neo4j.consistency.checker.RecordLoading.safeLoadDynamicRecordChain;
import static org.neo4j.io.IOUtils.closeAllUnchecked;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;

import java.util.ArrayList;
import java.util.function.Function;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/**
 * A Property chain reader (and optionally checker) which can read, detect and abort broken property chains where a normal PropertyCursor
 * would have thrown exception on inconsistent chain.
 */
class SafePropertyChainReader implements AutoCloseable {
    private final int stringStoreBlockSize;
    private final int arrayStoreBlockSize;
    private final PropertyStore propertyStore;
    private final RecordReader<PropertyRecord> propertyReader;
    private final RecordReader<DynamicRecord> stringReader;
    private final RecordReader<DynamicRecord> arrayReader;
    private final ConsistencyReport.Reporter reporter;
    private final CheckerContext context;
    private final NeoStores neoStores;
    private final boolean internalTokens;
    private final FreeIdCache freeIdCache;
    private LongHashSet seenRecords;
    private LongHashSet seenDynamicRecordIds;
    private ArrayList<DynamicRecord> dynamicRecords;

    SafePropertyChainReader(CheckerContext context, CursorContext cursorContext) {
        this(context, cursorContext, false);
    }

    SafePropertyChainReader(CheckerContext context, CursorContext cursorContext, boolean checkInternalTokens) {
        this.context = context;
        this.neoStores = context.neoStores;
        this.reporter = context.reporter;
        this.stringStoreBlockSize =
                neoStores.getPropertyStore().getStringStore().getRecordDataSize();
        this.arrayStoreBlockSize = neoStores.getPropertyStore().getArrayStore().getRecordDataSize();
        this.propertyStore = neoStores.getPropertyStore();
        this.propertyReader =
                new RecordReader<>(neoStores.getPropertyStore(), false, cursorContext, context.memoryTracker);
        this.stringReader = new RecordReader<>(
                neoStores.getPropertyStore().getStringStore(), false, cursorContext, context.memoryTracker);
        this.arrayReader = new RecordReader<>(
                neoStores.getPropertyStore().getArrayStore(), false, cursorContext, context.memoryTracker);
        this.seenRecords = new LongHashSet();
        this.seenDynamicRecordIds = new LongHashSet();
        this.dynamicRecords = new ArrayList<>();
        this.internalTokens = checkInternalTokens;
        this.freeIdCache = context.propertyFreeIdCache;
    }

    /**
     * Reads all property values from an entity into the given {@code intoValues}. Values are safely read and encountered inconsistencies are reported.
     *
     * @param intoValues map to put read values into.
     * @param entity the entity to read property values from.
     * @param primitiveReporter reporter for encountered inconsistencies.
     * @param storeCursors to get cursors from.
     * @param <PRIMITIVE> entity type.
     * @return {@code true} if there were no inconsistencies encountered, otherwise {@code false}.
     */
    <PRIMITIVE extends PrimitiveRecord> boolean read(
            MutableIntObjectMap<Value> intoValues,
            PRIMITIVE entity,
            Function<PRIMITIVE, ConsistencyReport.PrimitiveConsistencyReport> primitiveReporter,
            StoreCursors storeCursors) {
        seenRecords = lightReplace(seenRecords);
        long propertyRecordId = entity.getNextProp();
        long previousRecordId = NULL_REFERENCE.longValue();
        boolean chainIsOk = true;
        while (!NULL_REFERENCE.is(propertyRecordId) && !context.isCancelled()) {
            if (!seenRecords.add(propertyRecordId)) {
                primitiveReporter.apply(entity).propertyChainContainsCircularReference(propertyReader.record());
                chainIsOk = false;
                break;
            }

            PropertyRecord propertyRecord = propertyReader.read(propertyRecordId);
            if (!propertyRecord.inUse()) {
                primitiveReporter.apply(entity).propertyNotInUse(propertyRecord);
                reporter.forProperty(
                                context.recordLoader.property(previousRecordId, storeCursors, context.memoryTracker))
                        .nextNotInUse(propertyRecord);
                return false;
            } else {
                if (freeIdCache.isIdFree(propertyRecordId)) {
                    reporter.forProperty(propertyRecord).idIsFreed();
                }

                if (propertyRecord.getPrevProp() != previousRecordId) {
                    if (NULL_REFERENCE.is(previousRecordId)) {
                        primitiveReporter.apply(entity).propertyNotFirstInChain(propertyRecord);
                    } else {
                        reporter.forProperty(context.recordLoader.property(
                                        previousRecordId, storeCursors, context.memoryTracker))
                                .nextDoesNotReferenceBack(propertyRecord);
                        // prevDoesNotReferenceBack is not reported, unnecessary double report (same inconsistency from
                        // different directions)
                    }
                    chainIsOk = false;
                }

                for (PropertyBlock block : propertyRecord) {
                    int propertyKeyId = block.getKeyIndexId();
                    if (internalTokens) {
                        if (!checkValidInternalToken(
                                propertyRecord,
                                propertyKeyId,
                                context.tokenHolders.propertyKeyTokens(),
                                neoStores.getPropertyKeyTokenStore(),
                                (property, token) ->
                                        reporter.forProperty(property).invalidPropertyKey(block),
                                // apparently counts for internal tokens are not collected
                                (property, token) -> {},
                                storeCursors,
                                context.memoryTracker)) {
                            chainIsOk = false;
                        }
                    } else {
                        if (!checkValidToken(
                                propertyRecord,
                                propertyKeyId,
                                context.tokenHolders.propertyKeyTokens(),
                                neoStores.getPropertyKeyTokenStore(),
                                (property, token) ->
                                        reporter.forProperty(property).invalidPropertyKey(block),
                                (property, token) ->
                                        reporter.forProperty(property).keyNotInUse(block, token),
                                storeCursors,
                                context.memoryTracker)) {
                            chainIsOk = false;
                        }
                    }
                    PropertyType type = block.forceGetType();
                    Value value = Values.NO_VALUE;
                    if (type == null) {
                        reporter.forProperty(propertyRecord).invalidPropertyType(block);
                    } else {
                        try {
                            switch (type) {
                                case STRING:
                                    dynamicRecords = lightReplace(dynamicRecords);
                                    seenDynamicRecordIds = lightReplace(seenDynamicRecordIds);
                                    if (safeLoadDynamicRecordChain(
                                            record -> dynamicRecords.add(new DynamicRecord(record)),
                                            stringReader,
                                            seenDynamicRecordIds,
                                            block.getSingleValueLong(),
                                            stringStoreBlockSize,
                                            NO_DYNAMIC_HANDLER,
                                            (id, record) -> reporter.forProperty(propertyRecord)
                                                    .stringNotInUse(block, record),
                                            (id, record) -> reporter.forDynamicBlock(
                                                            RecordType.STRING_PROPERTY, stringReader.record())
                                                    .nextNotInUse(record),
                                            (id, record) -> reporter.forProperty(propertyRecord)
                                                    .stringEmpty(block, record),
                                            record -> reporter.forDynamicBlock(RecordType.STRING_PROPERTY, record)
                                                    .recordNotFullReferencesNext(),
                                            record -> reporter.forDynamicBlock(RecordType.STRING_PROPERTY, record)
                                                    .invalidLength())) {
                                        value = propertyStore.getTextValueFor(
                                                dynamicRecords, storeCursors, context.memoryTracker);
                                    }
                                    break;
                                case ARRAY:
                                    dynamicRecords = lightReplace(dynamicRecords);
                                    seenDynamicRecordIds = lightReplace(seenDynamicRecordIds);
                                    if (safeLoadDynamicRecordChain(
                                            record -> dynamicRecords.add(new DynamicRecord(record)),
                                            arrayReader,
                                            seenDynamicRecordIds,
                                            block.getSingleValueLong(),
                                            arrayStoreBlockSize,
                                            NO_DYNAMIC_HANDLER,
                                            (id, record) -> reporter.forProperty(propertyRecord)
                                                    .arrayNotInUse(block, record),
                                            (id, record) -> reporter.forDynamicBlock(
                                                            RecordType.ARRAY_PROPERTY, arrayReader.record())
                                                    .nextNotInUse(record),
                                            (id, record) -> reporter.forProperty(propertyRecord)
                                                    .arrayEmpty(block, record),
                                            record -> reporter.forDynamicBlock(RecordType.ARRAY_PROPERTY, record)
                                                    .recordNotFullReferencesNext(),
                                            record -> reporter.forDynamicBlock(RecordType.ARRAY_PROPERTY, record)
                                                    .invalidLength())) {
                                        value = propertyStore.getArrayFor(
                                                dynamicRecords, storeCursors, context.memoryTracker);
                                    }
                                    break;
                                default:
                                    value = type.value(block, null, storeCursors, context.memoryTracker);
                                    break;
                            }
                        } catch (Exception e) {
                            reporter.forProperty(propertyRecord)
                                    .invalidPropertyValue(propertyRecord.getId(), block.getKeyIndexId());
                        }
                    }
                    if (value == Values.NO_VALUE) {
                        chainIsOk = false;
                    } else if (propertyKeyId >= 0 && intoValues.put(propertyKeyId, value) != null) {
                        primitiveReporter.apply(entity).propertyKeyNotUniqueInChain();
                        chainIsOk = false;
                    }
                }
            }
            previousRecordId = propertyRecordId;
            propertyRecordId = propertyRecord.getNextProp();
        }
        return chainIsOk;
    }

    @Override
    public void close() {
        closeAllUnchecked(propertyReader, stringReader, arrayReader);
    }
}
