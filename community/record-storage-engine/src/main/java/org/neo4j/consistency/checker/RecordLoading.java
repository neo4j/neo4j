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

import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.GROUP_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.NODE_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.RELATIONSHIP_CURSOR;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;
import static org.neo4j.values.storable.Values.NO_VALUE;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.function.ThrowingIntFunction;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaPatternMatchingType;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.DynamicNodeLabels;
import org.neo4j.kernel.impl.store.DynamicStringStore;
import org.neo4j.kernel.impl.store.InlineNodeLabels;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.token.api.TokenNotFoundException;
import org.neo4j.values.storable.Value;

/**
 * Loads records. This is only meant to be used when actually finding inconsistencies.
 */
class RecordLoading {
    static final BiConsumer<Long, DynamicRecord> NO_DYNAMIC_HANDLER = (id, r) -> {};
    private final NeoStores neoStores;

    RecordLoading(NeoStores neoStores) {
        this.neoStores = neoStores;
    }

    static int[] safeGetNodeLabels(
            CheckerContext context,
            StoreCursors storeCursors,
            long nodeId,
            long labelField,
            RecordReader<DynamicRecord> labelReader) {
        if (!NodeLabelsField.fieldPointsToDynamicRecordOfLabels(labelField)) {
            return InlineNodeLabels.parseInlined(labelField);
        }

        // The idea here is that we don't pass in a lot of cursors and stuff because dynamic labels are so rare?
        List<DynamicRecord> records = new ArrayList<>();
        MutableLongSet seenRecordIds = new LongHashSet();
        ConsistencyReport.Reporter reporter = context.reporter;
        RecordLoading recordLoader = context.recordLoader;
        int nodeLabelBlockSize =
                context.neoStores.getNodeStore().getDynamicLabelStore().getRecordDataSize();
        if (safeLoadDynamicRecordChain(
                record -> records.add(new DynamicRecord(record)),
                labelReader,
                seenRecordIds,
                NodeLabelsField.firstDynamicLabelRecordId(labelField),
                nodeLabelBlockSize,
                (id, labelRecord) -> reporter.forNode(recordLoader.node(nodeId, storeCursors))
                        .dynamicRecordChainCycle(labelRecord),
                (id, labelRecord) -> reporter.forNode(recordLoader.node(nodeId, storeCursors))
                        .dynamicLabelRecordNotInUse(labelRecord),
                (id, labelRecord) -> reporter.forNode(recordLoader.node(nodeId, storeCursors))
                        .dynamicLabelRecordNotInUse(labelRecord),
                (id, labelRecord) -> reporter.forDynamicBlock(RecordType.NODE_DYNAMIC_LABEL, labelRecord)
                        .emptyBlock(),
                labelRecord -> reporter.forDynamicBlock(RecordType.NODE_DYNAMIC_LABEL, labelRecord)
                        .recordNotFullReferencesNext(),
                labelRecord -> reporter.forDynamicBlock(RecordType.NODE_DYNAMIC_LABEL, labelRecord)
                        .invalidLength())) {
            return DynamicNodeLabels.getDynamicLabelsArray(records, labelReader.store(), storeCursors);
        }
        return null;
    }

    static Value[] entityIntersectionWithSchema(int[] entityTokens, IntObjectMap<Value> values, IndexDescriptor index) {
        final var schema = index.schema();
        if (!schema.isAffected(entityTokens)) {
            return null;
        }

        final var capability = index.getCapability();
        final var noValueIsValid = schema.schemaPatternMatchingType() == SchemaPatternMatchingType.PARTIAL_ANY_TOKEN;
        final var propertyKeyIds = schema.getPropertyIds();
        final var matched = new Value[propertyKeyIds.length];
        for (int i = 0; i < propertyKeyIds.length; i++) {
            var value = values.get(propertyKeyIds[i]);
            if (value == null || !capability.areValuesAccepted(value)) {
                if (!noValueIsValid) {
                    // shortcut!! ...also needed as RangeIndexCapability doesn't handle NO_VALUE as an optimisation
                    return null;
                }
                value = NO_VALUE;
            }
            matched[i] = value;
        }

        return capability.areValuesAccepted(matched) ? matched : null;
    }

    <T extends PrimitiveRecord> T entity(T entityCursor, StoreCursors storeCursors) {
        if (entityCursor instanceof NodeRecord) {
            return (T) node(entityCursor.getId(), storeCursors);
        } else if (entityCursor instanceof RelationshipRecord) {
            return (T) relationship(entityCursor.getId(), storeCursors);
        }
        throw new IllegalArgumentException(
                "Was expecting either node cursor or relationship cursor, got " + entityCursor);
    }

    NodeRecord node(long id, StoreCursors storeCursors) {
        return loadRecord(neoStores.getNodeStore(), id, storeCursors.readCursor(NODE_CURSOR));
    }

    PropertyRecord property(long id, StoreCursors storeCursors) {
        return loadRecord(neoStores.getPropertyStore(), id, storeCursors.readCursor(PROPERTY_CURSOR));
    }

    RelationshipRecord relationship(long id, StoreCursors storeCursors) {
        return loadRecord(neoStores.getRelationshipStore(), id, storeCursors.readCursor(RELATIONSHIP_CURSOR));
    }

    RelationshipRecord relationship(RelationshipRecord into, long id, StoreCursors storeCursors) {
        return loadRecord(neoStores.getRelationshipStore(), into, id, storeCursors.readCursor(RELATIONSHIP_CURSOR));
    }

    RelationshipGroupRecord relationshipGroup(long id, StoreCursors storeCursors) {
        return loadRecord(neoStores.getRelationshipGroupStore(), id, storeCursors.readCursor(GROUP_CURSOR));
    }

    static <RECORD extends AbstractBaseRecord> RECORD loadRecord(
            RecordStore<RECORD> store, long id, PageCursor pageCursor) {
        return loadRecord(store, store.newRecord(), id, pageCursor);
    }

    static <RECORD extends AbstractBaseRecord> RECORD loadRecord(
            RecordStore<RECORD> store, RECORD record, long id, PageCursor pageCursor) {
        return store.getRecordByCursor(id, record, RecordLoad.FORCE, pageCursor);
    }

    static <RECORD extends TokenRecord> List<NamedToken> safeLoadTokens(
            TokenStore<RECORD> tokenStore, CursorContext cursorContext) {
        long highId = tokenStore.getIdGenerator().getHighId();
        List<NamedToken> tokens = new ArrayList<>();
        DynamicStringStore nameStore = tokenStore.getNameStore();
        List<DynamicRecord> nameRecords = new ArrayList<>();
        LongHashSet seenRecordIds = new LongHashSet();
        int nameBlockSize = nameStore.getRecordDataSize();
        try (RecordReader<RECORD> tokenReader = new RecordReader<>(tokenStore, true, cursorContext);
                RecordReader<DynamicRecord> nameReader = new RecordReader<>(nameStore, false, cursorContext)) {
            for (long id = 0; id < highId; id++) {
                RECORD record = tokenReader.read(id);
                nameRecords.clear();
                if (record.inUse()) {
                    String name;
                    seenRecordIds = lightReplace(seenRecordIds);
                    if (!NULL_REFERENCE.is(record.getNameId())
                            && safeLoadDynamicRecordChain(
                                    r -> nameRecords.add(new DynamicRecord(r)),
                                    nameReader,
                                    seenRecordIds,
                                    record.getNameId(),
                                    nameBlockSize)) {
                        record.addNameRecords(nameRecords);
                        name = tokenStore.getStringFor(record, StoreCursors.NULL);
                    } else {
                        name = format("<name not loaded due to token(%d) referencing unused name record>", id);
                    }
                    tokens.add(new NamedToken(name, toIntExact(id), record.isInternal()));
                }
            }
        }
        return tokens;
    }

    static boolean safeLoadDynamicRecordChain(
            Consumer<DynamicRecord> target,
            RecordReader<DynamicRecord> reader,
            MutableLongSet seenRecordIds,
            long recordId,
            int blockSize) {
        return safeLoadDynamicRecordChain(
                target,
                reader,
                seenRecordIds,
                recordId,
                blockSize,
                NO_DYNAMIC_HANDLER,
                NO_DYNAMIC_HANDLER,
                NO_DYNAMIC_HANDLER,
                NO_DYNAMIC_HANDLER,
                r -> {},
                r -> {});
    }

    static boolean safeLoadDynamicRecordChain(
            Consumer<DynamicRecord> target,
            RecordReader<DynamicRecord> reader,
            MutableLongSet seenRecordIds,
            long recordId,
            int blockSize,
            BiConsumer<Long, DynamicRecord> circularReferenceReport,
            BiConsumer<Long, DynamicRecord> unusedChainReport,
            BiConsumer<Long, DynamicRecord> brokenChainReport,
            BiConsumer<Long, DynamicRecord> emptyRecordReport,
            Consumer<DynamicRecord> notFullReferencesNextReport,
            Consumer<DynamicRecord> invalidLengthReport) {
        assert seenRecordIds.isEmpty();
        long firstRecordId = recordId;
        long prevRecordId = NULL_REFERENCE.longValue();
        boolean chainIsOk = true;
        while (!NULL_REFERENCE.is(recordId)) {
            if (!seenRecordIds.add(recordId)) {
                // Circular reference
                circularReferenceReport.accept(firstRecordId, reader.record());
                return false;
            }
            DynamicRecord record = reader.read(recordId);
            if (!record.inUse()) {
                // Broken chain somehow
                BiConsumer<Long, DynamicRecord> reporter =
                        recordId == firstRecordId ? unusedChainReport : brokenChainReport;
                reporter.accept(prevRecordId, record);
                return false;
            }
            if (record.getLength() == 0) {
                // Empty record
                emptyRecordReport.accept(firstRecordId, record);
                chainIsOk = false;
            }
            if (record.getLength() < blockSize && !NULL_REFERENCE.is(record.getNextBlock())) {
                notFullReferencesNextReport.accept(record);
                chainIsOk = false;
            }
            if (record.getLength() > blockSize) {
                invalidLengthReport.accept(record);
                chainIsOk = false;
            }
            target.accept(record);
            prevRecordId = recordId;
            recordId = record.getNextBlock();
        }
        return chainIsOk;
    }

    static <RECORD extends AbstractBaseRecord, TOKEN extends TokenRecord> boolean checkValidInternalToken(
            RECORD entity,
            int token,
            TokenHolder tokens,
            TokenStore<TOKEN> tokenStore,
            BiConsumer<RECORD, Integer> illegalTokenReport,
            BiConsumer<RECORD, TOKEN> unusedReporter,
            StoreCursors storeCursors) {
        return checkValidToken(
                entity,
                token,
                tokenStore,
                illegalTokenReport,
                unusedReporter,
                tokens::getInternalTokenById,
                storeCursors);
    }

    static <RECORD extends AbstractBaseRecord, TOKEN extends TokenRecord> boolean checkValidToken(
            RECORD entity,
            int token,
            TokenHolder tokens,
            TokenStore<TOKEN> tokenStore,
            BiConsumer<RECORD, Integer> illegalTokenReport,
            BiConsumer<RECORD, TOKEN> unusedReporter,
            StoreCursors storeCursors) {
        return checkValidToken(
                entity, token, tokenStore, illegalTokenReport, unusedReporter, tokens::getTokenById, storeCursors);
    }

    private static <RECORD extends AbstractBaseRecord, TOKEN extends TokenRecord> boolean checkValidToken(
            RECORD entity,
            int token,
            TokenStore<TOKEN> tokenStore,
            BiConsumer<RECORD, Integer> illegalTokenReport,
            BiConsumer<RECORD, TOKEN> unusedReporter,
            ThrowingIntFunction<NamedToken, TokenNotFoundException> tokenGetter,
            StoreCursors storeCursors) {
        if (token < 0) {
            illegalTokenReport.accept(entity, token);
            return false;
        } else {
            try {
                tokenGetter.apply(token);
                // It's in use, good
            } catch (TokenNotFoundException tnfe) {
                TOKEN tokenRecord = tokenStore.getRecordByCursor(
                        token, tokenStore.newRecord(), RecordLoad.FORCE, tokenStore.getTokenStoreCursor(storeCursors));
                unusedReporter.accept(entity, tokenRecord);
                return false;
            }
            // Regardless of whether or not it's in use apparently we're expected to count it
            return true;
        }
    }

    static ArrayList<DynamicRecord> lightReplace(ArrayList<DynamicRecord> list) {
        return list.isEmpty() ? list : new ArrayList<>();
    }

    static LongHashSet lightReplace(LongHashSet set) {
        return set.isEmpty() ? set : new LongHashSet();
    }

    static IntHashSet lightReplace(IntHashSet set) {
        return set.isEmpty() ? set : new IntHashSet();
    }

    static IntObjectHashMap<Value> lightReplace(IntObjectHashMap<Value> map) {
        return map.isEmpty() ? map : new IntObjectHashMap<>();
    }
}
