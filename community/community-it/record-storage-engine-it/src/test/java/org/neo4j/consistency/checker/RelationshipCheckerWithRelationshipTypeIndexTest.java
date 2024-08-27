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

import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.RELATIONSHIP_CURSOR;

import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.consistency.checking.ConsistencyFlags;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.TokenIndexEntryUpdate;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class RelationshipCheckerWithRelationshipTypeIndexTest extends CheckerTestBase {
    private int type;
    private int lowerType;
    private int higherType;
    private IndexDescriptor rtiDescriptor;
    private IndexProxy rtiProxy;

    @Inject
    private RandomSupport random;

    @BeforeEach
    void extractRelationshipTypeIndexProxy() {
        IndexingService indexingService = db.getDependencyResolver().resolveDependency(IndexingService.class);
        final IndexDescriptor[] indexDescriptors = schemaStorage.indexGetForSchema(
                () -> SchemaDescriptors.ANY_TOKEN_RELATIONSHIP_SCHEMA_DESCRIPTOR,
                storeCursors,
                EmptyMemoryTracker.INSTANCE);
        // The Relationship Type Index should exist and be unique.
        assertThat(indexDescriptors.length).isEqualTo(1);
        rtiDescriptor = indexDescriptors[0];
        try {
            rtiProxy = indexingService.getIndexProxy(rtiDescriptor);
        } catch (IndexNotFoundKernelException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    void initialData(KernelTransaction tx) throws KernelException {
        lowerType = tx.tokenWrite().relationshipTypeGetOrCreateForName("A");
        type = tx.tokenWrite().relationshipTypeGetOrCreateForName("B");
        higherType = tx.tokenWrite().relationshipTypeGetOrCreateForName("C");
        assertThat(lowerType).isLessThan(type);
        assertThat(type).isLessThan(higherType);
    }

    @ParameterizedTest
    @EnumSource(Density.class)
    void testShouldNotReportAnythingIfDataIsCorrect(Density density) throws Exception {
        doVerifyCorrectReport(
                density,
                writer -> createCompleteEntry(writer, type),
                (Consumer<ConsistencyReport.RelationshipTypeScanConsistencyReport>[]) null);
    }

    @ParameterizedTest
    @EnumSource(Density.class)
    void indexPointToRelationshipNotInUse(Density density) throws Exception {
        doVerifyCorrectReport(
                density,
                writer -> {
                    long relationshipId = createStoreEntry(type);
                    notInUse(relationshipId);
                    createIndexEntry(writer, relationshipId, type);
                },
                reporter -> reporter.relationshipNotInUse(any()));
    }

    @ParameterizedTest
    @EnumSource(Density.class)
    void indexHasHigherTypeThanRelationshipInStore(Density density) throws Exception {
        doVerifyCorrectReport(
                density,
                writer -> {
                    long relationshipId = createStoreEntry(type);
                    createIndexEntry(writer, relationshipId, higherType);
                },
                reporter -> reporter.relationshipDoesNotHaveExpectedRelationshipType(any(), anyLong()),
                reporter -> reporter.relationshipTypeNotInIndex(any(), anyLong()));
    }

    @ParameterizedTest
    @EnumSource(Density.class)
    void indexHasLowerTypeThanRelationshipInStore(Density density) throws Exception {
        doVerifyCorrectReport(
                density,
                writer -> {
                    long relationshipId = createStoreEntry(type);
                    createIndexEntry(writer, relationshipId, lowerType);
                },
                reporter -> reporter.relationshipDoesNotHaveExpectedRelationshipType(any(), anyLong()),
                reporter -> reporter.relationshipTypeNotInIndex(any(), anyLong()));
    }

    @ParameterizedTest
    @EnumSource(Density.class)
    void indexIsMissingEntryForRelationshipInUse(Density density) throws Exception {
        doVerifyCorrectReport(
                density,
                writer -> createStoreEntry(type),
                reporter -> reporter.relationshipTypeNotInIndex(any(), anyLong()));
    }

    @ParameterizedTest
    @EnumSource(Density.class)
    void indexHasMultipleTypesForSameRelationshipOneCorrect(Density density) throws Exception {
        doVerifyCorrectReport(
                density,
                writer -> {
                    long relationshipId = createStoreEntry(type);
                    createIndexEntry(writer, relationshipId, type);
                    createIndexEntry(writer, relationshipId, lowerType);
                    createIndexEntry(writer, relationshipId, higherType);
                },
                reporter -> reporter.relationshipDoesNotHaveExpectedRelationshipType(any(), anyLong()));
    }

    @ParameterizedTest
    @EnumSource(Density.class)
    void indexHasMultipleTypesForSameRelationshipNoneCorrect(Density density) throws Exception {
        doVerifyCorrectReport(
                density,
                writer -> {
                    long relationshipId = createStoreEntry(type);
                    createIndexEntry(writer, relationshipId, lowerType);
                    createIndexEntry(writer, relationshipId, higherType);
                },
                reporter -> reporter.relationshipTypeNotInIndex(any(), anyLong()),
                reporter -> reporter.relationshipDoesNotHaveExpectedRelationshipType(any(), anyLong()));
    }

    @ParameterizedTest
    @EnumSource(Density.class)
    void indexHasMultipleTypesForSameRelationshipNotInUse(Density density) throws Exception {
        doVerifyCorrectReport(
                density,
                writer -> {
                    long relationshipId = createStoreEntry(type);
                    notInUse(relationshipId);
                    createIndexEntry(writer, relationshipId, type);
                    createIndexEntry(writer, relationshipId, lowerType);
                    createIndexEntry(writer, relationshipId, higherType);
                },
                reporter -> reporter.relationshipNotInUse(any()));
    }

    @Test
    void storeHasBigGapButIndexDoesNot() throws Exception {
        // given
        try (Transaction tx = db.beginTx();
                IndexUpdater writer = relationshipTypeIndexWriter()) {
            for (int i = 0; i < 2 * IDS_PER_CHUNK; i++) {
                if (i == 0) {
                    createCompleteEntry(writer, type);
                } else if (i == 10) {
                    long relationshipId = createStoreEntry(type);
                    notInUse(relationshipId);
                    createIndexEntry(writer, relationshipId, type);
                } else if (i == IDS_PER_CHUNK - 1) {
                    createCompleteEntry(writer, type);
                } else {
                    long relationshipId = createStoreEntry(type);
                    notInUse(relationshipId);
                }
            }

            tx.commit();
        }

        // when
        check();

        // then
        expect(
                ConsistencyReport.RelationshipTypeScanConsistencyReport.class,
                reporter -> reporter.relationshipNotInUse(any()));
    }

    @Test
    void checkShouldBeSuccessfulIfNoRelationshipTypeIndexExist() throws Exception {
        // Remove the Relationship Type Index
        try (Transaction tx = db.beginTx()) {
            final Iterable<IndexDefinition> indexes = tx.schema().getIndexes();
            for (IndexDefinition index : indexes) {
                if (index.getIndexType() == IndexType.LOOKUP && index.isRelationshipIndex()) {
                    index.drop();
                }
            }
            tx.commit();
        }

        // Add some data to the relationship store
        try (Transaction tx = db.beginTx()) {
            createStoreEntry(type);
            tx.commit();
        }

        // when
        check();

        // then
        verifyNoInteractions(monitor);
    }

    @SafeVarargs
    private void doVerifyCorrectReport(
            Density density,
            ThrowingConsumer<IndexUpdater, IndexEntryConflictException> targetRelationshipAction,
            Consumer<ConsistencyReport.RelationshipTypeScanConsistencyReport>... expectedCalls)
            throws Exception {
        double recordFrequency = densityAsFrequency(density);
        int nbrOfRelationships = random.nextInt(1, 2 * IDS_PER_CHUNK);
        int targetRelationshipRelationship = random.nextInt(nbrOfRelationships);

        // given
        try (Transaction tx = db.beginTx();
                IndexUpdater writer = relationshipTypeIndexWriter()) {
            for (int i = 0; i < nbrOfRelationships; i++) {
                if (i == targetRelationshipRelationship) {
                    targetRelationshipAction.accept(writer);
                } else {
                    if (random.nextDouble() < recordFrequency) {
                        createCompleteEntry(writer, type);
                    } else {
                        notInUse(createStoreEntry(type));
                    }
                }
            }

            tx.commit();
        }

        // when
        check(context(ConsistencyFlags.ALL));

        // then
        if (expectedCalls != null) {
            for (Consumer<ConsistencyReport.RelationshipTypeScanConsistencyReport> expectedCall : expectedCalls) {
                expect(ConsistencyReport.RelationshipTypeScanConsistencyReport.class, expectedCall);
            }
        } else {
            verifyNoInteractions(monitor);
        }
    }

    private double densityAsFrequency(Density density) {
        return switch (density) {
            case DENSE -> 1;
            case SPARSE -> 0;
            case RANDOM -> random.nextDouble();
        };
    }

    private void createCompleteEntry(IndexUpdater writer, int type) throws IndexEntryConflictException {
        long relationshipId = createStoreEntry(type);
        createIndexEntry(writer, relationshipId, type);
    }

    private void createIndexEntry(IndexUpdater writer, long relationshipId, int type)
            throws IndexEntryConflictException {
        writer.process(TokenIndexEntryUpdate.change(relationshipId, rtiDescriptor, EMPTY_INT_ARRAY, new int[] {type}));
    }

    private long createStoreEntry(int type) {
        var relIdGenerator = relationshipStore.getIdGenerator();
        var nodeIdGenerator = nodeStore.getIdGenerator();
        long relationship = relIdGenerator.nextId(CursorContext.NULL_CONTEXT);
        long node1 = nodePlusCached(nodeIdGenerator.nextId(CursorContext.NULL_CONTEXT), NULL, relationship);
        long node2 = nodePlusCached(nodeIdGenerator.nextId(CursorContext.NULL_CONTEXT), NULL, relationship);
        relationship(relationship, node1, node2, type, NULL, NULL, NULL, NULL, true, true);
        return relationship;
    }

    private void notInUse(long relationshipId) {
        RelationshipRecord relationshipRecord = relationshipStore.newRecord();
        relationshipStore.getRecordByCursor(
                relationshipId,
                relationshipRecord,
                RecordLoad.NORMAL,
                storeCursors.readCursor(RELATIONSHIP_CURSOR),
                EmptyMemoryTracker.INSTANCE);
        relationshipRecord.setInUse(false);
        try (var storeCursor = storeCursors.writeCursor(RELATIONSHIP_CURSOR)) {
            relationshipStore.updateRecord(relationshipRecord, storeCursor, CursorContext.NULL_CONTEXT, storeCursors);
        }
    }

    private void check() throws Exception {
        check(context());
    }

    private void check(CheckerContext context) throws Exception {
        new RelationshipChecker(context, noMandatoryProperties, noAllowedTypes)
                .check(
                        LongRange.range(0, nodeStore.getIdGenerator().getHighId()),
                        true,
                        true,
                        EmptyMemoryTracker.INSTANCE);
    }

    private IndexUpdater relationshipTypeIndexWriter() {
        return rtiProxy.newUpdater(IndexUpdateMode.ONLINE, CursorContext.NULL_CONTEXT, false);
    }

    private enum Density {
        DENSE,
        SPARSE,
        RANDOM
    }
}
