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

import static org.mockito.ArgumentMatchers.any;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.RELATIONSHIP_CURSOR;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.consistency.report.ConsistencyReport.RelationshipConsistencyReport;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.memory.EmptyMemoryTracker;

class RelationshipChainCheckerTest extends CheckerTestBase {
    private static final MyRelTypes TYPE = MyRelTypes.TEST;

    private long nodeId1;
    private long nodeId2;
    private long nodeId3;

    @Override
    void initialData(KernelTransaction tx) throws KernelException {
        nodeId1 = tx.dataWrite().nodeCreate();
        nodeId2 = tx.dataWrite().nodeCreate();
        nodeId3 = tx.dataWrite().nodeCreate();
    }

    int numberOfThreads() {
        return NUMBER_OF_THREADS;
    }

    @Test
    void shouldReportSourcePrevDoesNotReferenceBack() throws Exception {
        testRelationshipChainInconsistency(
                (relationship1, relationship2) -> relationship1.setFirstNextRel(NULL),
                report -> report.sourcePrevDoesNotReferenceBack(any()));
    }

    @Test
    void shouldReportSourceNextDoesNotReferenceBack() throws Exception {
        testRelationshipChainInconsistency(
                (relationship1, relationship2) -> relationship2.setFirstPrevRel(NULL),
                report -> report.sourceNextDoesNotReferenceBack(any()));
    }

    @Test
    void shouldReportTargetPrevDoesNotReferenceBack() throws Exception {
        testRelationshipChainInconsistency(
                (relationship1, relationship2) -> relationship1.setSecondNextRel(NULL),
                report -> report.targetPrevDoesNotReferenceBack(any()));
    }

    @Test
    void shouldReportTargetNextDoesNotReferenceBack() throws Exception {
        testRelationshipChainInconsistency(
                (relationship1, relationship2) -> relationship2.setSecondPrevRel(NULL),
                report -> report.targetNextDoesNotReferenceBack(any()));
    }

    @Test
    void shouldReportSourceNextPrevDoesNotReferenceBack() throws Exception {
        testRelationshipChainInconsistency(
                (relationship1, relationship2) -> relationship1.setFirstNode(nodeId3), report -> {
                    report.sourceNextDoesNotReferenceBack(any());
                    report.sourcePrevDoesNotReferenceBack(any());
                });
    }

    @Test
    void shouldReportReferencesOtherNodesForward() throws Exception {
        shouldReportReferencesOtherNodes(
                true,
                relationship -> {
                    MutableLongSet set = LongSets.mutable.of(nodeId1, nodeId2, nodeId3);
                    set.remove(relationship.getFirstNode());
                    set.remove(relationship.getSecondNode());
                    relationship.setFirstNode(set.longIterator().next());
                },
                report -> {
                    report.sourceNextDoesNotReferenceBack(any());
                    report.targetPrevReferencesOtherNodes(any());
                });
    }

    @Test
    void shouldReportReferencesOtherNodesBackward() throws Exception {
        shouldReportReferencesOtherNodes(
                false,
                relationship -> {
                    MutableLongSet set = LongSets.mutable.of(nodeId1, nodeId2, nodeId3);
                    set.remove(relationship.getFirstNode());
                    set.remove(relationship.getSecondNode());
                    relationship.setSecondNode(set.longIterator().next());
                },
                report -> {
                    report.targetNextDoesNotReferenceBack(any());
                    report.sourcePrevReferencesOtherNodes(any());
                });
    }

    @Test
    void shouldReportNotUsedFirstRelationshipReferencedInChainForSingleRelationshipChain() throws Exception {
        testRelationshipChainInconsistency((relationship1, relationship2) -> relationship1.setInUse(false), report -> {
            report.sourcePrevDoesNotReferenceBack(any());
            report.targetPrevDoesNotReferenceBack(any());
        });
    }

    @Test
    void shouldReportNotUsedSecondRelationshipReferencedInChainForSingleRelationshipChain() throws Exception {
        testRelationshipChainInconsistency((relationship1, relationship2) -> relationship2.setInUse(false), report -> {
            report.sourceNextDoesNotReferenceBack(any());
            report.targetNextDoesNotReferenceBack(any());
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldReportNotUsedSecondRelationshipReferencedInChain(boolean forward) throws Exception {
        shouldReportReferencesOtherNodes(
                forward,
                relationship -> relationship.setInUse(false),
                report -> report.notUsedRelationshipReferencedInChain(any()));
    }

    private void shouldReportReferencesOtherNodes(
            boolean forward,
            Consumer<RelationshipRecord> vandal,
            Consumer<RelationshipConsistencyReport> expectedReport)
            throws Exception {
        long[] relationshipIds = new long[20];
        try (Transaction tx = db.beginTx()) {
            Node[] nodes = new Node[] {tx.getNodeById(nodeId1), tx.getNodeById(nodeId2), tx.getNodeById(nodeId3)};
            for (int i = 0; i < relationshipIds.length; i++) {
                Node node1 = nodes[i % nodes.length];
                Node node2 = nodes[(i + 1) % nodes.length];
                Node startNode = forward ? node1 : node2;
                Node endNode = forward ? node2 : node1;
                Relationship relationship = endNode.createRelationshipTo(startNode, TYPE);
                relationshipIds[i] = relationship.getId();
            }
            tx.commit();
        }

        RelationshipStore relationshipStore =
                context(numberOfThreads()).neoStores.getRelationshipStore();
        RelationshipRecord arbitraryRelationship = relationshipStore.newRecord();
        try (var cursor = relationshipStore.openPageCursorForReading(0, CursorContext.NULL_CONTEXT)) {
            relationshipStore.getRecordByCursor(
                    relationshipIds[relationshipIds.length / 2],
                    arbitraryRelationship,
                    NORMAL,
                    cursor,
                    EmptyMemoryTracker.INSTANCE);
        }
        vandal.accept(arbitraryRelationship);
        try (var storeCursor = storeCursors.writeCursor(RELATIONSHIP_CURSOR)) {
            relationshipStore.updateRecord(
                    arbitraryRelationship, storeCursor, CursorContext.NULL_CONTEXT, storeCursors);
        }

        // when
        check();

        // then
        expect(RelationshipConsistencyReport.class, expectedReport);
    }

    void testRelationshipChainInconsistency(
            BiConsumer<RelationshipRecord, RelationshipRecord> vandal, Consumer<RelationshipConsistencyReport> report)
            throws Exception {
        // given
        long firstRelationshipId;
        long secondRelationshipId;
        try (Transaction tx = db.beginTx()) {
            // Create the basic (and consistent) relationship structure by normal transaction processing because
            // getting all the pointers right is daunting and fragile
            Node node1 = tx.getNodeById(nodeId1);
            Node node2 = tx.getNodeById(nodeId2);
            Relationship relationship1 = node1.createRelationshipTo(node2, MyRelTypes.TEST);
            Relationship relationship2 = node1.createRelationshipTo(node2, MyRelTypes.TEST);

            // Sort of reversed here because last created is first in chain
            firstRelationshipId = relationship2.getId();
            secondRelationshipId = relationship1.getId();

            tx.commit();
        }

        try (var tx = tx()) {
            RelationshipRecord first = relationshipStore.newRecord();
            RelationshipRecord second = relationshipStore.newRecord();
            try (var cursor = relationshipStore.openPageCursorForReading(0, CursorContext.NULL_CONTEXT)) {
                relationshipStore.getRecordByCursor(
                        firstRelationshipId, first, NORMAL, cursor, EmptyMemoryTracker.INSTANCE);
                relationshipStore.getRecordByCursor(
                        secondRelationshipId, second, NORMAL, cursor, EmptyMemoryTracker.INSTANCE);
            }
            vandal.accept(first, second);
            try (var storeCursor = storeCursors.writeCursor(RELATIONSHIP_CURSOR)) {
                relationshipStore.updateRecord(first, storeCursor, CursorContext.NULL_CONTEXT, storeCursors);
                relationshipStore.updateRecord(second, storeCursor, CursorContext.NULL_CONTEXT, storeCursors);
            }
        }

        // when
        check();

        // then
        expect(RelationshipConsistencyReport.class, report);
    }

    private void check() throws Exception {
        new RelationshipChainChecker(context(numberOfThreads()))
                .check(
                        LongRange.range(0, nodeStore.getIdGenerator().getHighId()),
                        true,
                        true,
                        EmptyMemoryTracker.INSTANCE);
    }
}
