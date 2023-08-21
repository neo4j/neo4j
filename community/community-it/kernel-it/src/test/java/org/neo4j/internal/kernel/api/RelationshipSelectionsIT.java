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
package org.neo4j.internal.kernel.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.internal.helpers.collection.Iterators.count;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.allCursor;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.allIterator;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.incomingCursor;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.incomingIterator;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.outgoingCursor;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.outgoingIterator;

import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.DirectedTypes;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

@DbmsExtension
public class RelationshipSelectionsIT {
    private static final RelationshipType relationshipType = withName("relType");

    // Outgoing types
    private static final RelationshipType O1 = withName("O1");
    private static final RelationshipType O2 = withName("O2");

    // Incoming types
    private static final RelationshipType I1 = withName("I1");
    private static final RelationshipType I2 = withName("I2");

    // Loop types
    private static final RelationshipType L1 = withName("L1");
    private static final RelationshipType L2 = withName("L2");

    @Inject
    private GraphDatabaseAPI database;

    @Test
    void tracePageCacheAccessOnOutgoingCursor() {
        long nodeId = getSparseNodeId();
        try (var transaction = database.beginTx()) {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType(relationshipType.name());
            var cursorContext = kernelTransaction.cursorContext();
            try (var nodeCursor = cursors.allocateNodeCursor(cursorContext)) {
                setNodeCursor(nodeId, kernelTransaction, nodeCursor);
                assertCursorHits(cursorContext, 1);

                try (var cursor = outgoingCursor(cursors, nodeCursor, new int[] {typeId}, cursorContext)) {
                    consumeCursor(cursor);
                }

                assertCursorHits(cursorContext, 2);
            }
        }
    }

    @Test
    void tracePageCacheAccessOnIncomingCursor() {
        long nodeId = getSparseNodeId();
        try (var transaction = database.beginTx()) {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType(relationshipType.name());
            var cursorContext = kernelTransaction.cursorContext();
            try (var nodeCursor = cursors.allocateNodeCursor(cursorContext)) {
                setNodeCursor(nodeId, kernelTransaction, nodeCursor);
                assertCursorHits(cursorContext, 1);

                try (var cursor = incomingCursor(cursors, nodeCursor, new int[] {typeId}, cursorContext)) {
                    consumeCursor(cursor);
                }

                assertCursorHits(cursorContext, 2);
            }
        }
    }

    @Test
    void tracePageCacheAccessOnAllCursor() {
        var nodeId = getSparseNodeId();

        try (var transaction = database.beginTx()) {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType(relationshipType.name());
            var cursorContext = kernelTransaction.cursorContext();
            try (var nodeCursor = cursors.allocateNodeCursor(cursorContext)) {
                setNodeCursor(nodeId, kernelTransaction, nodeCursor);
                assertCursorHits(cursorContext, 1);

                try (var cursor = allCursor(cursors, nodeCursor, new int[] {typeId}, cursorContext)) {
                    consumeCursor(cursor);
                }

                assertCursorHits(cursorContext, 2);
            }
        }
    }

    @Test
    void tracePageCacheAccessOnOutgoingIterator() {
        var nodeId = getSparseNodeId();

        try (var transaction = database.beginTx()) {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType(relationshipType.name());
            var cursorContext = kernelTransaction.cursorContext();
            try (var nodeCursor = cursors.allocateNodeCursor(cursorContext)) {
                setNodeCursor(nodeId, kernelTransaction, nodeCursor);
                assertCursorHits(cursorContext, 1);

                try (var iterator = outgoingIterator(
                        cursors,
                        nodeCursor,
                        new int[] {typeId},
                        RelationshipDataAccessor::relationshipReference,
                        cursorContext)) {
                    assertEquals(2, count(iterator));
                }

                assertCursorHits(cursorContext, 2);
            }
        }
    }

    @Test
    void tracePageCacheAccessOnIncomingIterator() {
        var nodeId = getSparseNodeId();

        try (var transaction = database.beginTx()) {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType(relationshipType.name());
            var cursorContext = kernelTransaction.cursorContext();
            try (var nodeCursor = cursors.allocateNodeCursor(cursorContext)) {
                setNodeCursor(nodeId, kernelTransaction, nodeCursor);
                assertCursorHits(cursorContext, 1);

                try (var iterator = incomingIterator(
                        cursors,
                        nodeCursor,
                        new int[] {typeId},
                        RelationshipDataAccessor::relationshipReference,
                        cursorContext)) {
                    assertEquals(2, count(iterator));
                }

                assertCursorHits(cursorContext, 2);
            }
        }
    }

    @Test
    void tracePageCacheAccessOnAllIterator() {
        var nodeId = getSparseNodeId();

        try (var transaction = database.beginTx()) {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType(relationshipType.name());
            var cursorContext = kernelTransaction.cursorContext();
            try (var nodeCursor = cursors.allocateNodeCursor(cursorContext)) {
                setNodeCursor(nodeId, kernelTransaction, nodeCursor);
                assertCursorHits(cursorContext, 1);

                try (var iterator = allIterator(
                        cursors,
                        nodeCursor,
                        new int[] {typeId},
                        RelationshipDataAccessor::relationshipReference,
                        cursorContext)) {
                    assertEquals(4, count(iterator));
                }

                assertCursorHits(cursorContext, 2);
            }
        }
    }

    @Test
    void tracePageCacheAccessOnOutgoingDenseCursor() {
        long nodeId = getDenseNodeId();
        try (var transaction = database.beginTx()) {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType(relationshipType.name());
            var cursorContext = kernelTransaction.cursorContext();
            try (var nodeCursor = cursors.allocateNodeCursor(cursorContext)) {
                setNodeCursor(nodeId, kernelTransaction, nodeCursor);
                assertCursorHits(cursorContext, 1);

                try (var cursor = outgoingCursor(cursors, nodeCursor, new int[] {typeId}, cursorContext)) {
                    consumeCursor(cursor);
                }

                assertCursorHits(cursorContext, 3);
            }
        }
    }

    @Test
    void tracePageCacheAccessOnIncomingDenseCursor() {
        long nodeId = getDenseNodeId();
        try (var transaction = database.beginTx()) {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType(relationshipType.name());
            var cursorContext = kernelTransaction.cursorContext();
            try (var nodeCursor = cursors.allocateNodeCursor(cursorContext)) {
                setNodeCursor(nodeId, kernelTransaction, nodeCursor);
                assertCursorHits(cursorContext, 1);

                try (var cursor = incomingCursor(cursors, nodeCursor, new int[] {typeId}, cursorContext)) {
                    consumeCursor(cursor);
                }

                assertCursorHits(cursorContext, 3);
            }
        }
    }

    @Test
    void tracePageCacheAccessOnAllDenseCursor() {
        var nodeId = getDenseNodeId();

        try (var transaction = database.beginTx()) {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType(relationshipType.name());
            var cursorContext = kernelTransaction.cursorContext();
            try (var nodeCursor = cursors.allocateNodeCursor(cursorContext)) {
                setNodeCursor(nodeId, kernelTransaction, nodeCursor);
                assertCursorHits(cursorContext, 1);

                try (var cursor = allCursor(cursors, nodeCursor, new int[] {typeId}, cursorContext)) {
                    consumeCursor(cursor);
                }

                assertCursorHits(cursorContext, 3);
            }
        }
    }

    @Test
    void tracePageCacheAccessOnOutgoingDenseIterator() {
        var nodeId = getDenseNodeId();

        try (var transaction = database.beginTx()) {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType(relationshipType.name());
            var cursorContext = kernelTransaction.cursorContext();
            try (var nodeCursor = cursors.allocateNodeCursor(cursorContext)) {
                setNodeCursor(nodeId, kernelTransaction, nodeCursor);
                assertCursorHits(cursorContext, 1);

                try (var iterator = outgoingIterator(
                        cursors,
                        nodeCursor,
                        new int[] {typeId},
                        RelationshipDataAccessor::relationshipReference,
                        cursorContext)) {
                    assertEquals(2, count(iterator));
                }

                assertCursorHits(cursorContext, 3);
            }
        }
    }

    @Test
    void tracePageCacheAccessOnIncomingDenseIterator() {
        var nodeId = getDenseNodeId();

        try (var transaction = database.beginTx()) {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType(relationshipType.name());
            var cursorContext = kernelTransaction.cursorContext();
            try (var nodeCursor = cursors.allocateNodeCursor(cursorContext)) {
                setNodeCursor(nodeId, kernelTransaction, nodeCursor);
                assertCursorHits(cursorContext, 1);

                try (var iterator = incomingIterator(
                        cursors,
                        nodeCursor,
                        new int[] {typeId},
                        RelationshipDataAccessor::relationshipReference,
                        cursorContext)) {
                    assertEquals(2, count(iterator));
                }

                assertCursorHits(cursorContext, 3);
            }
        }
    }

    @Test
    void tracePageCacheAccessOnAllDenseIterator() {
        var nodeId = getDenseNodeId();

        try (var transaction = database.beginTx()) {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType(relationshipType.name());
            var cursorContext = kernelTransaction.cursorContext();
            try (var nodeCursor = cursors.allocateNodeCursor(cursorContext)) {
                setNodeCursor(nodeId, kernelTransaction, nodeCursor);
                assertCursorHits(cursorContext, 1);

                try (var iterator = allIterator(
                        cursors,
                        nodeCursor,
                        new int[] {typeId},
                        RelationshipDataAccessor::relationshipReference,
                        cursorContext)) {
                    assertEquals(4, count(iterator));
                }

                assertCursorHits(cursorContext, 3);
            }
        }
    }

    @Test
    void multiDirectionalMultiType() {
        long nodeId = getNodeWithManyDifferentIncidentTypes();

        try (var transaction = database.beginTx()) {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var cursorContext = kernelTransaction.cursorContext();
            try (var nodeCursor = cursors.allocateNodeCursor(cursorContext);
                    var relCursor = cursors.allocateRelationshipTraversalCursor(cursorContext)) {

                setNodeCursor(nodeId, kernelTransaction, nodeCursor);
                assertCursorHits(cursorContext, 1);

                int[] everyType = new int[] {
                    // Outgoing
                    kernelTransaction.tokenRead().relationshipType(O1.name()),
                    kernelTransaction.tokenRead().relationshipType(O2.name()),

                    // Incoming
                    kernelTransaction.tokenRead().relationshipType(I1.name()),
                    kernelTransaction.tokenRead().relationshipType(I2.name()),

                    // Loop
                    kernelTransaction.tokenRead().relationshipType(L1.name()),
                    kernelTransaction.tokenRead().relationshipType(L2.name()),
                };

                DirectedTypes directedTypes = new DirectedTypes(kernelTransaction.memoryTracker());

                // We are going to test every possible way of assigning types from `everyType` to
                // the one of the three different type arrays or no array.

                // We decide between 4 outcomes (one of the three arrays or no array) 6 times (one time for every type),
                // resulting in 4^6 = 4096 combinations.
                final int N_COMBINATIONS = 4096;

                IntArrayList expectedTraversedTypes = new IntArrayList();
                IntArrayList traversedTypes = new IntArrayList();
                for (int iteration = 0; iteration < N_COMBINATIONS; iteration++) {

                    directedTypes.clear();

                    expectedTraversedTypes.clear();

                    for (int typeIndex = 0; typeIndex < everyType.length; typeIndex++) {

                        // Every type is assigned 1 of 4 outcomes. Either it's placed in one of the 3
                        // type arrays, or it's placed nowhere. The `typeIndex`-th digit in the base 4
                        // representation of `iteration` determines the outcome of the corresponding type.
                        switch ((iteration >> (typeIndex * 2)) % 4) {
                            case 0:
                                if (typeIndex < 2 || typeIndex >= 4) {
                                    // If the type corresponds to a relationship that's outgoing w.r.t the source,
                                    // we expect to traverse it.
                                    expectedTraversedTypes.add(everyType[typeIndex]);
                                }
                                directedTypes.addTypes(new int[] {everyType[typeIndex]}, Direction.OUTGOING);
                                break;
                            case 1:
                                if (typeIndex >= 2) {
                                    // If the type corresponds to a relationship that's incoming w.r.t the source,
                                    // we expect to traverse it.
                                    expectedTraversedTypes.add(everyType[typeIndex]);
                                }
                                directedTypes.addTypes(new int[] {everyType[typeIndex]}, Direction.INCOMING);
                                break;
                            case 2:
                                expectedTraversedTypes.add(everyType[typeIndex]);
                                directedTypes.addTypes(new int[] {everyType[typeIndex]}, Direction.BOTH);
                                break;
                            case 3:
                                // Don't add to any of the three arrays
                                break;
                        }
                    }
                    directedTypes.compact();

                    RelationshipTraversalCursor traversalCursor =
                            RelationshipSelections.multiTypeMultiDirectionCursor(relCursor, nodeCursor, directedTypes);

                    traversedTypes.clear();
                    while (traversalCursor.next()) {
                        traversedTypes.add(traversalCursor.type());
                    }
                    assertEquals(expectedTraversedTypes.sortThis(), traversedTypes.sortThis());
                }
            }
        }
    }

    @Test
    void multiDirectionalMultiTypeWriteInSameTx() {

        try (var transaction = database.beginTx()) {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var cursorContext = kernelTransaction.cursorContext();
            long nodeId = getNodeWithManyDifferentIncidentTypes(transaction);

            try (var nodeCursor = cursors.allocateNodeCursor(cursorContext);
                    var relCursor = cursors.allocateRelationshipTraversalCursor(cursorContext)) {

                setNodeCursor(nodeId, kernelTransaction, nodeCursor);
                assertCursorHits(cursorContext, 1);

                int[] everyType = new int[] {
                    // Outgoing
                    kernelTransaction.tokenRead().relationshipType(O1.name()),
                    kernelTransaction.tokenRead().relationshipType(O2.name()),

                    // Incoming
                    kernelTransaction.tokenRead().relationshipType(I1.name()),
                    kernelTransaction.tokenRead().relationshipType(I2.name()),

                    // Loop
                    kernelTransaction.tokenRead().relationshipType(L1.name()),
                    kernelTransaction.tokenRead().relationshipType(L2.name()),
                };

                DirectedTypes directedTypes = new DirectedTypes(kernelTransaction.memoryTracker());

                // We are going to test every possible way of assigning types from `everyType` to
                // the one of the three different type arrays or no array.

                // We decide between 4 outcomes (one of the three arrays or no array) 6 times (one time for every type),
                // resulting in 4^6 = 4096 combinations.
                final int N_COMBINATIONS = 4096;

                IntArrayList expectedTraversedTypes = new IntArrayList();
                IntArrayList traversedTypes = new IntArrayList();
                for (int iteration = 0; iteration < N_COMBINATIONS; iteration++) {

                    directedTypes.clear();

                    expectedTraversedTypes.clear();

                    for (int typeIndex = 0; typeIndex < everyType.length; typeIndex++) {

                        // Every type is assigned 1 of 4 outcomes. Either it's placed in one of the 3
                        // type arrays, or it's placed nowhere. The `typeIndex`-th digit in the base 4
                        // representation of `iteration` determines the outcome of the corresponding type.
                        switch ((iteration >> (typeIndex * 2)) % 4) {
                            case 0:
                                if (typeIndex < 2 || typeIndex >= 4) {
                                    // If the type corresponds to a relationship that's outgoing w.r.t the source,
                                    // we expect to traverse it.
                                    expectedTraversedTypes.add(everyType[typeIndex]);
                                }
                                directedTypes.addTypes(new int[] {everyType[typeIndex]}, Direction.OUTGOING);
                                break;
                            case 1:
                                if (typeIndex >= 2) {
                                    // If the type corresponds to a relationship that's incoming w.r.t the source,
                                    // we expect to traverse it.
                                    expectedTraversedTypes.add(everyType[typeIndex]);
                                }
                                directedTypes.addTypes(new int[] {everyType[typeIndex]}, Direction.INCOMING);
                                break;
                            case 2:
                                expectedTraversedTypes.add(everyType[typeIndex]);
                                directedTypes.addTypes(new int[] {everyType[typeIndex]}, Direction.BOTH);
                                break;
                            case 3:
                                // Don't add to any of the three arrays
                                break;
                        }
                    }

                    directedTypes.compact();
                    RelationshipTraversalCursor traversalCursor =
                            RelationshipSelections.multiTypeMultiDirectionCursor(relCursor, nodeCursor, directedTypes);

                    traversedTypes.clear();
                    while (traversalCursor.next()) {
                        traversedTypes.add(traversalCursor.type());
                    }
                    assertEquals(expectedTraversedTypes.sortThis(), traversedTypes.sortThis());
                }
            }
        }
    }

    @Test
    void multiDirectionalMultiTypeWriteBothInAndOutOfTx() {

        Transaction tx = database.beginTx();

        var source = tx.createNode();
        var target = tx.createNode();
        var sourceId = source.getElementId();
        var targetId = target.getElementId();

        source.createRelationshipTo(target, O1);
        source.createRelationshipTo(target, O2);

        target.createRelationshipTo(source, I1);
        target.createRelationshipTo(source, I2);

        source.createRelationshipTo(source, L1);
        source.createRelationshipTo(source, L2);

        tx.commit();

        try (var transaction = database.beginTx()) {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var cursorContext = kernelTransaction.cursorContext();

            var sourceInTx = transaction.getNodeByElementId(sourceId);
            var targetInTx = transaction.getNodeByElementId(targetId);

            sourceInTx.createRelationshipTo(targetInTx, O1);
            sourceInTx.createRelationshipTo(targetInTx, O2);

            targetInTx.createRelationshipTo(sourceInTx, I1);
            targetInTx.createRelationshipTo(sourceInTx, I2);

            sourceInTx.createRelationshipTo(sourceInTx, L1);
            sourceInTx.createRelationshipTo(sourceInTx, L2);

            try (var nodeCursor = cursors.allocateNodeCursor(cursorContext);
                    var relCursor = cursors.allocateRelationshipTraversalCursor(cursorContext)) {

                setNodeCursor(sourceInTx.getId(), kernelTransaction, nodeCursor);
                assertCursorHits(cursorContext, 1);

                int[] typeOfEveryRelationship = new int[] {
                    // Outgoing
                    kernelTransaction.tokenRead().relationshipType(O1.name()),
                    kernelTransaction.tokenRead().relationshipType(O2.name()),

                    // Incoming
                    kernelTransaction.tokenRead().relationshipType(I1.name()),
                    kernelTransaction.tokenRead().relationshipType(I2.name()),

                    // Loop
                    kernelTransaction.tokenRead().relationshipType(L1.name()),
                    kernelTransaction.tokenRead().relationshipType(L2.name()),
                };

                DirectedTypes directedTypes = new DirectedTypes(kernelTransaction.memoryTracker());

                // We are going to test every possible way of assigning types from `everyType` to
                // the one of the three different type arrays or no array.

                // We decide between 4 outcomes (one of the three arrays or no array) 12 times (one time for every
                // relationship),
                // resulting in 4^12 = 4096 combinations.
                final int N_COMBINATIONS = 4096;

                IntArrayList expectedTraversedTypes = new IntArrayList();
                IntArrayList traversedTypes = new IntArrayList();
                for (int iteration = 0; iteration < N_COMBINATIONS; iteration++) {

                    directedTypes.clear();

                    expectedTraversedTypes.clear();

                    for (int typeIndex = 0; typeIndex < typeOfEveryRelationship.length; typeIndex++) {

                        // Every type is assigned 1 of 4 outcomes. Either it's placed in one of the 3
                        // type arrays, or it's placed nowhere. The `typeIndex`-th digit in the base 4
                        // representation of `iteration` determines the outcome of the corresponding type.
                        switch ((iteration >> (typeIndex * 2)) % 4) {
                            case 0:
                                if (typeIndex < 2 || typeIndex >= 4) {
                                    // If the type corresponds to a relationship that's outgoing w.r.t the source,
                                    // we expect to traverse it.
                                    expectedTraversedTypes.add(typeOfEveryRelationship[typeIndex]);
                                    // Add once for in tx rel, and once for out of tx rel
                                    expectedTraversedTypes.add(typeOfEveryRelationship[typeIndex]);
                                }
                                directedTypes.addTypes(
                                        new int[] {typeOfEveryRelationship[typeIndex]}, Direction.OUTGOING);
                                break;
                            case 1:
                                if (typeIndex >= 2) {
                                    // If the type corresponds to a relationship that's incoming w.r.t the source,
                                    // we expect to traverse it.
                                    expectedTraversedTypes.add(typeOfEveryRelationship[typeIndex]);
                                    // Add once for in tx rel, and once for out of tx rel
                                    expectedTraversedTypes.add(typeOfEveryRelationship[typeIndex]);
                                }
                                directedTypes.addTypes(
                                        new int[] {typeOfEveryRelationship[typeIndex]}, Direction.INCOMING);
                                break;
                            case 2:
                                expectedTraversedTypes.add(typeOfEveryRelationship[typeIndex]);
                                // Add once for in tx rel, and once for out of tx rel
                                expectedTraversedTypes.add(typeOfEveryRelationship[typeIndex]);
                                directedTypes.addTypes(new int[] {typeOfEveryRelationship[typeIndex]}, Direction.BOTH);
                                break;
                            case 3:
                                // Don't add to any of the three arrays
                                break;
                        }
                    }

                    directedTypes.compact();
                    RelationshipTraversalCursor traversalCursor =
                            RelationshipSelections.multiTypeMultiDirectionCursor(relCursor, nodeCursor, directedTypes);

                    traversedTypes.clear();
                    while (traversalCursor.next()) {
                        traversedTypes.add(traversalCursor.type());
                    }
                    assertEquals(expectedTraversedTypes.sortThis(), traversedTypes.sortThis());
                }
            }
        }
    }

    @Test
    void multiDirectionalMultiTypeAllOutgoing() {
        long nodeId = getNodeWithManyDifferentIncidentTypes();

        try (var transaction = database.beginTx()) {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var cursorContext = kernelTransaction.cursorContext();
            try (var nodeCursor = cursors.allocateNodeCursor(cursorContext);
                    var relCursor = cursors.allocateRelationshipTraversalCursor(cursorContext)) {

                setNodeCursor(nodeId, kernelTransaction, nodeCursor);
                assertCursorHits(cursorContext, 1);

                int[] everyType = new int[] {
                    // Outgoing
                    kernelTransaction.tokenRead().relationshipType(O1.name()),
                    kernelTransaction.tokenRead().relationshipType(O2.name()),

                    // Incoming
                    kernelTransaction.tokenRead().relationshipType(I1.name()),
                    kernelTransaction.tokenRead().relationshipType(I2.name()),

                    // Loop
                    kernelTransaction.tokenRead().relationshipType(L1.name()),
                    kernelTransaction.tokenRead().relationshipType(L2.name()),
                };

                DirectedTypes directedTypes = new DirectedTypes(kernelTransaction.memoryTracker());

                // We are going to test every possible way of assigning types from `everyType` to
                // either be included in bothTypes or completely excluded

                // We decide between 2 outcomes (bothTypes or no array) 6 times (one time for every type),
                // resulting in 2^6 = 64 combinations.
                final int N_COMBINATIONS = 64;

                IntArrayList expectedTraversedTypes = new IntArrayList();
                IntArrayList traversedTypes = new IntArrayList();
                for (int iteration = 0; iteration < N_COMBINATIONS; iteration++) {

                    directedTypes.clear();
                    directedTypes.addUntyped(Direction.OUTGOING);

                    expectedTraversedTypes.clear();

                    for (int typeIndex = 0; typeIndex < everyType.length; typeIndex++) {

                        // Every type is assigned 1 of 2 outcomes. Either it's placed in `bothTypes`, or it's placed
                        // nowhere. The `typeIndex`-th digit in the binary representation of `iteration` determines the
                        // outcome of the corresponding type.
                        switch ((iteration >> typeIndex) & 1) {
                            case 0:
                                expectedTraversedTypes.add(everyType[typeIndex]);

                                directedTypes.addTypes(new int[] {everyType[typeIndex]}, Direction.BOTH);
                                break;
                            case 1:
                                // Don't add to any of the three arrays
                                break;
                        }
                    }

                    // Since we allow all types in the incoming direction, we expect to traverse these types
                    // every time.
                    if (!expectedTraversedTypes.contains(everyType[0])) {
                        expectedTraversedTypes.add(everyType[0]);
                    }
                    if (!expectedTraversedTypes.contains(everyType[1])) {
                        expectedTraversedTypes.add(everyType[1]);
                    }
                    if (!expectedTraversedTypes.contains(everyType[4])) {
                        expectedTraversedTypes.add(everyType[4]);
                    }
                    if (!expectedTraversedTypes.contains(everyType[5])) {
                        expectedTraversedTypes.add(everyType[5]);
                    }

                    directedTypes.compact();

                    RelationshipTraversalCursor traversalCursor =
                            RelationshipSelections.multiTypeMultiDirectionCursor(relCursor, nodeCursor, directedTypes);

                    traversedTypes.clear();
                    while (traversalCursor.next()) {
                        traversedTypes.add(traversalCursor.type());
                    }
                    assertEquals(expectedTraversedTypes.sortThis(), traversedTypes.sortThis());
                }
            }
        }
    }

    @Test
    void multiDirectionalMultiTypeAllOutgoingWriteInSameTx() {

        try (var transaction = database.beginTx()) {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var cursorContext = kernelTransaction.cursorContext();
            long nodeId = getNodeWithManyDifferentIncidentTypes(transaction);
            try (var nodeCursor = cursors.allocateNodeCursor(cursorContext);
                    var relCursor = cursors.allocateRelationshipTraversalCursor(cursorContext)) {

                setNodeCursor(nodeId, kernelTransaction, nodeCursor);
                assertCursorHits(cursorContext, 1);

                int[] everyType = new int[] {
                    // Outgoing
                    kernelTransaction.tokenRead().relationshipType(O1.name()),
                    kernelTransaction.tokenRead().relationshipType(O2.name()),

                    // Incoming
                    kernelTransaction.tokenRead().relationshipType(I1.name()),
                    kernelTransaction.tokenRead().relationshipType(I2.name()),

                    // Loop
                    kernelTransaction.tokenRead().relationshipType(L1.name()),
                    kernelTransaction.tokenRead().relationshipType(L2.name()),
                };

                // Null signifies all types, and since the three arrays need to be disjoint,
                // null outgoing means empty incoming.
                DirectedTypes directedTypes = new DirectedTypes(kernelTransaction.memoryTracker());

                // We are going to test every possible way of assigning types from `everyType` to
                // either be included in bothTypes or completely excluded

                // We decide between 2 outcomes (bothTypes or no array) 6 times (one time for every type),
                // resulting in 2^6 = 64 combinations.
                final int N_COMBINATIONS = 64;

                IntArrayList expectedTraversedTypes = new IntArrayList();
                IntArrayList traversedTypes = new IntArrayList();
                for (int iteration = 0; iteration < N_COMBINATIONS; iteration++) {

                    directedTypes.clear();
                    directedTypes.addUntyped(Direction.OUTGOING);

                    expectedTraversedTypes.clear();

                    for (int typeIndex = 0; typeIndex < everyType.length; typeIndex++) {

                        // Every type is assigned 1 of 2 outcomes. Either it's placed in `bothTypes`, or it's placed
                        // nowhere. The `typeIndex`-th digit in the binary representation of `iteration` determines the
                        // outcome of the corresponding type.
                        switch ((iteration >> typeIndex) & 1) {
                            case 0:
                                expectedTraversedTypes.add(everyType[typeIndex]);
                                directedTypes.addTypes(new int[] {everyType[typeIndex]}, Direction.BOTH);
                                break;
                            case 1:
                                // Don't add to any of the three arrays
                                break;
                        }
                    }

                    // Since we allow all types in the incoming direction, we expect to traverse these types
                    // every time.
                    if (!expectedTraversedTypes.contains(everyType[0])) {
                        expectedTraversedTypes.add(everyType[0]);
                    }
                    if (!expectedTraversedTypes.contains(everyType[1])) {
                        expectedTraversedTypes.add(everyType[1]);
                    }
                    if (!expectedTraversedTypes.contains(everyType[4])) {
                        expectedTraversedTypes.add(everyType[4]);
                    }
                    if (!expectedTraversedTypes.contains(everyType[5])) {
                        expectedTraversedTypes.add(everyType[5]);
                    }

                    directedTypes.compact();

                    RelationshipTraversalCursor traversalCursor =
                            RelationshipSelections.multiTypeMultiDirectionCursor(relCursor, nodeCursor, directedTypes);

                    traversedTypes.clear();
                    while (traversalCursor.next()) {
                        traversedTypes.add(traversalCursor.type());
                    }
                    assertEquals(expectedTraversedTypes.sortThis(), traversedTypes.sortThis());
                }
            }
        }
    }

    @Test
    void multiDirectionalMultiTypeAllIncoming() {
        long nodeId = getNodeWithManyDifferentIncidentTypes();

        try (var transaction = database.beginTx()) {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var cursorContext = kernelTransaction.cursorContext();
            try (var nodeCursor = cursors.allocateNodeCursor(cursorContext);
                    var relCursor = cursors.allocateRelationshipTraversalCursor(cursorContext)) {

                setNodeCursor(nodeId, kernelTransaction, nodeCursor);
                assertCursorHits(cursorContext, 1);

                int[] everyType = new int[] {
                    // Outgoing
                    kernelTransaction.tokenRead().relationshipType(O1.name()),
                    kernelTransaction.tokenRead().relationshipType(O2.name()),

                    // Incoming
                    kernelTransaction.tokenRead().relationshipType(I1.name()),
                    kernelTransaction.tokenRead().relationshipType(I2.name()),

                    // Loop
                    kernelTransaction.tokenRead().relationshipType(L1.name()),
                    kernelTransaction.tokenRead().relationshipType(L2.name()),
                };

                // Null signifies all types, and since the three arrays need to be disjoint,
                // null incoming means empty outgoing.
                DirectedTypes directedTypes = new DirectedTypes(kernelTransaction.memoryTracker());

                // We are going to test every possible way of assigning types from `everyType` to
                // either be included in bothTypes or completely excluded

                // We decide between 2 outcomes (bothTypes or no array) 6 times (one time for every type),
                // resulting in 2^6 = 64 combinations.
                final int N_COMBINATIONS = 64;

                IntArrayList expectedTraversedTypes = new IntArrayList();
                IntArrayList traversedTypes = new IntArrayList();
                for (int iteration = 0; iteration < N_COMBINATIONS; iteration++) {

                    directedTypes.clear();
                    directedTypes.addUntyped(Direction.INCOMING);

                    expectedTraversedTypes.clear();

                    for (int typeIndex = 0; typeIndex < everyType.length; typeIndex++) {

                        // Every type is assigned 1 of 2 outcomes. Either it's placed in `bothTypes`, or it's placed
                        // nowhere. The `typeIndex`-th digit in the binary representation of `iteration` determines the
                        // outcome of the corresponding type.
                        switch ((iteration >> typeIndex) & 1) {
                            case 0:
                                expectedTraversedTypes.add(everyType[typeIndex]);
                                directedTypes.addTypes(new int[] {everyType[typeIndex]}, Direction.BOTH);
                                break;
                            case 1:
                                // Don't add to any of the three arrays
                                break;
                        }
                    }

                    // Since we allow all types in the incoming direction, we expect to traverse these types
                    // every time.
                    if (!expectedTraversedTypes.contains(everyType[2])) {
                        expectedTraversedTypes.add(everyType[2]);
                    }
                    if (!expectedTraversedTypes.contains(everyType[3])) {
                        expectedTraversedTypes.add(everyType[3]);
                    }
                    if (!expectedTraversedTypes.contains(everyType[4])) {
                        expectedTraversedTypes.add(everyType[4]);
                    }
                    if (!expectedTraversedTypes.contains(everyType[5])) {
                        expectedTraversedTypes.add(everyType[5]);
                    }

                    directedTypes.compact();

                    RelationshipTraversalCursor traversalCursor =
                            RelationshipSelections.multiTypeMultiDirectionCursor(relCursor, nodeCursor, directedTypes);

                    traversedTypes.clear();
                    while (traversalCursor.next()) {
                        traversedTypes.add(traversalCursor.type());
                    }
                    assertEquals(expectedTraversedTypes.sortThis(), traversedTypes.sortThis());
                }
            }
        }
    }

    @Test
    void multiDirectionalMultiTypeAllIncomingWriteInSameTx() {

        try (var transaction = database.beginTx()) {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var cursorContext = kernelTransaction.cursorContext();
            long nodeId = getNodeWithManyDifferentIncidentTypes(transaction);
            try (var nodeCursor = cursors.allocateNodeCursor(cursorContext);
                    var relCursor = cursors.allocateRelationshipTraversalCursor(cursorContext)) {

                setNodeCursor(nodeId, kernelTransaction, nodeCursor);
                assertCursorHits(cursorContext, 1);

                int[] everyType = new int[] {
                    // Outgoing
                    kernelTransaction.tokenRead().relationshipType(O1.name()),
                    kernelTransaction.tokenRead().relationshipType(O2.name()),

                    // Incoming
                    kernelTransaction.tokenRead().relationshipType(I1.name()),
                    kernelTransaction.tokenRead().relationshipType(I2.name()),

                    // Loop
                    kernelTransaction.tokenRead().relationshipType(L1.name()),
                    kernelTransaction.tokenRead().relationshipType(L2.name()),
                };

                // Null signifies all types, and since the three arrays need to be disjoint,
                // null incoming means empty outgoing.
                DirectedTypes directedTypes = new DirectedTypes(kernelTransaction.memoryTracker());

                // We are going to test every possible way of assigning types from `everyType` to
                // either be included in bothTypes or completely excluded

                // We decide between 2 outcomes (bothTypes or no array) 6 times (one time for every type),
                // resulting in 2^6 = 64 combinations.
                final int N_COMBINATIONS = 64;

                IntArrayList expectedTraversedTypes = new IntArrayList();
                IntArrayList traversedTypes = new IntArrayList();
                for (int iteration = 0; iteration < N_COMBINATIONS; iteration++) {

                    directedTypes.clear();
                    directedTypes.addUntyped(Direction.INCOMING);

                    expectedTraversedTypes.clear();

                    expectedTraversedTypes.clear();

                    for (int typeIndex = 0; typeIndex < everyType.length; typeIndex++) {

                        // Every type is assigned 1 of 2 outcomes. Either it's placed in `bothTypes`, or it's placed
                        // nowhere. The `typeIndex`-th digit in the binary representation of `iteration` determines the
                        // outcome of the corresponding type.
                        switch ((iteration >> typeIndex) & 1) {
                            case 0:
                                expectedTraversedTypes.add(everyType[typeIndex]);
                                directedTypes.addTypes(new int[] {everyType[typeIndex]}, Direction.BOTH);
                                break;
                            case 1:
                                // Don't add to any of the three arrays
                                break;
                        }
                    }

                    // Since we allow all types in the incoming direction, we expect to traverse these types
                    // every time.
                    if (!expectedTraversedTypes.contains(everyType[2])) {
                        expectedTraversedTypes.add(everyType[2]);
                    }
                    if (!expectedTraversedTypes.contains(everyType[3])) {
                        expectedTraversedTypes.add(everyType[3]);
                    }
                    if (!expectedTraversedTypes.contains(everyType[4])) {
                        expectedTraversedTypes.add(everyType[4]);
                    }
                    if (!expectedTraversedTypes.contains(everyType[5])) {
                        expectedTraversedTypes.add(everyType[5]);
                    }

                    directedTypes.compact();

                    RelationshipTraversalCursor traversalCursor =
                            RelationshipSelections.multiTypeMultiDirectionCursor(relCursor, nodeCursor, directedTypes);

                    traversedTypes.clear();
                    while (traversalCursor.next()) {
                        traversedTypes.add(traversalCursor.type());
                    }
                    assertEquals(expectedTraversedTypes.sortThis(), traversedTypes.sortThis());
                }
            }
        }
    }

    private long getSparseNodeId() {
        try (Transaction tx = database.beginTx()) {
            var source = tx.createNode();
            var endNode1 = tx.createNode();
            var endNode2 = tx.createNode();
            source.createRelationshipTo(endNode1, relationshipType);
            source.createRelationshipTo(endNode2, relationshipType);
            endNode1.createRelationshipTo(source, relationshipType);
            endNode2.createRelationshipTo(source, relationshipType);
            long nodeId = source.getId();
            tx.commit();
            return nodeId;
        }
    }

    private long getDenseNodeId() {
        try (Transaction tx = database.beginTx()) {
            var source = tx.createNode();
            var endNode1 = tx.createNode();
            var endNode2 = tx.createNode();
            source.createRelationshipTo(endNode1, relationshipType);
            source.createRelationshipTo(endNode2, relationshipType);
            endNode1.createRelationshipTo(source, relationshipType);
            endNode2.createRelationshipTo(source, relationshipType);

            var other = withName("other");
            for (int i = 0; i < 100; i++) {
                var node = tx.createNode();
                source.createRelationshipTo(node, other);
            }
            long nodeId = source.getId();
            tx.commit();
            return nodeId;
        }
    }

    private long getNodeWithManyDifferentIncidentTypes() {
        try (Transaction tx = database.beginTx()) {
            long source = getNodeWithManyDifferentIncidentTypes(tx);
            tx.commit();

            return source;
        }
    }

    private long getNodeWithManyDifferentIncidentTypes(Transaction tx) {
        var source = tx.createNode();
        var target = tx.createNode();

        source.createRelationshipTo(target, O1);
        source.createRelationshipTo(target, O2);

        target.createRelationshipTo(source, I1);
        target.createRelationshipTo(source, I2);

        source.createRelationshipTo(source, L1);
        source.createRelationshipTo(source, L2);

        return source.getId();
    }

    private static void setNodeCursor(long nodeId, KernelTransaction kernelTransaction, NodeCursor nodeCursor) {
        kernelTransaction.dataRead().singleNode(nodeId, nodeCursor);
        assertTrue(nodeCursor.next());
    }

    private static void consumeCursor(RelationshipTraversalCursor cursor) {
        while (cursor.next()) {
            // consume cursor
        }
    }

    private static void assertCursorHits(CursorContext cursorContext, int atMostHits) {
        PageCursorTracer cursorTracer = cursorContext.getCursorTracer();
        assertThat(cursorTracer.hits()).isLessThanOrEqualTo(atMostHits).isLessThanOrEqualTo(cursorTracer.pins());
        // Since the storage cursor is merely reset(), not closed the state of things is that not all unpins gets
        // registered due to cursor context being closed before the storage cursor on KTI#commit()
    }
}
