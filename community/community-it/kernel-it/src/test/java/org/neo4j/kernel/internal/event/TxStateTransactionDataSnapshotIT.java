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

import static org.apache.commons.lang3.RandomStringUtils.randomAscii;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

@DbmsExtension
class TxStateTransactionDataSnapshotIT {
    @Inject
    private GraphDatabaseAPI database;

    private long emptySnapshotSize;

    @BeforeEach
    void setUp() {
        emptySnapshotSize = countEmptySnapshotSize();
    }

    @Test
    void countRemovedNodeWithPropertiesInTransactionStateSnapshot() {
        long nodeIdToDelete;
        int attachedPropertySize = (int) ByteUnit.mebiBytes(1);
        try (Transaction transaction = database.beginTx()) {
            var node = transaction.createNode(label("label1"), label("label2"));
            node.setProperty("a", randomAscii(attachedPropertySize));
            node.setProperty("b", randomAscii(attachedPropertySize));
            nodeIdToDelete = node.getId();
            transaction.commit();
        }

        try (Transaction transaction = database.beginTx()) {
            transaction.getNodeById(nodeIdToDelete).delete();

            var kernelTransaction = getKernelTransaction(transaction);
            var transactionState = kernelTransaction.txState();
            final MemoryTracker memoryTracker = kernelTransaction.memoryTracker();

            // reset to count only snapshot memory
            var trackingData = resetMemoryTracker(memoryTracker);

            try (var snapshot = new TxStateTransactionDataSnapshot(
                    transactionState, kernelTransaction.newStorageReader(), kernelTransaction, true)) {
                assertThat(memoryTracker.usedNativeMemory()).isZero();
                assertThat(memoryTracker.estimatedHeapMemory())
                        .isGreaterThanOrEqualTo(emptySnapshotSize
                                + (2 * attachedPropertySize)
                                + (2 * NodePropertyEntryView.SHALLOW_SIZE)
                                + (2 * LabelEntryView.SHALLOW_SIZE));
            } finally {
                restoreMemoryTracker(memoryTracker, trackingData);
            }
        }
    }

    @Test
    void countRemovedRelationshipsWithPropertiesInTransactionStateSnapshot() {
        List<Long> relationshipsIdToDelete;
        int attachedPropertySize = (int) ByteUnit.mebiBytes(1);
        try (Transaction transaction = database.beginTx()) {
            var start = transaction.createNode();
            var end = transaction.createNode();
            var relationship1 = start.createRelationshipTo(end, withName("type1"));
            var relationship2 = start.createRelationshipTo(end, withName("type2"));

            relationship1.setProperty("a", randomAscii(attachedPropertySize));
            relationship2.setProperty("a", randomAscii(attachedPropertySize));
            relationship2.setProperty("b", randomAscii(attachedPropertySize));

            relationshipsIdToDelete = List.of(relationship1.getId(), relationship2.getId());
            transaction.commit();
        }

        assertThat(relationshipsIdToDelete).hasSize(2);

        try (Transaction transaction = database.beginTx()) {
            relationshipsIdToDelete.forEach(
                    id -> transaction.getRelationshipById(id).delete());

            var kernelTransaction = getKernelTransaction(transaction);
            var transactionState = kernelTransaction.txState();
            final MemoryTracker memoryTracker = kernelTransaction.memoryTracker();

            // reset to count only snapshot memory
            var trackingData = resetMemoryTracker(memoryTracker);

            try (var snapshot = new TxStateTransactionDataSnapshot(
                    transactionState, kernelTransaction.newStorageReader(), kernelTransaction, true)) {
                assertThat(memoryTracker.usedNativeMemory()).isZero();
                assertThat(memoryTracker.estimatedHeapMemory())
                        .isGreaterThanOrEqualTo(emptySnapshotSize
                                + (3 * attachedPropertySize)
                                + (2 * RelationshipPropertyEntryView.SHALLOW_SIZE));
            } finally {
                restoreMemoryTracker(memoryTracker, trackingData);
            }
        }
    }

    @Test
    void countChangedNodeInTransactionStateSnapshot() {
        long nodeIdToChange;
        int attachedPropertySize = (int) ByteUnit.mebiBytes(1);
        int doublePropertySize = attachedPropertySize * 2;
        Label label1 = label("label1");
        Label label2 = label("label2");
        final String property = "a";
        final String doubleProperty = "b";

        try (Transaction transaction = database.beginTx()) {
            var node = transaction.createNode(label1, label2);
            node.setProperty(property, randomAscii(attachedPropertySize));
            node.setProperty(doubleProperty, randomAscii(doublePropertySize));
            nodeIdToChange = node.getId();
            transaction.commit();
        }

        try (Transaction transaction = database.beginTx()) {
            var node = transaction.getNodeById(nodeIdToChange);
            node.removeLabel(label1);
            node.setProperty(doubleProperty, randomAscii(attachedPropertySize));
            node.removeProperty(property);
            node.addLabel(Label.label("newLabel"));

            var kernelTransaction = getKernelTransaction(transaction);
            var transactionState = kernelTransaction.txState();
            final MemoryTracker memoryTracker = kernelTransaction.memoryTracker();

            // reset to count only snapshot memory
            var trackingData = resetMemoryTracker(memoryTracker);

            try (var snapshot = new TxStateTransactionDataSnapshot(
                    transactionState, kernelTransaction.newStorageReader(), kernelTransaction, true)) {
                assertThat(memoryTracker.usedNativeMemory()).isZero();
                assertThat(memoryTracker.estimatedHeapMemory())
                        .isGreaterThanOrEqualTo(emptySnapshotSize
                                + (attachedPropertySize + doublePropertySize)
                                + (2 * NodePropertyEntryView.SHALLOW_SIZE)
                                + (2 * LabelEntryView.SHALLOW_SIZE));
            } finally {
                restoreMemoryTracker(memoryTracker, trackingData);
            }
        }
    }

    @Test
    void countChangedRelationshipInTransactionStateSnapshot() {
        long relationshipIdToChange;
        int attachedPropertySize = (int) ByteUnit.mebiBytes(1);
        int doublePropertySize = attachedPropertySize * 2;
        final String property = "a";
        final String doubleProperty = "b";

        try (Transaction transaction = database.beginTx()) {
            var start = transaction.createNode();
            var end = transaction.createNode();
            var relationship = start.createRelationshipTo(end, withName("relType"));
            relationship.setProperty(property, randomAscii(attachedPropertySize));
            relationship.setProperty(doubleProperty, randomAscii(doublePropertySize));
            relationshipIdToChange = relationship.getId();
            transaction.commit();
        }

        try (Transaction transaction = database.beginTx()) {
            var relationship = transaction.getRelationshipById(relationshipIdToChange);
            relationship.setProperty(doubleProperty, randomAscii(attachedPropertySize));
            relationship.removeProperty(property);

            var kernelTransaction = getKernelTransaction(transaction);
            var transactionState = kernelTransaction.txState();
            final MemoryTracker memoryTracker = kernelTransaction.memoryTracker();

            // reset to count only snapshot memory
            var trackingData = resetMemoryTracker(memoryTracker);

            try (var snapshot = new TxStateTransactionDataSnapshot(
                    transactionState, kernelTransaction.newStorageReader(), kernelTransaction, false)) {
                assertThat(memoryTracker.usedNativeMemory()).isZero();
                assertThat(memoryTracker.estimatedHeapMemory())
                        .isGreaterThanOrEqualTo(emptySnapshotSize
                                + (attachedPropertySize + doublePropertySize)
                                + (2 * RelationshipPropertyEntryView.SHALLOW_SIZE));
            } finally {
                restoreMemoryTracker(memoryTracker, trackingData);
            }
        }
    }

    @Test
    void noPageCacheAccessOnEmptyTransactionSnapshot() {
        try (Transaction transaction = database.beginTx()) {
            var kernelTransaction = getKernelTransaction(transaction);
            var transactionState = kernelTransaction.txState();
            var cursorContext = kernelTransaction.cursorContext();
            try (var snapshot = new TxStateTransactionDataSnapshot(
                    transactionState, kernelTransaction.newStorageReader(), kernelTransaction, true)) {
                // empty
            }
            assertZeroTracer(cursorContext);
        }
    }

    @Test
    void tracePageCacheAccessOnTransactionSnapshotCreation() {
        String nodeId1;
        String nodeId2;
        String relationshipId;
        try (Transaction transaction = database.beginTx()) {
            var node1 = transaction.createNode();
            // Create some more nodes such that the two likely will end up on different pages
            for (int i = 0; i < 1000; i++) {
                transaction.createNode();
            }
            var node2 = transaction.createNode();
            var relationship = node1.createRelationshipTo(node2, withName("marker"));
            node1.setProperty("foo", "bar");
            nodeId1 = node1.getElementId();
            nodeId2 = node2.getElementId();
            relationshipId = relationship.getElementId();
            transaction.commit();
        }
        try (Transaction transaction = database.beginTx()) {
            transaction.getNodeByElementId(nodeId1).delete();
            transaction.getNodeByElementId(nodeId2).delete();
            transaction.getRelationshipByElementId(relationshipId).delete();

            var kernelTransaction = getKernelTransaction(transaction);
            var transactionState = kernelTransaction.txState();
            var cursorContext = kernelTransaction.cursorContext();
            PageCursorTracer cursorTracer = cursorContext.getCursorTracer();
            ((DefaultPageCursorTracer) cursorTracer).setIgnoreCounterCheck(true);
            kernelTransaction.storeCursors().reset(cursorContext);
            cursorTracer.reportEvents();

            try (var snapshot = new TxStateTransactionDataSnapshot(
                    transactionState, kernelTransaction.newStorageReader(), kernelTransaction, true)) {
                // no work for snapshot
            }
            kernelTransaction.storeCursors().reset(cursorContext);

            assertThat(cursorTracer.pins()).isGreaterThan(0);
            assertThat(cursorTracer.hits()).isEqualTo(cursorTracer.pins());
            assertThat(cursorTracer.unpins()).isEqualTo(cursorTracer.pins());
        }
    }

    private static KernelTransactionImplementation getKernelTransaction(Transaction transaction) {
        return (KernelTransactionImplementation) ((InternalTransaction) transaction).kernelTransaction();
    }

    private static void assertZeroTracer(CursorContext cursorContext) {
        PageCursorTracer cursorTracer = cursorContext.getCursorTracer();
        assertThat(cursorTracer.pins()).isZero();
        assertThat(cursorTracer.hits()).isZero();
        assertThat(cursorTracer.unpins()).isZero();
    }

    private long countEmptySnapshotSize() {
        try (Transaction transaction = database.beginTx()) {
            var kernelTransaction = getKernelTransaction(transaction);
            var transactionState = kernelTransaction.txState();
            final MemoryTracker memoryTracker = kernelTransaction.memoryTracker();

            // reset to count only snapshot memory
            resetMemoryTracker(memoryTracker);

            try (var snapshot = new TxStateTransactionDataSnapshot(
                    transactionState, kernelTransaction.newStorageReader(), kernelTransaction, false)) {
                return memoryTracker.estimatedHeapMemory();
            }
        }
    }

    private static MemoryTrackingData resetMemoryTracker(MemoryTracker memoryTracker) {
        var trackingData =
                new MemoryTrackingData(memoryTracker.estimatedHeapMemory(), memoryTracker.usedNativeMemory());
        memoryTracker.releaseHeap(trackingData.heapUsage());
        memoryTracker.releaseNative(trackingData.nativeUsage());
        return trackingData;
    }

    private static void restoreMemoryTracker(MemoryTracker memoryTracker, MemoryTrackingData restoreData) {
        memoryTracker.allocateHeap(restoreData.heapUsage());
        memoryTracker.allocateNative(restoreData.nativeUsage());
    }

    private record MemoryTrackingData(long heapUsage, long nativeUsage) {}
}
