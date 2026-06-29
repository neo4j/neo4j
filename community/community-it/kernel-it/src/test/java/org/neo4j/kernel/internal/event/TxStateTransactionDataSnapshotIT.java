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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData.DataSelection;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.test.extension.SkipOnSpd;

@DbmsExtension
@RandomSupportExtension
class TxStateTransactionDataSnapshotIT {
    @Inject
    private GraphDatabaseAPI database;

    @Inject
    private RandomSupport random;

    private long emptySnapshotSize;

    @BeforeEach
    void setUp() {
        emptySnapshotSize = countEmptySnapshotSize();
    }

    @Test
    @SkipOnSpd(reason = "Properties read from store will be null for graph shard, resulting in less memory tracked")
    void countRemovedNodeWithPropertiesInTransactionStateSnapshot() {
        String nodeIdToDelete;
        int attachedPropertySize = (int) ByteUnit.mebiBytes(1);
        try (Transaction transaction = database.beginTx()) {
            var node = transaction.createNode(label("label1"), label("label2"));
            node.setProperty("a", random.nextAsciiStringOfLength(attachedPropertySize));
            node.setProperty("b", random.nextAsciiStringOfLength(attachedPropertySize));
            nodeIdToDelete = node.getElementId();
            transaction.commit();
        }

        try (Transaction transaction = database.beginTx()) {
            transaction.getNodeByElementId(nodeIdToDelete).delete();

            var kernelTransaction = getKernelTransaction(transaction);
            var transactionState = kernelTransaction.txState();
            final MemoryTracker memoryTracker = kernelTransaction.memoryTracker();

            // reset to count only snapshot memory
            var trackingData = resetMemoryTracker(memoryTracker);

            try (var snapshot = new TxStateTransactionDataSnapshot(
                    transactionState, kernelTransaction.newStorageReader(), kernelTransaction, true, null)) {
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
    @SkipOnSpd(reason = "Properties read from store will be null for graph shard, resulting in less memory tracked")
    void countRemovedRelationshipsWithPropertiesInTransactionStateSnapshot() {
        List<String> relationshipsIdToDelete;
        int attachedPropertySize = (int) ByteUnit.mebiBytes(1);
        try (Transaction transaction = database.beginTx()) {
            var start = transaction.createNode();
            var end = transaction.createNode();
            var relationship1 = start.createRelationshipTo(end, withName("type1"));
            var relationship2 = start.createRelationshipTo(end, withName("type2"));

            relationship1.setProperty("a", random.nextAsciiStringOfLength(attachedPropertySize));
            relationship2.setProperty("a", random.nextAsciiStringOfLength(attachedPropertySize));
            relationship2.setProperty("b", random.nextAsciiStringOfLength(attachedPropertySize));

            relationshipsIdToDelete = List.of(relationship1.getElementId(), relationship2.getElementId());
            transaction.commit();
        }

        assertThat(relationshipsIdToDelete).hasSize(2);

        try (Transaction transaction = database.beginTx()) {
            relationshipsIdToDelete.forEach(
                    id -> transaction.getRelationshipByElementId(id).delete());

            var kernelTransaction = getKernelTransaction(transaction);
            var transactionState = kernelTransaction.txState();
            final MemoryTracker memoryTracker = kernelTransaction.memoryTracker();

            // reset to count only snapshot memory
            var trackingData = resetMemoryTracker(memoryTracker);

            try (var snapshot = new TxStateTransactionDataSnapshot(
                    transactionState, kernelTransaction.newStorageReader(), kernelTransaction, true, null)) {
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
    @SkipOnSpd(reason = "Properties read from store will be null for graph shard, resulting in less memory tracked")
    void countChangedNodeInTransactionStateSnapshot() {
        String nodeIdToChange;
        int attachedPropertySize = (int) ByteUnit.mebiBytes(1);
        int doublePropertySize = attachedPropertySize * 2;
        Label label1 = label("label1");
        Label label2 = label("label2");
        final String property = "a";
        final String doubleProperty = "b";

        try (Transaction transaction = database.beginTx()) {
            var node = transaction.createNode(label1, label2);
            node.setProperty(property, random.nextAsciiStringOfLength(attachedPropertySize));
            node.setProperty(doubleProperty, random.nextAsciiStringOfLength(doublePropertySize));
            nodeIdToChange = node.getElementId();
            transaction.commit();
        }

        try (Transaction transaction = database.beginTx()) {
            var node = transaction.getNodeByElementId(nodeIdToChange);
            node.removeLabel(label1);
            node.setProperty(doubleProperty, random.nextAsciiStringOfLength(attachedPropertySize));
            node.removeProperty(property);
            node.addLabel(Label.label("newLabel"));

            var kernelTransaction = getKernelTransaction(transaction);
            var transactionState = kernelTransaction.txState();
            final MemoryTracker memoryTracker = kernelTransaction.memoryTracker();

            // reset to count only snapshot memory
            var trackingData = resetMemoryTracker(memoryTracker);

            try (var snapshot = new TxStateTransactionDataSnapshot(
                    transactionState, kernelTransaction.newStorageReader(), kernelTransaction, true, null)) {
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
    @SkipOnSpd(reason = "Properties read from store will be null for graph shard, resulting in less memory tracked")
    void countChangedRelationshipInTransactionStateSnapshot() {
        String relationshipIdToChange;
        int attachedPropertySize = (int) ByteUnit.mebiBytes(1);
        int doublePropertySize = attachedPropertySize * 2;
        final String property = "a";
        final String doubleProperty = "b";

        try (Transaction transaction = database.beginTx()) {
            var start = transaction.createNode();
            var end = transaction.createNode();
            var relationship = start.createRelationshipTo(end, withName("relType"));
            relationship.setProperty(property, random.nextAsciiStringOfLength(attachedPropertySize));
            relationship.setProperty(doubleProperty, random.nextAsciiStringOfLength(doublePropertySize));
            relationshipIdToChange = relationship.getElementId();
            transaction.commit();
        }

        try (Transaction transaction = database.beginTx()) {
            var relationship = transaction.getRelationshipByElementId(relationshipIdToChange);
            relationship.setProperty(doubleProperty, random.nextAsciiStringOfLength(attachedPropertySize));
            relationship.removeProperty(property);

            var kernelTransaction = getKernelTransaction(transaction);
            var transactionState = kernelTransaction.txState();
            final MemoryTracker memoryTracker = kernelTransaction.memoryTracker();

            // reset to count only snapshot memory
            var trackingData = resetMemoryTracker(memoryTracker);

            try (var snapshot = new TxStateTransactionDataSnapshot(
                    transactionState, kernelTransaction.newStorageReader(), kernelTransaction, false, null)) {
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
                    transactionState, kernelTransaction.newStorageReader(), kernelTransaction, true, null)) {
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
                    transactionState, kernelTransaction.newStorageReader(), kernelTransaction, true, null)) {
                // no work for snapshot
            }
            kernelTransaction.storeCursors().reset(cursorContext);

            assertThat(cursorTracer.pins()).isGreaterThan(0);
            assertThat(cursorTracer.hits()).isEqualTo(cursorTracer.pins());
            assertThat(cursorTracer.unpins()).isEqualTo(cursorTracer.pins());
        }
    }

    @SkipOnSpd(reason = "When running SPD there's some TransactionEventListener that selects this data")
    @Test
    void shouldSkipReplacedPropertyValuesIfToldTo() {
        // given
        String propertyKey = "p";
        String originalValue = "original";
        String replacedValue = "replaced";
        String nodeId;
        try (Transaction transaction = database.beginTx()) {
            var node = transaction.createNode();
            node.setProperty(propertyKey, originalValue);
            nodeId = node.getElementId();
            transaction.commit();
        }

        // when snapshotting without the replacedPropertyValues capability
        try (Transaction transaction = database.beginTx()) {
            transaction.getNodeByElementId(nodeId).setProperty(propertyKey, replacedValue);

            var kernelTransaction = getKernelTransaction(transaction);
            try (var snapshot = new TxStateTransactionDataSnapshot(
                    kernelTransaction.txState(),
                    kernelTransaction.newStorageReader(),
                    kernelTransaction,
                    true,
                    Set.of())) {
                // then the previously committed value is not read from store
                var entry = Iterables.single(snapshot.assignedNodeProperties());
                assertThat(entry.key()).isEqualTo(propertyKey);
                assertThat(entry.value()).isEqualTo(replacedValue);
                assertThat(entry.previouslyCommittedValue()).isNull();
            }
        }

        // when snapshotting with the replacedPropertyValues capability (positive control)
        try (Transaction transaction = database.beginTx()) {
            transaction.getNodeByElementId(nodeId).setProperty(propertyKey, replacedValue);

            var kernelTransaction = getKernelTransaction(transaction);
            try (var snapshot = new TxStateTransactionDataSnapshot(
                    kernelTransaction.txState(),
                    kernelTransaction.newStorageReader(),
                    kernelTransaction,
                    true,
                    Set.of(DataSelection.replacedPropertyValues))) {
                // then the previously committed value is read from store
                var entry = Iterables.single(snapshot.assignedNodeProperties());
                assertThat(entry.key()).isEqualTo(propertyKey);
                assertThat(entry.value()).isEqualTo(replacedValue);
                assertThat(entry.previouslyCommittedValue()).isEqualTo(originalValue);
            }
        }
    }

    @SkipOnSpd(reason = "When running SPD there's some TransactionEventListener that selects this data")
    @Test
    void shouldSkipRemovedPropertyValuesIfToldTo() {
        // given
        String propertyKey = "p";
        String value = "value";
        String nodeId;
        try (Transaction transaction = database.beginTx()) {
            var node = transaction.createNode();
            node.setProperty(propertyKey, value);
            nodeId = node.getElementId();
            transaction.commit();
        }

        // when snapshotting without the removedPropertyValues capability
        try (Transaction transaction = database.beginTx()) {
            transaction.getNodeByElementId(nodeId).removeProperty(propertyKey);

            var kernelTransaction = getKernelTransaction(transaction);
            try (var snapshot = new TxStateTransactionDataSnapshot(
                    kernelTransaction.txState(),
                    kernelTransaction.newStorageReader(),
                    kernelTransaction,
                    true,
                    Set.of())) {
                // then the previously committed value is not read from store
                var entry = Iterables.single(snapshot.removedNodeProperties());
                assertThat(entry.key()).isEqualTo(propertyKey);
                assertThatThrownBy(entry::value).hasMessageContaining("This property has been removed");
                assertThat(entry.previouslyCommittedValue()).isNull();
            }
        }

        // when snapshotting with the removedPropertyValues capability (positive control)
        try (Transaction transaction = database.beginTx()) {
            transaction.getNodeByElementId(nodeId).removeProperty(propertyKey);

            var kernelTransaction = getKernelTransaction(transaction);
            try (var snapshot = new TxStateTransactionDataSnapshot(
                    kernelTransaction.txState(),
                    kernelTransaction.newStorageReader(),
                    kernelTransaction,
                    true,
                    Set.of(DataSelection.removedPropertyValues))) {
                // then the previously committed value is read from store
                var entry = Iterables.single(snapshot.removedNodeProperties());
                assertThat(entry.key()).isEqualTo(propertyKey);
                assertThatThrownBy(entry::value).hasMessageContaining("This property has been removed");
                assertThat(entry.previouslyCommittedValue()).isEqualTo(value);
            }
        }
    }

    @Test
    void shouldSkipLabelsOfDeletedNodesIfToldTo() {
        // given
        Label label = Label.label("label");
        String nodeId;
        try (Transaction transaction = database.beginTx()) {
            var node = transaction.createNode(label);
            nodeId = node.getElementId();
            transaction.commit();
        }

        // when snapshotting without the deletedNodeLabels capability
        try (Transaction transaction = database.beginTx()) {
            transaction.getNodeByElementId(nodeId).delete();

            var kernelTransaction = getKernelTransaction(transaction);
            try (var snapshot = new TxStateTransactionDataSnapshot(
                    kernelTransaction.txState(),
                    kernelTransaction.newStorageReader(),
                    kernelTransaction,
                    true,
                    Set.of())) {
                // then the previously committed labels are not read from store
                var entry = Iterables.singleOrNull(snapshot.removedLabels());
                assertThat(entry).isNull();
            }
        }

        // when snapshotting with the deletedNodeLabels capability (positive control)
        try (Transaction transaction = database.beginTx()) {
            transaction.getNodeByElementId(nodeId).delete();

            var kernelTransaction = getKernelTransaction(transaction);
            try (var snapshot = new TxStateTransactionDataSnapshot(
                    kernelTransaction.txState(),
                    kernelTransaction.newStorageReader(),
                    kernelTransaction,
                    true,
                    Set.of(DataSelection.deletedNodeLabels))) {
                // then the previously committed value is read from store
                var entry = Iterables.single(snapshot.removedLabels());
                assertThat(entry.label().name()).isEqualTo(label.name());
                assertThat(entry.node().getElementId()).isEqualTo(nodeId);
            }
        }
    }

    @Test
    void shouldAccessDeletedEntitiesEvenIfNotSelectingRemovedProperties() throws TransactionFailureException {
        // given
        String nodeId;
        String relationshipId;
        try (Transaction transaction = database.beginTx()) {
            var node = transaction.createNode();
            node.setProperty("p", "v");
            nodeId = node.getElementId();
            Relationship relationship = node.createRelationshipTo(transaction.createNode(), withName("relType"));
            relationship.setProperty("p", "v");
            relationshipId = relationship.getElementId();
            transaction.commit();
        }

        // when
        try (Transaction transaction = database.beginTx()) {
            Node node = transaction.getNodeByElementId(nodeId);
            Relationship relationship = transaction.getRelationshipByElementId(relationshipId);
            node.delete();
            relationship.delete();

            var kernelTransaction = getKernelTransaction(transaction);
            kernelTransaction.commit(new KernelTransaction.Monitor() {
                private TxStateTransactionDataSnapshot snapshot;

                @Override
                public void beforeApply() {
                    snapshot = new TxStateTransactionDataSnapshot(
                            kernelTransaction.txState(),
                            kernelTransaction.newStorageReader(),
                            kernelTransaction,
                            true,
                            Set.of());
                }

                @Override
                public void afterApply() {
                    try (var close = snapshot) {
                        assertThat(snapshot.isDeleted(node));
                        assertThat(snapshot.isDeleted(relationship));
                        assertThat(Iterables.asList(snapshot.deletedNodes())).isEqualTo(List.of(node));
                        assertThat(Iterables.asList(snapshot.deletedRelationships()))
                                .isEqualTo(List.of(relationship));
                    }
                }
            });
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
                    transactionState, kernelTransaction.newStorageReader(), kernelTransaction, false, null)) {
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
