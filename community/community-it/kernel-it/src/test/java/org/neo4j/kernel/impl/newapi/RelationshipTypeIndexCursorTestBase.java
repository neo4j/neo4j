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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.newapi.IndexReadAsserts.assertRelationshipCount;
import static org.neo4j.kernel.impl.newapi.IndexReadAsserts.assertRelationships;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.primitive.LongLists;
import org.eclipse.collections.api.factory.primitive.LongSets;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.kernel.api.TokenReadSession;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

abstract class RelationshipTypeIndexCursorTestBase<G extends KernelAPIWriteTestSupport>
        extends KernelAPIWriteTestBase<G> {
    private static final int typeOne = 1;
    private static final int typeTwo = 2;
    private static final int typeThree = 3;

    @ParameterizedTest
    @EnumSource(value = IndexOrder.class)
    void shouldFindRelationshipsByType(IndexOrder order) throws KernelException {
        // GIVEN
        long toDelete;
        long relTwo;
        long relThree;
        long relTwo2;
        long relThree2;
        long relThree3;
        try (KernelTransaction tx = beginTransaction()) {
            createRelationship(tx.dataWrite(), typeOne);
            relTwo = createRelationship(tx.dataWrite(), typeTwo);
            relThree = createRelationship(tx.dataWrite(), typeThree);
            toDelete = createRelationship(tx.dataWrite(), typeOne);
            relTwo2 = createRelationship(tx.dataWrite(), typeTwo);
            relThree2 = createRelationship(tx.dataWrite(), typeThree);
            relThree3 = createRelationship(tx.dataWrite(), typeThree);
            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            tx.dataWrite().relationshipDelete(toDelete);
            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            final var assertOrder = isNodeBased(tx) ? IndexOrder.NONE : order;

            try (RelationshipTypeIndexCursor cursor = tx.cursors().allocateRelationshipTypeIndexCursor(NULL_CONTEXT)) {
                MutableLongSet uniqueIds = new LongHashSet();

                // WHEN
                relationshipTypeScan(tx, typeOne, cursor, order);

                // THEN
                assertRelationshipCount(cursor, 1, uniqueIds);

                // WHEN
                relationshipTypeScan(tx, typeTwo, cursor, order);

                // THEN
                assertRelationships(cursor, uniqueIds, assertOrder, relTwo, relTwo2);

                // WHEN
                relationshipTypeScan(tx, typeThree, cursor, order);

                // THEN
                assertRelationships(cursor, uniqueIds, assertOrder, relThree, relThree2, relThree3);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(value = IndexOrder.class)
    void shouldFindRelationshipsByTypeInTx(IndexOrder order) throws KernelException {
        final var one = Values.intValue(1);
        final var two = Values.intValue(2);

        long inStore;
        long inStore2;
        long deletedInTx;
        long createdInTx;
        long createdInTx2;

        int propKey;
        try (KernelTransaction tx = beginTransaction()) {
            propKey = tx.tokenWrite().propertyKeyGetOrCreateForName("prop");

            inStore = createRelationship(tx.dataWrite(), typeOne);
            createRelationship(tx.dataWrite(), typeTwo);
            deletedInTx = createRelationship(tx.dataWrite(), typeOne);
            inStore2 = createRelationship(tx.dataWrite(), typeOne);
            tx.dataWrite().relationshipSetProperty(inStore2, propKey, one);
            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            final var assertOrder = isNodeBased(tx) ? IndexOrder.NONE : order;

            tx.dataWrite().relationshipDelete(deletedInTx);
            createdInTx = createRelationship(tx.dataWrite(), typeOne);

            createRelationship(tx.dataWrite(), typeTwo);

            createdInTx2 = createRelationship(tx.dataWrite(), typeOne);
            tx.dataWrite().relationshipSetProperty(createdInTx2, propKey, two);

            final var expectedReads = Lists.mutable.of(
                    new RelPropRead(inStore, null),
                    new RelPropRead(inStore2, one),
                    new RelPropRead(createdInTx, null),
                    new RelPropRead(createdInTx2, two));
            expectedReads.sortThis();
            if (assertOrder == IndexOrder.DESCENDING) {
                Collections.reverse(expectedReads);
            }

            try (RelationshipTypeIndexCursor relCursor =
                            tx.cursors().allocateRelationshipTypeIndexCursor(NULL_CONTEXT);
                    var propCursor = tx.cursors().allocatePropertyCursor(NULL_CONTEXT, EmptyMemoryTracker.INSTANCE)) {
                relationshipTypeScan(tx, typeOne, relCursor, order);

                final var actualReads = Lists.mutable.<RelPropRead>empty();
                for (final var ignored : expectedReads) {
                    assertThat(relCursor.next()).isTrue();
                    assertThat(relCursor.readFromStore()).isTrue();

                    final var reference = relCursor.reference();

                    Value propValue = null;
                    relCursor.properties(propCursor);
                    if (propCursor.next()) {
                        propValue = propCursor.propertyValue();
                        assertThat(propCursor.next()).isFalse();
                    }

                    actualReads.add(new RelPropRead(reference, propValue));
                }

                assertThat(relCursor.next()).isFalse();

                switch (assertOrder) {
                    case ASCENDING, DESCENDING -> assertThat(actualReads).containsExactlyElementsOf(expectedReads);
                    case NONE -> assertThat(actualReads).containsExactlyInAnyOrderElementsOf(expectedReads);
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource(value = IndexOrder.class)
    void shouldFindRelationshipDetailsByTypeAllInSameTx(IndexOrder order) throws KernelException {
        final var propValue = Values.intValue(42);
        try (var tx = beginTransaction()) {
            final var typeToken = tx.tokenWrite().relationshipTypeCreateForName("REL", false);
            final var propToken = tx.tokenWrite().propertyKeyGetOrCreateForName("prop");

            final var write = tx.dataWrite();
            write.nodeCreate(); // do a nudge passed 0-ID
            final var source = write.nodeCreate();
            final var target = write.nodeCreate();
            final var rel = write.relationshipCreate(source, typeToken, target);

            write.relationshipSetProperty(rel, propToken, propValue);

            try (var relCursor = tx.cursors().allocateRelationshipTypeIndexCursor(NULL_CONTEXT);
                    var propCursor = tx.cursors().allocatePropertyCursor(NULL_CONTEXT, EmptyMemoryTracker.INSTANCE)) {
                relationshipTypeScan(tx, typeToken, relCursor, order);
                assertThat(relCursor.next()).isTrue();
                assertThat(relCursor.readFromStore()).isTrue();
                assertThat(rel).isEqualTo(relCursor.reference());
                assertThat(source).isEqualTo(relCursor.sourceNodeReference());
                assertThat(target).isEqualTo(relCursor.targetNodeReference());

                relCursor.properties(propCursor);
                assertThat(propCursor.next()).isTrue();
                assertThat(propCursor.propertyKey()).isEqualTo(propToken);
                assertThat(propCursor.propertyValue()).isEqualTo(propValue);
                assertThat(propCursor.next()).isFalse();

                assertThat(relCursor.next()).isFalse();
            } finally {
                tx.rollback();
            }
        }
    }

    @Test
    void shouldTraceRelationshipTypeScanEvents() throws KernelException {
        long rel1;
        final var typeTwos = new long[2];
        try (KernelTransaction tx = beginTransaction()) {
            final var write = tx.dataWrite();
            write.nodeCreate(); // nudge passed 0
            rel1 = createRelationship(write, typeOne);
            typeTwos[0] = createRelationship(write, typeTwo);
            typeTwos[1] = createRelationship(write, typeTwo);
            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            try (RelationshipTypeIndexCursor cursor = tx.cursors().allocateRelationshipTypeIndexCursor(NULL_CONTEXT)) {
                TestKernelReadTracer tracer = new TestKernelReadTracer();
                cursor.setTracer(tracer);

                // when
                relationshipTypeScan(tx, typeOne, cursor, IndexOrder.NONE);
                exhaustCursor(cursor);

                // then
                tracer.assertEvents(
                        TestKernelReadTracer.relationshipTypeScanEvent(typeOne),
                        TestKernelReadTracer.relationshipEvent(rel1));

                // when
                relationshipTypeScan(tx, typeTwo, cursor, IndexOrder.NONE);
                final var tracedRelationships = exhaustCursor(cursor);
                assertThat(tracedRelationships).containsExactlyInAnyOrder(typeTwos);

                // then
                tracer.assertEvents(
                        TestKernelReadTracer.relationshipTypeScanEvent(typeTwo),
                        TestKernelReadTracer.relationshipEvent(tracedRelationships[0]),
                        TestKernelReadTracer.relationshipEvent(tracedRelationships[1]));
            }
        }
    }

    @Test
    void shouldBeAbleToReadNodeCursorData() throws Exception {
        final long sourceNode, targetNode;
        final int label1, label2;
        try (var tx = beginTransaction()) {
            label1 = tx.tokenWrite().labelGetOrCreateForName("L1");
            label2 = tx.tokenWrite().labelGetOrCreateForName("L2");

            final var write = tx.dataWrite();
            sourceNode = write.nodeCreate();
            write.nodeAddLabel(sourceNode, label1);

            targetNode = write.nodeCreate();
            write.nodeAddLabel(targetNode, label2);

            write.relationshipCreate(sourceNode, typeOne, targetNode);
            tx.commit();
        }

        final var actualReads = new ArrayList<NodeRead>();
        final long txSource, txTarget;
        try (var tx = beginTransaction()) {
            try (var relCursor = tx.cursors().allocateRelationshipTypeIndexCursor(NULL_CONTEXT);
                    var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT)) {
                final var write = tx.dataWrite();

                txSource = write.nodeCreate();
                write.nodeAddLabel(txSource, label2);

                txTarget = write.nodeCreate();
                write.nodeAddLabel(txTarget, label1);

                write.relationshipCreate(txSource, typeOne, txTarget);

                relationshipTypeScan(tx, typeOne, relCursor, IndexOrder.NONE);

                while (relCursor.next() && relCursor.readFromStore()) {
                    relCursor.source(nodeCursor);
                    assertThat(nodeCursor.next()).isTrue();
                    actualReads.add(new NodeRead(
                            nodeCursor.nodeReference(),
                            true,
                            nodeCursor.hasLabel(label1),
                            nodeCursor.hasLabel(label2)));
                    assertThat(nodeCursor.next()).isFalse();

                    relCursor.target(nodeCursor);
                    assertThat(nodeCursor.next()).isTrue();
                    actualReads.add(new NodeRead(
                            nodeCursor.nodeReference(),
                            false,
                            nodeCursor.hasLabel(label1),
                            nodeCursor.hasLabel(label2)));
                    assertThat(nodeCursor.next()).isFalse();
                }
            }

            tx.rollback();
        }

        final var expectedReads = List.of(
                new NodeRead(sourceNode, true, true, false),
                new NodeRead(txSource, true, false, true),
                new NodeRead(targetNode, false, false, true),
                new NodeRead(txTarget, false, true, false));
        assertThat(expectedReads).containsExactlyInAnyOrderElementsOf(actualReads);
    }

    @Test
    void shouldReadMultipleRelationshipsBetweenSameNodes() throws Exception {
        long sourceNode, targetNode;
        long rel1, rel2, rel3;
        try (var tx = beginTransaction()) {
            final var prop = tx.tokenWrite().propertyKeyGetOrCreateForName("prop");
            final var write = tx.dataWrite();
            sourceNode = write.nodeCreate();
            targetNode = write.nodeCreate();
            rel1 = write.relationshipCreate(sourceNode, typeOne, targetNode);
            rel2 = write.relationshipCreate(sourceNode, typeOne, targetNode);
            rel3 = write.relationshipCreate(sourceNode, typeOne, targetNode);
            write.relationshipSetProperty(rel3, prop, Values.intValue(3));
            tx.commit();
        }

        final var expectedReads = List.of(
                new RelPropRead(rel1, null), new RelPropRead(rel2, null), new RelPropRead(rel3, Values.intValue(3)));

        try (var tx = beginTransaction()) {
            try (var relCursor = tx.cursors().allocateRelationshipTypeIndexCursor(NULL_CONTEXT);
                    var propCursor = tx.cursors().allocatePropertyCursor(NULL_CONTEXT, EmptyMemoryTracker.INSTANCE)) {
                relationshipTypeScan(tx, typeOne, relCursor, IndexOrder.NONE);

                final var actualReads = new ArrayList<RelPropRead>();
                while (relCursor.next() && relCursor.readFromStore()) {
                    final var reference = relCursor.reference();

                    Value propValue = null;
                    relCursor.properties(propCursor);
                    if (propCursor.next()) {
                        propValue = propCursor.propertyValue();
                        assertThat(propCursor.next()).isFalse();
                    }

                    actualReads.add(new RelPropRead(reference, propValue));
                }

                assertThat(expectedReads).containsExactlyInAnyOrderElementsOf(actualReads);
            } finally {
                tx.rollback();
            }
        }
    }

    @Test
    void shouldReadRelationshipOnReadFromStore() throws Exception {
        // given
        long first;
        long second;
        long third;
        try (var tx = beginTransaction()) {
            first = createRelationship(tx.dataWrite(), typeOne);
            second = createRelationship(tx.dataWrite(), typeOne);
            third = createRelationship(tx.dataWrite(), typeOne);
            tx.commit();
        }

        final var notDeleted = LongSets.immutable.of(second, third);

        try (var tx = beginTransaction();
                var cursor = tx.cursors().allocateRelationshipTypeIndexCursor(NULL_CONTEXT);
                var scanCursor = tx.cursors().allocateRelationshipScanCursor(NULL_CONTEXT)) {
            var read = tx.dataRead();
            read.singleRelationship(first, scanCursor);
            assertThat(scanCursor.next()).isTrue();
            var relSourceNode = scanCursor.sourceNodeReference();
            var relTargetNode = scanCursor.targetNodeReference();

            // when
            relationshipTypeScan(tx, typeOne, cursor, IndexOrder.NONE);

            // then
            while (cursor.next()) {
                assertThat(cursor.readFromStore()).isTrue();
                assertThat(cursor.type()).isEqualTo(typeOne);

                if (cursor.relationshipReference() == first) {
                    try (var tx2 = beginTransaction()) {
                        tx2.dataWrite().relationshipDelete(first);
                        tx2.commit();
                    }

                    assertThat(cursor.sourceNodeReference()).isEqualTo(relSourceNode);
                    assertThat(cursor.targetNodeReference()).isEqualTo(relTargetNode);
                } else {
                    assertThat(notDeleted.contains(cursor.relationshipReference()))
                            .isTrue();
                }
            }
        }
    }

    @Test
    void shouldHandleReadsWhenSourceRelationshipsPresentInStoreAndTxState() throws Exception {
        // given
        final var v1 = Values.intValue(1);
        final var v2 = Values.intValue(2);
        final var v3 = Values.intValue(3);
        int prop;
        long nodeToBeDeleted;
        long nodeInStore1;
        long nodeInStore2;
        long nodeInTx;
        long relInStore;
        long relToBeDeleted;
        long relInTx1;
        long relInTx2;
        try (var tx = beginTransaction()) {
            final var tokenWrite = tx.tokenWrite();
            prop = tokenWrite.propertyKeyGetOrCreateForName("prop");

            final var write = tx.dataWrite();

            nodeToBeDeleted = write.nodeCreate();
            nodeInStore1 = write.nodeCreate();
            nodeInStore2 = write.nodeCreate();

            relToBeDeleted = write.relationshipCreate(nodeToBeDeleted, typeOne, nodeInStore2);
            relInStore = write.relationshipCreate(nodeInStore1, typeOne, nodeInStore2);

            write.relationshipSetProperty(relInStore, prop, v1);
            tx.commit();
        }

        try (var tx = beginTransaction();
                var relCursor = tx.cursors().allocateRelationshipTypeIndexCursor(NULL_CONTEXT);
                var propCursor = tx.cursors().allocatePropertyCursor(NULL_CONTEXT, EmptyMemoryTracker.INSTANCE)) {
            final var write = tx.dataWrite();

            nodeInTx = write.nodeCreate();
            // need to be outgoing relationships for this type for them to be picked up in tx state for an indexed node
            relInTx1 = write.relationshipCreate(nodeInStore1, typeOne, nodeInTx);
            relInTx2 = write.relationshipCreate(nodeInStore2, typeOne, nodeInTx);

            write.relationshipSetProperty(relInTx1, prop, v2);
            write.relationshipSetProperty(relInTx2, prop, v3);

            write.nodeDelete(nodeToBeDeleted);
            write.relationshipDelete(relToBeDeleted);

            // when
            final var expectedReads = List.of(
                    new NodeRelRead(nodeInStore1, relInStore, nodeInStore2, v1),
                    new NodeRelRead(nodeInStore1, relInTx1, nodeInTx, v2),
                    new NodeRelRead(nodeInStore2, relInTx2, nodeInTx, v3));
            final var actualReads = new ArrayList<NodeRelRead>();

            relationshipTypeScan(tx, typeOne, relCursor, IndexOrder.NONE);
            while (relCursor.next()) {
                if (relCursor.readFromStore()) {
                    relCursor.properties(propCursor);

                    assertThat(propCursor.next()).isTrue();
                    actualReads.add(new NodeRelRead(
                            relCursor.sourceNodeReference(),
                            relCursor.reference(),
                            relCursor.targetNodeReference(),
                            propCursor.propertyValue()));
                    assertThat(propCursor.next()).isFalse();
                }
            }

            assertThat(expectedReads).containsExactlyInAnyOrderElementsOf(actualReads);
        }
    }

    @Test
    void shouldNotLoadDeletedRelationshipOnReadFromStore() throws Exception {
        // given
        long first;
        long second;
        long third;
        try (var tx = beginTransaction()) {
            first = createRelationship(tx.dataWrite(), typeOne);
            second = createRelationship(tx.dataWrite(), typeOne);
            third = createRelationship(tx.dataWrite(), typeOne);
            tx.commit();
        }

        final var expectedReads = Lists.mutable.empty();
        final var actualReads = Lists.mutable.empty();

        try (var tx = beginTransaction();
                var cursor = tx.cursors().allocateRelationshipTypeIndexCursor(NULL_CONTEXT);
                var scanCursor = tx.cursors().allocateRelationshipScanCursor(NULL_CONTEXT)) {
            var read = tx.dataRead();

            if (isNodeBased(tx)) {
                read.singleRelationship(first, scanCursor);
                assertThat(scanCursor.next()).isTrue();
                expectedReads.add(
                        new RelRead(first, true, scanCursor.sourceNodeReference(), scanCursor.targetNodeReference()));
            } else {
                expectedReads.add(new RelRead(first, false, -1, -1));
            }

            read.singleRelationship(second, scanCursor);
            assertThat(scanCursor.next()).isTrue();
            expectedReads.add(
                    new RelRead(second, true, scanCursor.sourceNodeReference(), scanCursor.targetNodeReference()));

            read.singleRelationship(third, scanCursor);
            assertThat(scanCursor.next()).isTrue();
            expectedReads.add(
                    new RelRead(third, true, scanCursor.sourceNodeReference(), scanCursor.targetNodeReference()));

            // when
            relationshipTypeScan(tx, typeOne, cursor, IndexOrder.NONE);
            var doneDelete = false;
            while (cursor.next()) {
                if (!doneDelete) {
                    try (var tx2 = beginTransaction()) {
                        tx2.dataWrite().relationshipDelete(first);
                        tx2.commit();
                    }
                    doneDelete = true;
                }

                // then
                assertThat(cursor.type()).isEqualTo(typeOne);
                final var relId = cursor.relationshipReference();
                // do not run this on node based relationship index
                if (relId == first && !isNodeBased(tx)) {
                    actualReads.add(new RelRead(relId, cursor.readFromStore(), -1, -1));
                } else {
                    actualReads.add(new RelRead(
                            relId, cursor.readFromStore(), cursor.sourceNodeReference(), cursor.targetNodeReference()));
                }
            }
        }

        assertThat(actualReads).containsExactlyInAnyOrderElementsOf(expectedReads);
    }

    @Test
    void shouldFailOnReadRelationshipBeforeReadFromStore() throws Exception {
        // Do not run this on node based relationship index
        assumeFalse(isNodeBased());

        // given
        long rel;
        try (var tx = beginTransaction()) {
            rel = createRelationship(tx.dataWrite(), typeOne);
            tx.commit();
        }

        try (var tx = beginTransaction();
                var cursor = tx.cursors().allocateRelationshipTypeIndexCursor(NULL_CONTEXT);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var propertyCursor = tx.cursors().allocatePropertyCursor(NULL_CONTEXT, EmptyMemoryTracker.INSTANCE)) {
            // when
            relationshipTypeScan(tx, typeOne, cursor, IndexOrder.NONE);
            assertThat(cursor.next()).isTrue();

            // then these should fail
            assertThatThrownBy(() -> cursor.source(nodeCursor)).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(cursor::sourceNodeReference).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> cursor.target(nodeCursor)).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(cursor::targetNodeReference).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> cursor.properties(propertyCursor)).isInstanceOf(IllegalStateException.class);

            // although these should succeed, since it already has those from the index
            assertThat(cursor.type()).isEqualTo(typeOne);
            assertThat(cursor.relationshipReference()).isEqualTo(rel);
            assertThat(cursor.next()).isFalse();
        }
    }

    private static long[] exhaustCursor(RelationshipTypeIndexCursor cursor) {
        final var rels = LongLists.mutable.empty();
        while (cursor.next()) {
            rels.add(cursor.relationshipReference());
        }
        return rels.toArray();
    }

    private static long createRelationship(Write write, int type) throws KernelException {
        long sourceNode = write.nodeCreate();
        long targetNode = write.nodeCreate();
        return write.relationshipCreate(sourceNode, type, targetNode);
    }

    private static void relationshipTypeScan(
            KernelTransaction tx, int label, RelationshipTypeIndexCursor cursor, IndexOrder indexOrder)
            throws KernelException {
        IndexDescriptor index = tx.schemaRead().indexGetForName("rti");
        TokenReadSession tokenReadSession = tx.dataRead().tokenReadSession(index);
        tx.dataRead()
                .relationshipTypeScan(
                        tokenReadSession,
                        cursor,
                        IndexQueryConstraints.ordered(indexOrder),
                        new TokenPredicate(label),
                        tx.cursorContext());
    }

    private record NodeRead(long id, boolean isSource, boolean label1Present, boolean label2Present) {}

    private record RelRead(long id, boolean fromStore, long source, long target) {}

    private record RelPropRead(long id, Value propValue) implements Comparable<RelPropRead> {
        @Override
        public int compareTo(RelPropRead other) {
            return Long.compare(id, other.id);
        }
    }

    private record NodeRelRead(long source, long rel, long target, Value propValue) {}
}
