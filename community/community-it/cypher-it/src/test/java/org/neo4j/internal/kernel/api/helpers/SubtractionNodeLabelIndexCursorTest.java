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
package org.neo4j.internal.kernel.api.helpers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.api.KernelTransaction.Type.EXPLICIT;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenReadSession;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ImpermanentDbmsExtension
@ExtendWith(RandomExtension.class)
class SubtractionNodeLabelIndexCursorTest {
    @Inject
    private Kernel kernel;

    @Inject
    private RandomSupport random;

    enum Order {
        ASCENDING {
            @Override
            void sort(List<Long> nodesToFind) {
                nodesToFind.sort(Comparator.naturalOrder());
            }
        },
        DESCENDING {
            @Override
            void sort(List<Long> nodesToFind) {
                nodesToFind.sort(Collections.reverseOrder());
            }
        };

        abstract void sort(List<Long> nodesToFind);
    }

    @ParameterizedTest
    @EnumSource(Order.class)
    void shouldHandleEmptyResult(Order order) throws KernelException {
        // given
        int positiveLabel;
        int negativeLabel;
        int nodeCount = 100;
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            positiveLabel = tokenWrite.labelGetOrCreateForName("A");
            negativeLabel = tokenWrite.labelGetOrCreateForName("B");
            for (int i = 0; i < nodeCount; i++) {
                write.nodeCreate();
            }
            tx.commit();
        }

        // when
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var positive = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var negative = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT); ) {
            var subtractionCursor =
                    subtractionNodeLabelIndexCursor(order, tx, positiveLabel, negativeLabel, positive, negative);

            // then
            assertThat(subtractionCursor.next()).isFalse();
        }
    }

    @ParameterizedTest
    @EnumSource(Order.class)
    void shouldFindSingleNodeOnePositiveOneNegative(Order order) throws KernelException {
        // given
        int positiveLabel;
        int negativeLabel;
        long nodeToFind;

        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            positiveLabel = tokenWrite.labelGetOrCreateForName("A");
            negativeLabel = tokenWrite.labelGetOrCreateForName("B");
            nodeToFind = write.nodeCreate();
            write.nodeAddLabel(nodeToFind, positiveLabel);
            long otherNode = write.nodeCreate();
            write.nodeAddLabel(otherNode, positiveLabel);
            write.nodeAddLabel(otherNode, negativeLabel);
            long otherNode2 = write.nodeCreate();
            write.nodeAddLabel(otherNode2, positiveLabel);
            write.nodeAddLabel(otherNode2, negativeLabel);
            tx.commit();
        }

        // when
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var positive = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var negative = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT); ) {
            var subtractionCursor =
                    subtractionNodeLabelIndexCursor(order, tx, positiveLabel, negativeLabel, positive, negative);

            // then
            assertThat(subtractionCursor.next()).isTrue();
            assertThat(subtractionCursor.reference()).isEqualTo(nodeToFind);
            assertThat(subtractionCursor.next()).isFalse();
        }
    }

    @ParameterizedTest
    @EnumSource(Order.class)
    void shouldFindSingleNodeTwoPositiveOneNegative(Order order) throws KernelException {
        // given
        int positiveLabel1;
        int positiveLabel2;
        int negativeLabel;
        long nodeToFind;

        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            positiveLabel1 = tokenWrite.labelGetOrCreateForName("A1");
            positiveLabel2 = tokenWrite.labelGetOrCreateForName("A2");
            negativeLabel = tokenWrite.labelGetOrCreateForName("B");
            nodeToFind = write.nodeCreate();
            write.nodeAddLabel(nodeToFind, positiveLabel1);
            write.nodeAddLabel(nodeToFind, positiveLabel2);
            long otherNode1 = write.nodeCreate();
            write.nodeAddLabel(otherNode1, positiveLabel1);
            write.nodeAddLabel(otherNode1, positiveLabel2);
            write.nodeAddLabel(otherNode1, negativeLabel);
            long otherNode2 = write.nodeCreate();
            write.nodeAddLabel(otherNode2, positiveLabel1);
            tx.commit();
        }

        // when
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var positive1 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var positive2 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var negative = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT); ) {
            var subtractionCursor = subtractionNodeLabelIndexCursor(
                    order,
                    tx,
                    new int[] {positiveLabel1, positiveLabel2},
                    new int[] {negativeLabel},
                    new NodeLabelIndexCursor[] {positive1, positive2},
                    new NodeLabelIndexCursor[] {negative});

            // then
            assertThat(subtractionCursor.next()).isTrue();
            assertThat(subtractionCursor.reference()).isEqualTo(nodeToFind);
            assertThat(subtractionCursor.next()).isFalse();
        }
    }

    @ParameterizedTest
    @EnumSource(Order.class)
    void shouldFindSingleNodeOnePositiveTwoNegative(Order order) throws KernelException {
        // given
        int positiveLabel;
        int negativeLabel1;
        int negativeLabel2;
        long nodeToFind;

        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            positiveLabel = tokenWrite.labelGetOrCreateForName("A");
            negativeLabel1 = tokenWrite.labelGetOrCreateForName("B1");
            negativeLabel2 = tokenWrite.labelGetOrCreateForName("B2");

            long otherNode1 = write.nodeCreate();
            write.nodeAddLabel(otherNode1, positiveLabel);
            write.nodeAddLabel(otherNode1, negativeLabel1);
            write.nodeAddLabel(otherNode1, negativeLabel2);
            long otherNode2 = write.nodeCreate();
            write.nodeAddLabel(otherNode2, negativeLabel1);

            long otherNode3 = write.nodeCreate();
            write.nodeAddLabel(otherNode3, negativeLabel1);
            write.nodeAddLabel(otherNode3, negativeLabel2);

            nodeToFind = write.nodeCreate();
            write.nodeAddLabel(nodeToFind, positiveLabel);
            write.nodeAddLabel(nodeToFind, negativeLabel1);

            tx.commit();
        }

        // when
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var positive = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var negative1 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var negative2 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT); ) {
            var subtractionCursor = subtractionNodeLabelIndexCursor(
                    order,
                    tx,
                    new int[] {positiveLabel},
                    new int[] {negativeLabel1, negativeLabel2},
                    new NodeLabelIndexCursor[] {positive},
                    new NodeLabelIndexCursor[] {negative1, negative2});

            // then
            assertThat(subtractionCursor.next()).isTrue();
            assertThat(subtractionCursor.reference()).isEqualTo(nodeToFind);
            assertThat(subtractionCursor.next()).isFalse();
        }
    }

    @ParameterizedTest
    @EnumSource(Order.class)
    void shouldFindManyNodesOnePositiveOneNegative(Order order) throws KernelException {
        // given
        int positiveLabel;
        int negativeLabel;
        int nodeCount = random.nextInt(1000);
        List<Long> nodesToFind = new ArrayList<>(nodeCount);

        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            positiveLabel = tokenWrite.labelGetOrCreateForName("A");
            negativeLabel = tokenWrite.labelGetOrCreateForName("B");
            for (int i = 0; i < nodeCount; i++) {
                long node = write.nodeCreate();
                if (random.nextBoolean()) {
                    write.nodeAddLabel(node, positiveLabel);
                    if (random.nextBoolean()) {
                        write.nodeAddLabel(node, negativeLabel);
                    } else {
                        nodesToFind.add(node);
                    }
                }
            }
            tx.commit();
        }

        // when
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var positive = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var negative = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT); ) {
            var subtractionCursor =
                    subtractionNodeLabelIndexCursor(order, tx, positiveLabel, negativeLabel, positive, negative);

            order.sort(nodesToFind);

            assertThat(asList(subtractionCursor)).isEqualTo(nodesToFind);
        }
    }

    @ParameterizedTest
    @EnumSource(Order.class)
    void shouldFindManyNodesManyPositiveManyNegative(Order order) throws KernelException {
        // given
        int positiveLabel1;
        int positiveLabel2;
        int positiveLabel3;
        int negativeLabel1;
        int negativeLabel2;
        int nodeCount = random.nextInt(1000);
        List<Long> nodesToFind = new ArrayList<>(nodeCount);

        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            positiveLabel1 = tokenWrite.labelGetOrCreateForName("A1");
            positiveLabel2 = tokenWrite.labelGetOrCreateForName("A2");
            positiveLabel3 = tokenWrite.labelGetOrCreateForName("A3");
            negativeLabel1 = tokenWrite.labelGetOrCreateForName("B1");
            negativeLabel2 = tokenWrite.labelGetOrCreateForName("B2");
            for (int i = 0; i < nodeCount; i++) {
                long node = write.nodeCreate();
                switch (random.nextInt(5)) {
                    case 0 -> {
                        write.nodeAddLabel(node, positiveLabel1);
                        write.nodeAddLabel(node, positiveLabel2);
                        write.nodeAddLabel(node, positiveLabel3);
                        nodesToFind.add(node);
                    }
                    case 1 -> {
                        write.nodeAddLabel(node, positiveLabel1);
                        write.nodeAddLabel(node, positiveLabel2);
                        write.nodeAddLabel(node, positiveLabel3);
                        write.nodeAddLabel(node, negativeLabel1);
                        nodesToFind.add(node);
                    }
                    case 2 -> {
                        write.nodeAddLabel(node, positiveLabel1);
                        write.nodeAddLabel(node, positiveLabel2);
                        write.nodeAddLabel(node, positiveLabel3);
                        write.nodeAddLabel(node, negativeLabel1);
                        write.nodeAddLabel(node, negativeLabel2);
                    }
                    case 3 -> {
                        write.nodeAddLabel(node, positiveLabel1);
                        write.nodeAddLabel(node, positiveLabel3);
                        write.nodeAddLabel(node, negativeLabel1);
                    }
                    case 4 -> write.nodeAddLabel(node, positiveLabel3);
                }
            }
            tx.commit();
        }

        // when
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var positive1 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var positive2 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var positive3 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var negative1 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var negative2 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT)) {
            var subtractionCursor = subtractionNodeLabelIndexCursor(
                    order,
                    tx,
                    new int[] {positiveLabel1, positiveLabel2, positiveLabel3},
                    new int[] {negativeLabel1, negativeLabel2},
                    new NodeLabelIndexCursor[] {positive1, positive2, positive3},
                    new NodeLabelIndexCursor[] {negative1, negative2});

            order.sort(nodesToFind);

            assertThat(asList(subtractionCursor)).isEqualTo(nodesToFind);
        }
    }

    @ParameterizedTest
    @EnumSource(Order.class)
    void shouldHandleEmptyNegativeLabelCursor(Order order) throws KernelException {
        // given
        int positiveLabel;
        int negativeLabel;
        int nodeCount = 100;
        List<Long> nodesToFind = new ArrayList<>(nodeCount);

        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            positiveLabel = tokenWrite.labelGetOrCreateForName("A");
            negativeLabel = tokenWrite.labelGetOrCreateForName("B");
            for (int i = 0; i < nodeCount; i++) {
                long node = write.nodeCreate();
                write.nodeAddLabel(node, positiveLabel);
                nodesToFind.add(node);
            }
            tx.commit();
        }

        // when
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var positive = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var negative = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT); ) {
            var subtractionCursor =
                    subtractionNodeLabelIndexCursor(order, tx, positiveLabel, negativeLabel, positive, negative);

            // then
            order.sort(nodesToFind);
            assertThat(asList(subtractionCursor)).isEqualTo(nodesToFind);
        }
    }

    private SubtractionNodeLabelIndexCursor subtractionNodeLabelIndexCursor(
            Order order,
            KernelTransaction tx,
            int positiveLabel,
            int negativeLabel,
            NodeLabelIndexCursor positiveCursor,
            NodeLabelIndexCursor negativeCursor)
            throws KernelException {
        return subtractionNodeLabelIndexCursor(
                order,
                tx,
                new int[] {positiveLabel},
                new int[] {negativeLabel},
                new NodeLabelIndexCursor[] {positiveCursor},
                new NodeLabelIndexCursor[] {negativeCursor});
    }

    private SubtractionNodeLabelIndexCursor subtractionNodeLabelIndexCursor(
            Order order,
            KernelTransaction tx,
            int[] positiveLabels,
            int[] negativeLabels,
            NodeLabelIndexCursor[] positiveCursors,
            NodeLabelIndexCursor[] negativeCursors)
            throws KernelException {
        Read read = tx.dataRead();
        SchemaRead schemaRead = tx.schemaRead();
        IndexDescriptor index = schemaRead
                .index(SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR)
                .next();
        TokenReadSession tokenReadSession = read.tokenReadSession(index);
        return switch (order) {
            case ASCENDING -> SubtractionNodeLabelIndexCursor.ascendingSubtractionNodeLabelIndexCursor(
                    read,
                    tokenReadSession,
                    tx.cursorContext(),
                    positiveLabels,
                    negativeLabels,
                    positiveCursors,
                    negativeCursors);
            case DESCENDING -> SubtractionNodeLabelIndexCursor.descendingSubtractionNodeLabelIndexCursor(
                    read,
                    tokenReadSession,
                    tx.cursorContext(),
                    positiveLabels,
                    negativeLabels,
                    positiveCursors,
                    negativeCursors);
        };
    }

    private List<Long> asList(SubtractionNodeLabelIndexCursor cursor) {
        var result = new ArrayList<Long>();
        while (cursor.next()) {
            result.add(cursor.reference());
        }
        return result;
    }
}
