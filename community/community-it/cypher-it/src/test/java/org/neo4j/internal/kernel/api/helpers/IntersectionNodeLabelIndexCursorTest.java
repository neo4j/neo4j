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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.SkippableCursor;
import org.neo4j.internal.kernel.api.TokenReadSession;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
class IntersectionNodeLabelIndexCursorTest {
    @Inject
    private Kernel kernel;

    @Test
    void shouldHandleEmptyResultAscending() throws KernelException {
        // given
        int[] labelsToLookFor = new int[3];
        int nodeCount = 100;
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            tokenWrite.labelGetOrCreateForNames(new String[] {"A", "B", "C"}, labelsToLookFor);

            for (int i = 0; i < nodeCount; i++) {
                write.nodeCreate();
            }
            tx.commit();
        }

        // when
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var cursor1 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor2 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor3 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT)) {
            var cursors = new NodeLabelIndexCursor[] {cursor1, cursor2, cursor3};
            var intersectionCursor = ascendingIntersectionLabelIndexCursor(tx, labelsToLookFor, cursors);

            // then
            assertThat(intersectionCursor.next()).isFalse();
        }
    }

    @Test
    void shouldHandleEmptyResultDescending() throws KernelException {
        // given
        int[] labelsToLookFor = new int[3];
        int nodeCount = 100;
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            tokenWrite.labelGetOrCreateForNames(new String[] {"A", "B", "C"}, labelsToLookFor);

            for (int i = 0; i < nodeCount; i++) {
                write.nodeCreate();
            }
            tx.commit();
        }

        // when
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var cursor1 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor2 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor3 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT)) {
            var cursors = new NodeLabelIndexCursor[] {cursor1, cursor2, cursor3};
            var intersectionCursor = descendingIntersectionLabelIndexCursor(tx, labelsToLookFor, cursors);

            // then
            assertThat(intersectionCursor.next()).isFalse();
        }
    }

    @Test
    void shouldFindNodesAscending() throws KernelException {
        // given
        //  | n1  | n2  | n3  | n4  |
        //  | --- | --- | --- | --- |
        // A |  *  |  *  |  *  |  *  |
        // B |     |  *  |  *  |  *  |
        // C |  *  |  *  |     |  *  |
        int[] labelsToLookFor = new int[3];
        long n1;
        long n2;
        long n3;
        long n4;
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            tokenWrite.labelGetOrCreateForNames(new String[] {"A", "B", "C"}, labelsToLookFor);
            n1 = write.nodeCreateWithLabels(new int[] {labelsToLookFor[0], labelsToLookFor[2]});
            n2 = write.nodeCreateWithLabels(labelsToLookFor);
            n3 = write.nodeCreateWithLabels(new int[] {labelsToLookFor[0], labelsToLookFor[1]});
            n4 = write.nodeCreateWithLabels(labelsToLookFor);
            tx.commit();
        }

        // when
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var cursor1 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor2 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor3 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT)) {
            var cursors = new NodeLabelIndexCursor[] {cursor1, cursor2, cursor3};
            var intersectionCursor = ascendingIntersectionLabelIndexCursor(tx, labelsToLookFor, cursors);

            // then
            assertThat(intersectionCursor.next()).isTrue();
            assertThat(intersectionCursor.reference()).isEqualTo(Math.min(n2, n4));
            assertThat(intersectionCursor.next()).isTrue();
            assertThat(intersectionCursor.reference()).isEqualTo(Math.max(n2, n4));
            assertThat(intersectionCursor.next()).isFalse();
        }
    }

    @Test
    void shouldFindNodesDescending() throws KernelException {
        // given
        //  | n1  | n2  | n3  | n4  |
        //  | --- | --- | --- | --- |
        // A |  *  |  *  |  *  |  *  |
        // B |     |  *  |  *  |  *  |
        // C |  *  |  *  |     |  *  |
        int[] labelsToLookFor = new int[3];
        long n1;
        long n2;
        long n3;
        long n4;
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            tokenWrite.labelGetOrCreateForNames(new String[] {"A", "B", "C"}, labelsToLookFor);
            n1 = write.nodeCreateWithLabels(new int[] {labelsToLookFor[0], labelsToLookFor[2]});
            n2 = write.nodeCreateWithLabels(labelsToLookFor);
            n3 = write.nodeCreateWithLabels(new int[] {labelsToLookFor[0], labelsToLookFor[1]});
            n4 = write.nodeCreateWithLabels(labelsToLookFor);
            tx.commit();
        }

        // when
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var cursor1 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor2 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor3 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT)) {
            var cursors = new NodeLabelIndexCursor[] {cursor1, cursor2, cursor3};
            var intersectionCursor = descendingIntersectionLabelIndexCursor(tx, labelsToLookFor, cursors);

            // then
            assertThat(intersectionCursor.next()).isTrue();
            assertThat(intersectionCursor.reference()).isEqualTo(Math.max(n2, n4));
            assertThat(intersectionCursor.next()).isTrue();
            assertThat(intersectionCursor.reference()).isEqualTo(Math.min(n2, n4));
            assertThat(intersectionCursor.next()).isFalse();
        }
    }

    @Test
    void shouldHandleNonEmptyResultAscending() throws KernelException {
        // given
        int[] labelsToLookFor = new int[3];
        int nodeCount = 10000;
        List<Long> nodesToFind = new ArrayList<>(nodeCount);
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            tokenWrite.labelGetOrCreateForNames(new String[] {"A", "B", "C"}, labelsToLookFor);

            for (int i = 0; i < nodeCount; i++) {
                long node = write.nodeCreate();
                if (i % 5 == 0) {
                    write.nodeAddLabel(node, labelsToLookFor[0]);
                    write.nodeAddLabel(node, labelsToLookFor[1]);
                    write.nodeAddLabel(node, labelsToLookFor[2]);
                    nodesToFind.add(node);
                } else if (i % 3 == 0) {
                    write.nodeAddLabel(node, labelsToLookFor[i % labelsToLookFor.length]);
                    write.nodeAddLabel(node, labelsToLookFor[(i + 1) % labelsToLookFor.length]);
                } else if (i % 2 == 0) {
                    write.nodeAddLabel(node, labelsToLookFor[i % labelsToLookFor.length]);
                } // else no labels
            }
            tx.commit();
        }
    }

    @Test
    void shouldHandleNonEmptyResultSparseAscending() throws KernelException {
        // given
        int[] labelsToLookFor = new int[3];
        int nodeCount = 10000;
        List<Long> nodesToFind = new ArrayList<>(nodeCount);
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            tokenWrite.labelGetOrCreateForNames(new String[] {"A", "B", "C"}, labelsToLookFor);

            for (int i = 0; i < nodeCount; i++) {
                long node = write.nodeCreate();
                if (i % 700 == 0) {
                    write.nodeAddLabel(node, labelsToLookFor[0]);
                    write.nodeAddLabel(node, labelsToLookFor[1]);
                    write.nodeAddLabel(node, labelsToLookFor[2]);
                    nodesToFind.add(node);
                } else if (i % 3000 == 0) {
                    write.nodeAddLabel(node, labelsToLookFor[i % labelsToLookFor.length]);
                    write.nodeAddLabel(node, labelsToLookFor[(i + 1) % labelsToLookFor.length]);
                } else if (i % 2000 == 0) {
                    write.nodeAddLabel(node, labelsToLookFor[i % labelsToLookFor.length]);
                } // else no labels
            }
            tx.commit();
        }

        // when
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var cursor1 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor2 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor3 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT)) {
            var cursors = new NodeLabelIndexCursor[] {cursor1, cursor2, cursor3};
            var intersectionCursor = ascendingIntersectionLabelIndexCursor(tx, labelsToLookFor, cursors);

            // then
            assertThat(asList(intersectionCursor)).isEqualTo(nodesToFind);
        }
    }

    @Test
    void shouldHandleNonEmptyResultSomeSparseSomeDenseAscending() throws KernelException {
        // given
        int[] labelsToLookFor = new int[2];
        int nodeCount = 10000;
        List<Long> nodesToFind = new ArrayList<>(nodeCount);
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            tokenWrite.labelGetOrCreateForNames(new String[] {"A", "B"}, labelsToLookFor);

            for (int i = 0; i < nodeCount; i++) {
                long node = write.nodeCreate();
                // Fun with primes!
                if (i % 711 == 0) {
                    write.nodeAddLabel(node, labelsToLookFor[0]);
                    write.nodeAddLabel(node, labelsToLookFor[1]);
                    nodesToFind.add(node);
                } else if (i % 2 == 0) {
                    write.nodeAddLabel(node, labelsToLookFor[1]);
                } // else no labels
            }
            tx.commit();
        }

        // when
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var cursor1 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor2 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT)) {
            var cursors = new NodeLabelIndexCursor[] {cursor1, cursor2};
            var intersectionCursor = ascendingIntersectionLabelIndexCursor(tx, labelsToLookFor, cursors);

            // then
            assertThat(asList(intersectionCursor)).isEqualTo(nodesToFind);
        }
    }

    @Test
    void shouldHandleNonEmptyResultDescending() throws KernelException {
        // given
        int[] labelsToLookFor = new int[3];
        int nodeCount = 10000;
        List<Long> nodesToFind = new ArrayList<>(nodeCount);
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            tokenWrite.labelGetOrCreateForNames(new String[] {"A", "B", "C"}, labelsToLookFor);

            for (int i = 0; i < nodeCount; i++) {
                long node = write.nodeCreate();
                if (i % 5 == 0) {
                    write.nodeAddLabel(node, labelsToLookFor[0]);
                    write.nodeAddLabel(node, labelsToLookFor[1]);
                    write.nodeAddLabel(node, labelsToLookFor[2]);
                    nodesToFind.add(node);
                } else if (i % 3 == 0) {
                    write.nodeAddLabel(node, labelsToLookFor[i % labelsToLookFor.length]);
                    write.nodeAddLabel(node, labelsToLookFor[(i + 1) % labelsToLookFor.length]);
                } else if (i % 2 == 0) {
                    write.nodeAddLabel(node, labelsToLookFor[i % labelsToLookFor.length]);
                } // else no labels
            }
            tx.commit();
        }

        // when
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var cursor1 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor2 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor3 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT)) {
            var cursors = new NodeLabelIndexCursor[] {cursor1, cursor2, cursor3};
            var intersectionCursor = descendingIntersectionLabelIndexCursor(tx, labelsToLookFor, cursors);

            // then
            nodesToFind.sort(Collections.reverseOrder());
            assertThat(asList(intersectionCursor)).isEqualTo(nodesToFind);
        }
    }

    @Test
    void shouldHandlePartiallyOverlappingNodesAscending() throws KernelException {
        // given
        int[] labelsToLookFor = new int[5];
        int nodeCount = 10000;
        List<Long> nodesToFind = new ArrayList<>(nodeCount);
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            tokenWrite.labelGetOrCreateForNames(new String[] {"A", "B", "C", "D", "E"}, labelsToLookFor);

            for (int i = 0; i < nodeCount; i++) {
                var node = write.nodeCreate();
                if (i % 13 == 0) {
                    for (int k : labelsToLookFor) {
                        write.nodeAddLabel(node, k);
                    }
                    nodesToFind.add(node);
                } else if (i % 11 == 0) {
                    for (int j = 0; j < labelsToLookFor.length - 1; j++) {
                        write.nodeAddLabel(node, labelsToLookFor[(i + j) % labelsToLookFor.length]);
                    }
                } else if (i % 7 == 0) {
                    for (int j = 0; j < labelsToLookFor.length - 2; j++) {
                        write.nodeAddLabel(node, labelsToLookFor[(i + j) % labelsToLookFor.length]);
                    }
                } else if (i % 5 == 0) {
                    for (int j = 0; j < labelsToLookFor.length - 3; j++) {
                        write.nodeAddLabel(node, labelsToLookFor[(i + j) % labelsToLookFor.length]);
                    }
                } else if (i % 3 == 0) {
                    write.nodeAddLabel(node, labelsToLookFor[i % labelsToLookFor.length]);
                }
            }
            tx.commit();
        }

        // when
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var cursor1 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor2 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor3 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor4 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor5 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT)) {
            var cursors = new NodeLabelIndexCursor[] {cursor1, cursor2, cursor3, cursor4, cursor5};
            var unionCursor = ascendingIntersectionLabelIndexCursor(tx, labelsToLookFor, cursors);

            // then
            assertThat(asList(unionCursor)).isEqualTo(nodesToFind);
        }
    }

    @Test
    void shouldHandlePartiallyOverlappingNodesDescending() throws KernelException {
        // given
        int[] labelsToLookFor = new int[5];
        int nodeCount = 10000;
        List<Long> nodesToFind = new ArrayList<>(nodeCount);
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            tokenWrite.labelGetOrCreateForNames(new String[] {"A", "B", "C", "D", "E"}, labelsToLookFor);

            for (int i = 0; i < nodeCount; i++) {
                var node = write.nodeCreate();
                if (i % 13 == 0) {
                    for (int k : labelsToLookFor) {
                        write.nodeAddLabel(node, k);
                    }
                    nodesToFind.add(node);
                } else if (i % 11 == 0) {
                    for (int j = 0; j < labelsToLookFor.length - 1; j++) {
                        write.nodeAddLabel(node, labelsToLookFor[(i + j) % labelsToLookFor.length]);
                    }
                } else if (i % 7 == 0) {
                    for (int j = 0; j < labelsToLookFor.length - 2; j++) {
                        write.nodeAddLabel(node, labelsToLookFor[(i + j) % labelsToLookFor.length]);
                    }
                } else if (i % 5 == 0) {
                    for (int j = 0; j < labelsToLookFor.length - 3; j++) {
                        write.nodeAddLabel(node, labelsToLookFor[(i + j) % labelsToLookFor.length]);
                    }
                } else if (i % 3 == 0) {
                    write.nodeAddLabel(node, labelsToLookFor[i % labelsToLookFor.length]);
                }
            }
            tx.commit();
        }

        // when
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var cursor1 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor2 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor3 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor4 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor5 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT)) {
            var cursors = new NodeLabelIndexCursor[] {cursor1, cursor2, cursor3, cursor4, cursor5};
            var unionCursor = descendingIntersectionLabelIndexCursor(tx, labelsToLookFor, cursors);

            // then
            nodesToFind.sort(Collections.reverseOrder());
            assertThat(asList(unionCursor)).isEqualTo(nodesToFind);
        }
    }

    @Test
    void shouldFindIntersectionOfUnionAscending() throws KernelException {
        // given
        //   | n1  | n2  | n3  | n4  | n5  | n6  | n7  | n8  | n9  | n10 | n11 | n12 | n14 | n14 |
        //   | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
        // A |  *  |  *  |  *  |  *  |     |  *  |  *  |     |  *  |     |  *  |     |     |     |
        // B |  *  |  *  |  *  |     |  *  |  *  |     |     |     |  *  |     |  *  |     |     |
        // C |  *  |  *  |     |  *  |  *  |     |     |  *  |     |  *  |     |     |  *  |     |
        // D |  *  |     |  *  |  *  |  *  |     |  *  |  *  |  *  |     |     |     |     |  *  |
        int labelA;
        int labelB;
        int labelC;
        int labelD;
        long n1;
        long n2;
        long n3;
        long n4;
        long n5;
        long n6;
        long n7;
        long n8;
        long n9;
        long n10;
        long n11;
        long n12;
        long n13;
        long n14;
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            labelA = tokenWrite.labelGetOrCreateForName("A");
            labelB = tokenWrite.labelGetOrCreateForName("B");
            labelC = tokenWrite.labelGetOrCreateForName("C");
            labelD = tokenWrite.labelGetOrCreateForName("D");
            n1 = write.nodeCreateWithLabels(new int[] {labelA, labelB, labelC, labelD});
            n2 = write.nodeCreateWithLabels(new int[] {labelA, labelB, labelC});
            n3 = write.nodeCreateWithLabels(new int[] {labelA, labelB, labelD});
            n4 = write.nodeCreateWithLabels(new int[] {labelA, labelC, labelD});
            n5 = write.nodeCreateWithLabels(new int[] {labelB, labelC, labelD});
            n6 = write.nodeCreateWithLabels(new int[] {labelA, labelB});
            n7 = write.nodeCreateWithLabels(new int[] {labelA, labelD});
            n8 = write.nodeCreateWithLabels(new int[] {labelC, labelD});
            n9 = write.nodeCreateWithLabels(new int[] {labelA, labelD});
            n10 = write.nodeCreateWithLabels(new int[] {labelB, labelC});
            n11 = write.nodeCreateWithLabels(new int[] {labelA});
            n12 = write.nodeCreateWithLabels(new int[] {labelB});
            n13 = write.nodeCreateWithLabels(new int[] {labelC});
            n14 = write.nodeCreateWithLabels(new int[] {labelD});
            tx.commit();
        }

        // when
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var cursor1 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor2 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor3 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor4 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT)) {
            var unionAB = UnionNodeLabelIndexCursor.ascendingUnionNodeLabelIndexCursor(
                    tx.dataRead(),
                    tokenReadSession(tx),
                    tx.cursorContext(),
                    new int[] {labelA, labelB},
                    new NodeLabelIndexCursor[] {cursor1, cursor2});
            var unionCD = UnionNodeLabelIndexCursor.ascendingUnionNodeLabelIndexCursor(
                    tx.dataRead(),
                    tokenReadSession(tx),
                    tx.cursorContext(),
                    new int[] {labelC, labelD},
                    new NodeLabelIndexCursor[] {cursor3, cursor4});
            var intersectionCursor = IntersectionNodeLabelIndexCursor.ascendingIntersectionNodeLabelIndexCursor(
                    new SkippableCursor[] {unionAB, unionCD});

            // then
            var expected = Arrays.asList(n1, n2, n3, n4, n5, n7, n9, n10);
            expected.sort(Comparator.naturalOrder());
            assertThat(asList(intersectionCursor)).isEqualTo(expected);
        }
    }

    @Test
    void shouldFindIntersectionOfUnionDescending() throws KernelException {
        // given
        //   | n1  | n2  | n3  | n4  | n5  | n6  | n7  | n8  | n9  | n10 | n11 | n12 | n14 | n14 |
        //   | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
        // A |  *  |  *  |  *  |  *  |     |  *  |  *  |     |  *  |     |  *  |     |     |     |
        // B |  *  |  *  |  *  |     |  *  |  *  |     |     |     |  *  |     |  *  |     |     |
        // C |  *  |  *  |     |  *  |  *  |     |     |  *  |     |  *  |     |     |  *  |     |
        // D |  *  |     |  *  |  *  |  *  |     |  *  |  *  |  *  |     |     |     |     |  *  |
        int labelA;
        int labelB;
        int labelC;
        int labelD;
        long n1;
        long n2;
        long n3;
        long n4;
        long n5;
        long n6;
        long n7;
        long n8;
        long n9;
        long n10;
        long n11;
        long n12;
        long n13;
        long n14;
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            labelA = tokenWrite.labelGetOrCreateForName("A");
            labelB = tokenWrite.labelGetOrCreateForName("B");
            labelC = tokenWrite.labelGetOrCreateForName("C");
            labelD = tokenWrite.labelGetOrCreateForName("D");
            n1 = write.nodeCreateWithLabels(new int[] {labelA, labelB, labelC, labelD});
            n2 = write.nodeCreateWithLabels(new int[] {labelA, labelB, labelC});
            n3 = write.nodeCreateWithLabels(new int[] {labelA, labelB, labelD});
            n4 = write.nodeCreateWithLabels(new int[] {labelA, labelC, labelD});
            n5 = write.nodeCreateWithLabels(new int[] {labelB, labelC, labelD});
            n6 = write.nodeCreateWithLabels(new int[] {labelA, labelB});
            n7 = write.nodeCreateWithLabels(new int[] {labelA, labelD});
            n8 = write.nodeCreateWithLabels(new int[] {labelC, labelD});
            n9 = write.nodeCreateWithLabels(new int[] {labelA, labelD});
            n10 = write.nodeCreateWithLabels(new int[] {labelB, labelC});
            n11 = write.nodeCreateWithLabels(new int[] {labelA});
            n12 = write.nodeCreateWithLabels(new int[] {labelB});
            n13 = write.nodeCreateWithLabels(new int[] {labelC});
            n14 = write.nodeCreateWithLabels(new int[] {labelD});
            tx.commit();
        }

        // when
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var cursor1 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor2 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor3 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor4 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT)) {
            var unionAB = UnionNodeLabelIndexCursor.descendingUnionNodeLabelIndexCursor(
                    tx.dataRead(),
                    tokenReadSession(tx),
                    tx.cursorContext(),
                    new int[] {labelA, labelB},
                    new NodeLabelIndexCursor[] {cursor1, cursor2});
            var unionCD = UnionNodeLabelIndexCursor.descendingUnionNodeLabelIndexCursor(
                    tx.dataRead(),
                    tokenReadSession(tx),
                    tx.cursorContext(),
                    new int[] {labelC, labelD},
                    new NodeLabelIndexCursor[] {cursor3, cursor4});
            var intersectionCursor = IntersectionNodeLabelIndexCursor.descendingIntersectionNodeLabelIndexCursor(
                    new SkippableCursor[] {unionAB, unionCD});

            // then
            var expected = Arrays.asList(n1, n2, n3, n4, n5, n7, n9, n10);
            expected.sort(Comparator.reverseOrder());
            assertThat(asList(intersectionCursor)).isEqualTo(expected);
        }
    }

    private IntersectionNodeLabelIndexCursor ascendingIntersectionLabelIndexCursor(
            KernelTransaction tx, int[] labelsToLookFor, NodeLabelIndexCursor[] cursors) throws KernelException {
        Read read = tx.dataRead();
        return IntersectionNodeLabelIndexCursor.ascendingIntersectionNodeLabelIndexCursor(
                read, tokenReadSession(tx), tx.cursorContext(), labelsToLookFor, cursors);
    }

    private IntersectionNodeLabelIndexCursor descendingIntersectionLabelIndexCursor(
            KernelTransaction tx, int[] labelsToLookFor, NodeLabelIndexCursor[] cursors) throws KernelException {
        Read read = tx.dataRead();
        return IntersectionNodeLabelIndexCursor.descendingIntersectionNodeLabelIndexCursor(
                read, tokenReadSession(tx), tx.cursorContext(), labelsToLookFor, cursors);
    }

    private TokenReadSession tokenReadSession(KernelTransaction tx) throws IndexNotFoundKernelException {
        return tx.dataRead().tokenReadSession(indexDescriptor(tx.schemaRead()));
    }

    private IndexDescriptor indexDescriptor(SchemaRead schemaRead) {
        return schemaRead
                .index(SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR)
                .next();
    }

    private List<Long> asList(IntersectionNodeLabelIndexCursor cursor) {
        var result = new ArrayList<Long>();
        while (cursor.next()) {
            result.add(cursor.reference());
        }
        return result;
    }
}
