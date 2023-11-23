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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
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
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
class UnionNodeLabelIndexCursorTest {
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
            var unionCursor = ascendingUnionNodeLabelIndexCursor(tx, labelsToLookFor, cursors);

            // then
            assertThat(unionCursor.next()).isFalse();
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
            var unionCursor = descendingUnionNodeLabelIndexCursor(tx, labelsToLookFor, cursors);

            // then
            assertThat(unionCursor.next()).isFalse();
        }
    }

    @Test
    void shouldHandlePartiallyEmptyResultAscending() throws KernelException {
        // given
        int[] labelsToLookFor = new int[3];
        int nodeCount = 100;
        List<Long> nodesToFind = new ArrayList<>(nodeCount);
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            tokenWrite.labelGetOrCreateForNames(new String[] {"A", "B", "C"}, labelsToLookFor);

            for (int i = 0; i < nodeCount; i++) {
                long node = write.nodeCreate();
                write.nodeAddLabel(node, labelsToLookFor[1]);
                nodesToFind.add(node);
            }
            tx.commit();
        }

        // when
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var cursor1 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor2 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor3 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT)) {
            var cursors = new NodeLabelIndexCursor[] {cursor1, cursor2, cursor3};
            var unionCursor = ascendingUnionNodeLabelIndexCursor(tx, labelsToLookFor, cursors);

            // then
            assertThat(asList(unionCursor)).isEqualTo(nodesToFind);
        }
    }

    @Test
    void shouldHandlePartiallyEmptyResultDescending() throws KernelException {
        // given
        int[] labelsToLookFor = new int[3];
        int nodeCount = 100;
        List<Long> nodesToFind = new ArrayList<>(nodeCount);
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            tokenWrite.labelGetOrCreateForNames(new String[] {"A", "B", "C"}, labelsToLookFor);

            for (int i = 0; i < nodeCount; i++) {
                long node = write.nodeCreate();
                write.nodeAddLabel(node, labelsToLookFor[1]);
                nodesToFind.add(node);
            }
            tx.commit();
        }

        // when
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var cursor1 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor2 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor3 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT)) {
            var cursors = new NodeLabelIndexCursor[] {cursor1, cursor2, cursor3};
            var unionCursor = descendingUnionNodeLabelIndexCursor(tx, labelsToLookFor, cursors);

            // then
            nodesToFind.sort(Collections.reverseOrder());
            assertThat(asList(unionCursor)).isEqualTo(nodesToFind);
        }
    }

    @Test
    void shouldHandledNodesWithNoOverlapAscending() throws KernelException {
        // given
        int[] labelsToLookFor = new int[3];
        int nodeCount = 100;
        List<Long> nodesToFind = new ArrayList<>(nodeCount);
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            tokenWrite.labelGetOrCreateForNames(new String[] {"A", "B", "C"}, labelsToLookFor);
            int notLookingFor = tx.tokenWrite().labelGetOrCreateForName("D");

            for (int i = 0; i < nodeCount; i++) {
                long nodeToFind = write.nodeCreate();
                nodesToFind.add(nodeToFind);
                long nodeNotToFind = write.nodeCreate();
                write.nodeAddLabel(nodeToFind, labelsToLookFor[i % 3]);
                write.nodeAddLabel(nodeToFind, notLookingFor);
                write.nodeAddLabel(nodeNotToFind, notLookingFor);
            }
            tx.commit();
        }

        // when
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var cursor1 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor2 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor3 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT)) {
            var cursors = new NodeLabelIndexCursor[] {cursor1, cursor2, cursor3};
            var unionCursor = ascendingUnionNodeLabelIndexCursor(tx, labelsToLookFor, cursors);

            // then
            assertThat(asList(unionCursor)).isEqualTo(nodesToFind);
        }
    }

    @Test
    void shouldHandledNodesWithNoOverlapDescending() throws KernelException {
        // given
        int[] labelsToLookFor = new int[3];
        int nodeCount = 100;
        List<Long> nodesToFind = new ArrayList<>(nodeCount);
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            tokenWrite.labelGetOrCreateForNames(new String[] {"A", "B", "C"}, labelsToLookFor);
            int notLookingFor = tx.tokenWrite().labelGetOrCreateForName("D");

            for (int i = 0; i < nodeCount; i++) {
                long nodeToFind = write.nodeCreate();
                nodesToFind.add(nodeToFind);
                long nodeNotToFind = write.nodeCreate();
                write.nodeAddLabel(nodeToFind, labelsToLookFor[i % 3]);
                write.nodeAddLabel(nodeToFind, notLookingFor);
                write.nodeAddLabel(nodeNotToFind, notLookingFor);
            }
            tx.commit();
        }

        // when
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var cursor1 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor2 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor3 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT)) {
            var cursors = new NodeLabelIndexCursor[] {cursor1, cursor2, cursor3};
            var unionCursor = descendingUnionNodeLabelIndexCursor(tx, labelsToLookFor, cursors);

            // then
            nodesToFind.sort(Collections.reverseOrder());
            assertThat(asList(unionCursor)).isEqualTo(nodesToFind);
        }
    }

    @Test
    void shouldHandleNodesWithFullOverlapAscending() throws KernelException {
        // given
        int[] labelsToLookFor = new int[3];
        int nodeCount = 100;
        List<Long> nodesToFind = new ArrayList<>(nodeCount);
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            tokenWrite.labelGetOrCreateForNames(new String[] {"A", "B", "C"}, labelsToLookFor);
            int notLookingFor = tx.tokenWrite().labelGetOrCreateForName("D");

            for (int i = 0; i < nodeCount; i++) {
                long nodeToFind = write.nodeCreateWithLabels(labelsToLookFor);
                long nodeNotToFind = write.nodeCreate();
                nodesToFind.add(nodeToFind);
                write.nodeAddLabel(nodeNotToFind, notLookingFor);
            }
            tx.commit();
        }

        // when
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var cursor1 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor2 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor3 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT)) {
            var cursors = new NodeLabelIndexCursor[] {cursor1, cursor2, cursor3};
            var unionCursor = ascendingUnionNodeLabelIndexCursor(tx, labelsToLookFor, cursors);

            // then
            assertThat(asList(unionCursor)).isEqualTo(nodesToFind);
        }
    }

    @Test
    void shouldHandleNodesWithFullOverlapDescending() throws KernelException {
        // given
        int[] labelsToLookFor = new int[3];
        int nodeCount = 100;
        List<Long> nodesToFind = new ArrayList<>(nodeCount);
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            tokenWrite.labelGetOrCreateForNames(new String[] {"A", "B", "C"}, labelsToLookFor);
            int notLookingFor = tx.tokenWrite().labelGetOrCreateForName("D");

            for (int i = 0; i < nodeCount; i++) {
                long nodeToFind = write.nodeCreateWithLabels(labelsToLookFor);
                long nodeNotToFind = write.nodeCreate();
                nodesToFind.add(nodeToFind);
                write.nodeAddLabel(nodeNotToFind, notLookingFor);
            }
            tx.commit();
        }

        // when
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var cursor1 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor2 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
                var cursor3 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT)) {
            var cursors = new NodeLabelIndexCursor[] {cursor1, cursor2, cursor3};
            var unionCursor = descendingUnionNodeLabelIndexCursor(tx, labelsToLookFor, cursors);

            // then
            nodesToFind.sort(Comparator.reverseOrder());
            assertThat(asList(unionCursor)).isEqualTo(nodesToFind);
        }
    }

    @Test
    void shouldHandlePartiallyOverlappingNodesAscending() throws KernelException {
        // given
        int[] labelsToLookFor = new int[5];
        int nodeCount = 1000;
        List<Long> nodesToFind = new ArrayList<>(nodeCount);
        Set<Long> seen = new HashSet<>();
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            tokenWrite.labelGetOrCreateForNames(new String[] {"A", "B", "C", "D", "E"}, labelsToLookFor);

            for (int i = 0; i < nodeCount; i++) {
                var node = write.nodeCreate();
                if (i % 13 == 0) {
                    write.nodeAddLabel(node, labelsToLookFor[0]);
                    if (seen.add(node)) {
                        nodesToFind.add(node);
                    }
                }
                if (i % 11 == 0) {
                    write.nodeAddLabel(node, labelsToLookFor[1]);
                    if (seen.add(node)) {
                        nodesToFind.add(node);
                    }
                }
                if (i % 7 == 0) {
                    write.nodeAddLabel(node, labelsToLookFor[2]);
                    if (seen.add(node)) {
                        nodesToFind.add(node);
                    }
                }
                if (i % 5 == 0) {
                    write.nodeAddLabel(node, labelsToLookFor[3]);
                    if (seen.add(node)) {
                        nodesToFind.add(node);
                    }
                }
                if (i % 3 == 0) {
                    write.nodeAddLabel(node, labelsToLookFor[3]);
                    if (seen.add(node)) {
                        nodesToFind.add(node);
                    }
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
            var unionCursor = ascendingUnionNodeLabelIndexCursor(tx, labelsToLookFor, cursors);

            // then
            assertThat(asList(unionCursor)).isEqualTo(nodesToFind);
        }
    }

    @Test
    void shouldHandlePartiallyOverlappingNodesDescending() throws KernelException {
        // given
        int[] labelsToLookFor = new int[5];
        int nodeCount = 1000;
        List<Long> nodesToFind = new ArrayList<>(nodeCount);
        Set<Long> seen = new HashSet<>();
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            tokenWrite.labelGetOrCreateForNames(new String[] {"A", "B", "C", "D", "E"}, labelsToLookFor);

            for (int i = 0; i < nodeCount; i++) {
                var node = write.nodeCreate();
                if (i % 13 == 0) {
                    write.nodeAddLabel(node, labelsToLookFor[0]);
                    if (seen.add(node)) {
                        nodesToFind.add(node);
                    }
                }
                if (i % 11 == 0) {
                    write.nodeAddLabel(node, labelsToLookFor[1]);
                    if (seen.add(node)) {
                        nodesToFind.add(node);
                    }
                }
                if (i % 7 == 0) {
                    write.nodeAddLabel(node, labelsToLookFor[2]);
                    if (seen.add(node)) {
                        nodesToFind.add(node);
                    }
                }
                if (i % 5 == 0) {
                    write.nodeAddLabel(node, labelsToLookFor[3]);
                    if (seen.add(node)) {
                        nodesToFind.add(node);
                    }
                }
                if (i % 3 == 0) {
                    write.nodeAddLabel(node, labelsToLookFor[3]);
                    if (seen.add(node)) {
                        nodesToFind.add(node);
                    }
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
            var unionCursor = descendingUnionNodeLabelIndexCursor(tx, labelsToLookFor, cursors);

            // then
            nodesToFind.sort(Collections.reverseOrder());
            assertThat(asList(unionCursor)).isEqualTo(nodesToFind);
        }
    }

    private UnionNodeLabelIndexCursor ascendingUnionNodeLabelIndexCursor(
            KernelTransaction tx, int[] labelsToLookFor, NodeLabelIndexCursor[] cursors) throws KernelException {
        Read read = tx.dataRead();
        SchemaRead schemaRead = tx.schemaRead();
        IndexDescriptor index = schemaRead
                .index(SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR)
                .next();
        TokenReadSession tokenReadSession = read.tokenReadSession(index);
        return UnionNodeLabelIndexCursor.ascendingUnionNodeLabelIndexCursor(
                read, tokenReadSession, tx.cursorContext(), labelsToLookFor, cursors);
    }

    private UnionNodeLabelIndexCursor descendingUnionNodeLabelIndexCursor(
            KernelTransaction tx, int[] labelsToLookFor, NodeLabelIndexCursor[] cursors) throws KernelException {
        Read read = tx.dataRead();
        SchemaRead schemaRead = tx.schemaRead();
        IndexDescriptor index = schemaRead
                .index(SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR)
                .next();
        TokenReadSession tokenReadSession = read.tokenReadSession(index);
        return UnionNodeLabelIndexCursor.descendingUnionNodeLabelIndexCursor(
                read, tokenReadSession, tx.cursorContext(), labelsToLookFor, cursors);
    }

    private List<Long> asList(UnionNodeLabelIndexCursor cursor) {
        var result = new ArrayList<Long>();
        while (cursor.next()) {
            result.add(cursor.reference());
        }
        return result;
    }
}
