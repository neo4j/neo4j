/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.kernel.api.helpers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.kernel.api.helpers.BFSPruningVarExpandCursor.allExpander;
import static org.neo4j.internal.kernel.api.helpers.BFSPruningVarExpandCursor.incomingExpander;
import static org.neo4j.internal.kernel.api.helpers.BFSPruningVarExpandCursor.outgoingExpander;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.api.KernelTransaction.Type.EXPLICIT;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import org.eclipse.collections.impl.block.factory.primitive.LongPredicates;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.Predicates;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
class BFSPruningVarExpandCursorTest {
    private static final EmptyMemoryTracker NO_TRACKING = EmptyMemoryTracker.INSTANCE;

    @Inject
    private Kernel kernel;

    @Test
    void shouldDoBreadthFirstSearch() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //          (a1) → (b1)
            //        ↗ (a2) → (b2)
            // (start) → (a3) → (b3)
            //        ↘ (a4) → (b4)
            //          (a5) → (b5)
            long start = write.nodeCreate();
            // layer 1
            long a1 = write.nodeCreate();
            long a2 = write.nodeCreate();
            long a3 = write.nodeCreate();
            long a4 = write.nodeCreate();
            long a5 = write.nodeCreate();
            // layer 2
            long b1 = write.nodeCreate();
            long b2 = write.nodeCreate();
            long b3 = write.nodeCreate();
            long b4 = write.nodeCreate();
            long b5 = write.nodeCreate();

            // layer 1
            write.relationshipCreate(start, rel, a1);
            write.relationshipCreate(start, rel, a2);
            write.relationshipCreate(start, rel, a3);
            write.relationshipCreate(start, rel, a4);
            write.relationshipCreate(start, rel, a5);
            // layer 2
            write.relationshipCreate(a1, rel, b1);
            write.relationshipCreate(a2, rel, b2);
            write.relationshipCreate(a3, rel, b3);
            write.relationshipCreate(a4, rel, b4);
            write.relationshipCreate(a5, rel, b5);

            // then
            assertThat(asList(outgoingExpander(start, true, 26, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(start, a1, a2, a3, a4, a5, b1, b2, b3, b4, b5));
            assertThat(asList(outgoingExpander(start, false, 26, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a1, a2, a3, a4, a5, b1, b2, b3, b4, b5));
        }
    }

    @Test
    void shouldDoBreadthFirstSearchUndirected() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //          (a1) → (b1)
            //        ↗ (a2) → (b2)
            // (start) → (a3) → (b3)
            //        ↘ (a4) → (b4)
            //          (a5) → (b5)
            long start = write.nodeCreate();
            // layer 1
            long a1 = write.nodeCreate();
            long a2 = write.nodeCreate();
            long a3 = write.nodeCreate();
            long a4 = write.nodeCreate();
            long a5 = write.nodeCreate();
            // layer 2
            long b1 = write.nodeCreate();
            long b2 = write.nodeCreate();
            long b3 = write.nodeCreate();
            long b4 = write.nodeCreate();
            long b5 = write.nodeCreate();

            // layer 1
            write.relationshipCreate(start, rel, a1);
            write.relationshipCreate(start, rel, a2);
            write.relationshipCreate(start, rel, a3);
            write.relationshipCreate(start, rel, a4);
            write.relationshipCreate(start, rel, a5);
            // layer 2
            write.relationshipCreate(a1, rel, b1);
            write.relationshipCreate(a2, rel, b2);
            write.relationshipCreate(a3, rel, b3);
            write.relationshipCreate(a4, rel, b4);
            write.relationshipCreate(a5, rel, b5);

            // then
            assertThat(asList(allExpander(start, true, 26, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(start, a1, a2, a3, a4, a5, b1, b2, b3, b4, b5));
            assertThat(asList(allExpander(start, false, 26, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a1, a2, a3, a4, a5, b1, b2, b3, b4, b5));
        }
    }

    @Test
    void shouldDoBreadthFirstSearchWithNodePredicate() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //          (a1) → (b1)
            //        ↗ (a2) → (b2)
            // (start) → (X) → (b3)
            //        ↘ (a4) → (b4)
            //          (a5) → (b5)
            long start = write.nodeCreate();
            // layer 1
            long a1 = write.nodeCreate();
            long a2 = write.nodeCreate();
            long a3 = write.nodeCreate();
            long a4 = write.nodeCreate();
            long a5 = write.nodeCreate();
            // layer 2
            long b1 = write.nodeCreate();
            long b2 = write.nodeCreate();
            long b3 = write.nodeCreate();
            long b4 = write.nodeCreate();
            long b5 = write.nodeCreate();

            // layer 1
            write.relationshipCreate(start, rel, a1);
            write.relationshipCreate(start, rel, a2);
            write.relationshipCreate(start, rel, a3);
            write.relationshipCreate(start, rel, a4);
            write.relationshipCreate(start, rel, a5);
            // layer 2
            write.relationshipCreate(a1, rel, b1);
            write.relationshipCreate(a2, rel, b2);
            write.relationshipCreate(a3, rel, b3);
            write.relationshipCreate(a4, rel, b4);
            write.relationshipCreate(a5, rel, b5);

            // then
            assertThat(asList(outgoingExpander(
                            start,
                            true,
                            26,
                            tx.dataRead(),
                            nodeCursor,
                            relCursor,
                            n -> n != a3,
                            Predicates.alwaysTrue(),
                            NO_TRACKING)))
                    .isEqualTo(List.of(start, a1, a2, a4, a5, b1, b2, b4, b5));
            assertThat(asList(outgoingExpander(
                            start,
                            false,
                            26,
                            tx.dataRead(),
                            nodeCursor,
                            relCursor,
                            n -> n != a3,
                            Predicates.alwaysTrue(),
                            NO_TRACKING)))
                    .isEqualTo(List.of(a1, a2, a4, a5, b1, b2, b4, b5));
        }
    }

    @Test
    void shouldDoBreadthFirstSearchWithNodePredicateUndirected() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //          (a1) → (b1)
            //        ↗ (a2) → (b2)
            // (start) → (X) → (b3)
            //        ↘ (a4) → (b4)
            //          (a5) → (b5)
            long start = write.nodeCreate();
            // layer 1
            long a1 = write.nodeCreate();
            long a2 = write.nodeCreate();
            long a3 = write.nodeCreate();
            long a4 = write.nodeCreate();
            long a5 = write.nodeCreate();
            // layer 2
            long b1 = write.nodeCreate();
            long b2 = write.nodeCreate();
            long b3 = write.nodeCreate();
            long b4 = write.nodeCreate();
            long b5 = write.nodeCreate();

            // layer 1
            write.relationshipCreate(start, rel, a1);
            write.relationshipCreate(start, rel, a2);
            write.relationshipCreate(start, rel, a3);
            write.relationshipCreate(start, rel, a4);
            write.relationshipCreate(start, rel, a5);
            // layer 2
            write.relationshipCreate(a1, rel, b1);
            write.relationshipCreate(a2, rel, b2);
            write.relationshipCreate(a3, rel, b3);
            write.relationshipCreate(a4, rel, b4);
            write.relationshipCreate(a5, rel, b5);

            // then
            assertThat(asList(allExpander(
                            start,
                            true,
                            26,
                            tx.dataRead(),
                            nodeCursor,
                            relCursor,
                            n -> n != a3,
                            Predicates.alwaysTrue(),
                            NO_TRACKING)))
                    .isEqualTo(List.of(start, a1, a2, a4, a5, b1, b2, b4, b5));
            assertThat(asList(allExpander(
                            start,
                            false,
                            26,
                            tx.dataRead(),
                            nodeCursor,
                            relCursor,
                            n -> n != a3,
                            Predicates.alwaysTrue(),
                            NO_TRACKING)))
                    .isEqualTo(List.of(a1, a2, a4, a5, b1, b2, b4, b5));
        }
    }

    @Test
    void shouldDoBreadthFirstSearchWithRelationshipPredicate() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //          (a1) → (b1)
            //        ↗ (a2) → (b2)
            // (start) → (a3) → (b3)
            //        ↘ (a4) → (b4)
            //          (a5) → (b5)
            long start = write.nodeCreate();
            // layer 1
            long a1 = write.nodeCreate();
            long a2 = write.nodeCreate();
            long a3 = write.nodeCreate();
            long a4 = write.nodeCreate();
            long a5 = write.nodeCreate();
            // layer 2
            long b1 = write.nodeCreate();
            long b2 = write.nodeCreate();
            long b3 = write.nodeCreate();
            long b4 = write.nodeCreate();
            long b5 = write.nodeCreate();

            // layer 1
            write.relationshipCreate(start, rel, a1);
            write.relationshipCreate(start, rel, a2);
            write.relationshipCreate(start, rel, a3);
            var filterThis = write.relationshipCreate(start, rel, a4);
            write.relationshipCreate(start, rel, a5);
            // layer 2
            write.relationshipCreate(a1, rel, b1);
            var andFilterThat = write.relationshipCreate(a2, rel, b2);
            write.relationshipCreate(a3, rel, b3);
            write.relationshipCreate(a4, rel, b4);
            write.relationshipCreate(a5, rel, b5);

            // then
            assertThat(asList(outgoingExpander(
                            start,
                            true,
                            26,
                            tx.dataRead(),
                            nodeCursor,
                            relCursor,
                            LongPredicates.alwaysTrue(),
                            cursor -> cursor.relationshipReference() != filterThis
                                    && cursor.relationshipReference() != andFilterThat,
                            NO_TRACKING)))
                    .isEqualTo(List.of(start, a1, a2, a3, a5, b1, b3, b5));
            assertThat(asList(outgoingExpander(
                            start,
                            false,
                            26,
                            tx.dataRead(),
                            nodeCursor,
                            relCursor,
                            LongPredicates.alwaysTrue(),
                            cursor -> cursor.relationshipReference() != filterThis
                                    && cursor.relationshipReference() != andFilterThat,
                            NO_TRACKING)))
                    .isEqualTo(List.of(a1, a2, a3, a5, b1, b3, b5));
        }
    }

    @Test
    void shouldDoBreadthFirstSearchWithRelationshipPredicateUndirected() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //          (a1) → (b1)
            //        ↗ (a2) → (b2)
            // (start) → (a3) → (b3)
            //        ↘ (a4) → (b4)
            //          (a5) → (b5)
            long start = write.nodeCreate();
            // layer 1
            long a1 = write.nodeCreate();
            long a2 = write.nodeCreate();
            long a3 = write.nodeCreate();
            long a4 = write.nodeCreate();
            long a5 = write.nodeCreate();
            // layer 2
            long b1 = write.nodeCreate();
            long b2 = write.nodeCreate();
            long b3 = write.nodeCreate();
            long b4 = write.nodeCreate();
            long b5 = write.nodeCreate();

            // layer 1
            write.relationshipCreate(start, rel, a1);
            write.relationshipCreate(start, rel, a2);
            write.relationshipCreate(start, rel, a3);
            var filterThis = write.relationshipCreate(start, rel, a4);
            write.relationshipCreate(start, rel, a5);
            // layer 2
            write.relationshipCreate(a1, rel, b1);
            var andFilterThat = write.relationshipCreate(a2, rel, b2);
            write.relationshipCreate(a3, rel, b3);
            write.relationshipCreate(a4, rel, b4);
            write.relationshipCreate(a5, rel, b5);

            // then
            assertThat(asList(allExpander(
                            start,
                            true,
                            26,
                            tx.dataRead(),
                            nodeCursor,
                            relCursor,
                            LongPredicates.alwaysTrue(),
                            cursor -> cursor.relationshipReference() != filterThis
                                    && cursor.relationshipReference() != andFilterThat,
                            NO_TRACKING)))
                    .isEqualTo(List.of(start, a1, a2, a3, a5, b1, b3, b5));
            assertThat(asList(allExpander(
                            start,
                            false,
                            26,
                            tx.dataRead(),
                            nodeCursor,
                            relCursor,
                            LongPredicates.alwaysTrue(),
                            cursor -> cursor.relationshipReference() != filterThis
                                    && cursor.relationshipReference() != andFilterThat,
                            NO_TRACKING)))
                    .isEqualTo(List.of(a1, a2, a3, a5, b1, b3, b5));
        }
    }

    @Test
    void shouldOnlyTakeShortestPathBetweenNodes() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //        ↗ (b1) → (b2) → (b3) ↘
            // (start) → (a1)  →  (a2)  →  (end)
            long start = write.nodeCreate();
            long end = write.nodeCreate();
            long a1 = write.nodeCreate();
            long a2 = write.nodeCreate();
            long b1 = write.nodeCreate();
            long b2 = write.nodeCreate();
            long b3 = write.nodeCreate();

            write.relationshipCreate(start, rel, a1);
            write.relationshipCreate(a1, rel, a2);
            write.relationshipCreate(a2, rel, end);
            write.relationshipCreate(start, rel, b1);
            write.relationshipCreate(b1, rel, b2);
            write.relationshipCreate(b2, rel, b3);
            long shouldNotCross = write.relationshipCreate(b3, rel, end);

            // when
            var expander = outgoingExpander(start, false, 26, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING);

            // then
            assertThat(asList(expander)).isEqualTo(List.of(a1, b1, a2, b2, end, b3));
        }
    }

    @Test
    void shouldExpandOutgoing() throws KernelException {
        // given
        var graph = circleGraph(10);
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            // then
            assertThat(asList(outgoingExpander(
                            graph.startNode(), true, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(graph.nodes.subList(0, 4));
            assertThat(asList(outgoingExpander(
                            graph.startNode(), false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(graph.nodes.subList(1, 4));
        }
    }

    @Test
    void shouldExpandIncoming() throws KernelException {
        // given
        var graph = circleGraph(10);
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            // then
            assertThat(asList(incomingExpander(
                            graph.startNode(), true, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(graph.startNode(), graph.nodes.get(9), graph.nodes.get(8), graph.nodes.get(7)));
            assertThat(asList(incomingExpander(
                            graph.startNode(), false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(graph.nodes.get(9), graph.nodes.get(8), graph.nodes.get(7)));
        }
    }

    @Test
    void shouldExpandAll() throws KernelException {
        // given
        var graph = circleGraph(10);
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {

            // then
            assertThat(asList(
                            allExpander(graph.startNode(), true, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(
                            graph.startNode(),
                            graph.nodes.get(9),
                            graph.nodes.get(1),
                            graph.nodes.get(2),
                            graph.nodes.get(8),
                            graph.nodes.get(3),
                            graph.nodes.get(7)));
            assertThat(asList(allExpander(
                            graph.startNode(), false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(
                            graph.nodes.get(9),
                            graph.nodes.get(1),
                            graph.nodes.get(2),
                            graph.nodes.get(8),
                            graph.nodes.get(3),
                            graph.nodes.get(7)));
        }
    }

    @Test
    void shouldRespectTypesOutgoing() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            // given
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            int type1 = tokenWrite.relationshipTypeGetOrCreateForName("R1");
            int type2 = tokenWrite.relationshipTypeGetOrCreateForName("R2");
            long start = write.nodeCreate();
            long end = write.nodeCreate();
            write.relationshipCreate(start, type1, end);
            write.relationshipCreate(start, type2, write.nodeCreate());

            // then
            assertThat(asList(outgoingExpander(
                            start, new int[] {type1}, true, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(start, end));
            assertThat(asList(outgoingExpander(
                            start, new int[] {type1}, false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(end));
        }
    }

    @Test
    void shouldRespectTypesIncoming() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            // given
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            int type1 = tokenWrite.relationshipTypeGetOrCreateForName("R1");
            int type2 = tokenWrite.relationshipTypeGetOrCreateForName("R2");
            long start = write.nodeCreate();
            long end = write.nodeCreate();
            write.relationshipCreate(end, type1, start);
            write.relationshipCreate(write.nodeCreate(), type2, start);

            // then
            assertThat(asList(incomingExpander(
                            start, new int[] {type1}, true, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(start, end));
            assertThat(asList(incomingExpander(
                            start, new int[] {type1}, false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(end));
        }
    }

    @Test
    void shouldRespectTypesAll() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            // given
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            int type1 = tokenWrite.relationshipTypeGetOrCreateForName("R1");
            int type2 = tokenWrite.relationshipTypeGetOrCreateForName("R2");
            long start = write.nodeCreate();
            long end = write.nodeCreate();
            write.relationshipCreate(start, type1, end);
            write.relationshipCreate(start, type2, write.nodeCreate());

            // then
            assertThat(asList(allExpander(
                            start, new int[] {type1}, true, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(start, end));
            assertThat(asList(allExpander(
                            start, new int[] {type1}, false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(end));
        }
    }

    @Test
    void shouldExpandWithLength0() throws KernelException {
        // given
        var graph = circleGraph(10);
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            // then
            assertThat(asList(outgoingExpander(
                            graph.startNode(), true, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(graph.startNode()));
            assertThat(asList(outgoingExpander(
                            graph.startNode(), false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEmpty();
            assertThat(asList(incomingExpander(
                            graph.startNode(), true, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(graph.startNode()));
            assertThat(asList(incomingExpander(
                            graph.startNode(), false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEmpty();
            assertThat(asList(
                            allExpander(graph.startNode(), true, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(graph.startNode()));
            assertThat(asList(allExpander(
                            graph.startNode(), false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEmpty();
        }
    }

    @Test
    void endNodesAreUnique() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            // given
            Write write = tx.dataWrite();
            TokenWrite tokenWrite = tx.tokenWrite();
            int type = tokenWrite.relationshipTypeGetOrCreateForName("R1");
            long start = write.nodeCreate();
            long middleNode = write.nodeCreate();
            long end = write.nodeCreate();
            write.relationshipCreate(start, type, end);
            write.relationshipCreate(start, type, middleNode);
            write.relationshipCreate(middleNode, type, end);

            // when
            var expander = outgoingExpander(
                    start, new int[] {type}, false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING);

            // then
            assertThat(asList(expander)).hasSameElementsAs(List.of(middleNode, end));
        }
    }

    @Test
    void shouldTraverseFullGraph() throws KernelException {
        // given
        var graph = fanOutGraph(3, 3);
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {

            // then
            assertThat(asList(outgoingExpander(
                            graph.startNode(), true, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .hasSameElementsAs(graph.nodes());
            assertThat(asList(outgoingExpander(
                            graph.startNode(), false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .hasSameElementsAs(graph.dropStartNode());
        }
    }

    @Test
    void shouldStopAtSpecifiedDepth() throws KernelException {
        // given
        var graph = fanOutGraph(2, 5);
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            // when
            var expander =
                    outgoingExpander(graph.startNode(), false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING);

            // then
            assertThat(asList(expander)).hasSameElementsAs(graph.nodes.subList(1, 15));
        }
    }

    @Test
    void shouldSatisfyPredicateOnNodes() throws KernelException {
        // given
        var graph = circleGraph(100);
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            // then
            assertThat(asList(outgoingExpander(
                            graph.startNode(),
                            true,
                            11,
                            tx.dataRead(),
                            nodeCursor,
                            relCursor,
                            value -> value <= graph.nodes.get(5),
                            Predicates.alwaysTrue(),
                            NO_TRACKING)))
                    .hasSameElementsAs(graph.nodes.subList(0, 6));
            assertThat(asList(outgoingExpander(
                            graph.startNode(),
                            false,
                            11,
                            tx.dataRead(),
                            nodeCursor,
                            relCursor,
                            value -> value <= graph.nodes.get(5),
                            Predicates.alwaysTrue(),
                            NO_TRACKING)))
                    .hasSameElementsAs(graph.nodes.subList(1, 6));
        }
    }

    @Test
    void shouldSatisfyPredicateOnRelationships() throws KernelException {
        // given
        var graph = circleGraph(100);
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            // then
            assertThat(asList(outgoingExpander(
                            graph.startNode(),
                            true,
                            11,
                            tx.dataRead(),
                            nodeCursor,
                            relCursor,
                            LongPredicates.alwaysTrue(),
                            cursor -> cursor.relationshipReference() < graph.relationships.get(9),
                            NO_TRACKING)))
                    .isEqualTo(graph.nodes.subList(0, 10));
            assertThat(asList(outgoingExpander(
                            graph.startNode(),
                            false,
                            11,
                            tx.dataRead(),
                            nodeCursor,
                            relCursor,
                            LongPredicates.alwaysTrue(),
                            cursor -> cursor.relationshipReference() < graph.relationships.get(9),
                            NO_TRACKING)))
                    .isEqualTo(graph.nodes.subList(1, 10));
        }
    }

    @Test
    void shouldHandleSimpleLoopOutgoing() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            long node1 = write.nodeCreate();
            long node2 = write.nodeCreate();
            write.relationshipCreate(node1, rel, node2);
            write.relationshipCreate(node2, rel, node1);

            // when

            // then
            assertThat(asList(outgoingExpander(node1, true, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(node1, node2));
            assertThat(asList(outgoingExpander(node1, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(node2, node1));
        }
    }

    // tests targeting undirected searches
    @Test
    void shouldNotRetraceSteps() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            long node1 = write.nodeCreate();
            long node2 = write.nodeCreate();
            write.relationshipCreate(node1, rel, node2);

            // then
            assertThat(asList(allExpander(node1, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEmpty();
            assertThat(asList(allExpander(node1, true, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(node1, node2));
            assertThat(asList(allExpander(node1, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(node2));
            assertThat(asList(allExpander(node1, true, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(node1, node2));
            assertThat(asList(allExpander(node1, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(node2));
        }
    }

    @Test
    void shouldHandleSingleSelfLoop() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            long node1 = write.nodeCreate();
            write.relationshipCreate(node1, rel, node1);

            // then
            assertThat(asList(allExpander(node1, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEmpty();
            assertThat(asList(allExpander(node1, true, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(node1));
            assertThat(asList(allExpander(node1, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(node1));
            assertThat(asList(allExpander(node1, true, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(node1));
            assertThat(asList(allExpander(node1, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(node1));
        }
    }

    @Test
    void shouldHandleSimpleLoop() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            long node1 = write.nodeCreate();
            long node2 = write.nodeCreate();
            write.relationshipCreate(node1, rel, node2);
            write.relationshipCreate(node2, rel, node1);

            // then
            assertThat(asList(allExpander(node1, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEmpty();
            assertThat(asList(allExpander(node1, true, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(node1, node2));
            assertThat(asList(allExpander(node1, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(node2));
            assertThat(asList(allExpander(node1, true, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(node1, node2));
            assertThat(asList(allExpander(node1, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(node2, node1));
        }
    }

    @Test
    void shouldHandleSimpleLooWithPredicate() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            long node1 = write.nodeCreate();
            long node2 = write.nodeCreate();
            write.relationshipCreate(node1, rel, node2);
            long dontUse = write.relationshipCreate(node2, rel, node1);

            // then
            assertThat(asList(allExpander(
                            node1,
                            true,
                            2,
                            tx.dataRead(),
                            nodeCursor,
                            relCursor,
                            LongPredicates.alwaysTrue(),
                            cursor -> cursor.relationshipReference() != dontUse,
                            NO_TRACKING)))
                    .isEqualTo(List.of(node1, node2));
            assertThat(asList(allExpander(
                            node1,
                            false,
                            2,
                            tx.dataRead(),
                            nodeCursor,
                            relCursor,
                            LongPredicates.alwaysTrue(),
                            cursor -> cursor.relationshipReference() != dontUse,
                            NO_TRACKING)))
                    .isEqualTo(List.of(node2));
        }
    }

    @Test
    void shouldHandleSimpleTriangularPattern() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //     (b)
            //    /
            // (a)
            //    \
            //     (c)
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();
            write.relationshipCreate(a, rel, b);
            write.relationshipCreate(a, rel, c);

            // then
            assertThat(asList(allExpander(a, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEmpty();
            assertThat(asList(allExpander(a, true, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a, b, c));
            assertThat(asList(allExpander(a, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b, c));
            assertThat(asList(allExpander(a, true, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a, b, c));
            assertThat(asList(allExpander(a, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b, c));
        }
    }

    @Test
    void shouldHandleSimpleTriangularPatternWithBackTrace() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //     (b)
            //    //
            // (a)
            //    \
            //     (c)
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();
            write.relationshipCreate(a, rel, b);
            write.relationshipCreate(a, rel, b);
            write.relationshipCreate(a, rel, c);

            // then
            assertThat(asList(allExpander(a, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEmpty();
            assertThat(asList(allExpander(a, true, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a, b, c));
            assertThat(asList(allExpander(a, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b, c));
            assertThat(asList(allExpander(a, true, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a, b, c));
            assertThat(asList(allExpander(a, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b, c, a));
        }
    }

    @Test
    void shouldHandleSimpleTriangularPatternWithBackTrace2() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //     (b)
            //    /
            // (a)
            //   \\
            //     (c)
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();
            write.relationshipCreate(a, rel, b);
            write.relationshipCreate(a, rel, c);
            write.relationshipCreate(c, rel, a);

            // then
            assertThat(asList(allExpander(a, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEmpty();
            assertThat(asList(allExpander(a, true, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a, c, b));
            assertThat(asList(allExpander(a, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(c, b));
            assertThat(asList(allExpander(a, true, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a, c, b));
            assertThat(asList(allExpander(a, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(c, b, a));
        }
    }

    @Test
    void shouldHandleTriangularLoop() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //     (b)
            //    / |
            // (a)  |
            //    \ |
            //     (c)
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();
            write.relationshipCreate(a, rel, b);
            write.relationshipCreate(a, rel, c);
            write.relationshipCreate(b, rel, c);

            // then
            assertThat(asList(allExpander(a, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEmpty();
            assertThat(asList(allExpander(a, true, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a, b, c));
            assertThat(asList(allExpander(a, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b, c));
            assertThat(asList(allExpander(a, true, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a, b, c));
            assertThat(asList(allExpander(a, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b, c));
            assertThat(asList(allExpander(a, true, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a, b, c));
            assertThat(asList(allExpander(a, false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b, c, a));
        }
    }

    @Test
    void shouldHandleTriangularLoop2() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //            (b)
            //           / |
            // (start)-(a)  |
            //           \ |
            //            (c)
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();
            write.relationshipCreate(start, rel, a);
            write.relationshipCreate(a, rel, b);
            write.relationshipCreate(a, rel, c);
            write.relationshipCreate(b, rel, c);

            // then
            assertThat(asList(allExpander(start, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEmpty();
            assertThat(asList(allExpander(start, true, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(start, a));
            assertThat(asList(allExpander(start, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a));
            assertThat(asList(allExpander(start, true, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(start, a, b, c));
            assertThat(asList(allExpander(start, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a, b, c));
            assertThat(asList(allExpander(start, true, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(start, a, b, c));
            assertThat(asList(allExpander(start, false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a, b, c));
            assertThat(asList(allExpander(start, true, 4, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(start, a, b, c));
            assertThat(asList(allExpander(start, false, 4, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a, b, c));
            assertThat(asList(allExpander(start, true, 5, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(start, a, b, c));
            assertThat(asList(allExpander(start, false, 5, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a, b, c, start));
        }
    }

    @Test
    void shouldHandleLoopBetweenDifferentBFSLayers() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //     (b)--(d)
            //    /     /
            // (a)     /
            //    \   /
            //     (c)
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();
            long d = write.nodeCreate();
            write.relationshipCreate(a, rel, b);
            write.relationshipCreate(a, rel, c);
            write.relationshipCreate(b, rel, d);
            write.relationshipCreate(d, rel, c);

            // then
            assertThat(asList(allExpander(a, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEmpty();
            assertThat(asList(allExpander(a, true, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a, b, c));
            assertThat(asList(allExpander(a, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b, c));
            assertThat(asList(allExpander(a, true, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a, b, c, d));
            assertThat(asList(allExpander(a, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b, c, d));
            assertThat(asList(allExpander(a, true, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a, b, c, d));
            assertThat(asList(allExpander(a, false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b, c, d));
            assertThat(asList(allExpander(a, true, 4, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a, b, c, d));
            assertThat(asList(allExpander(a, false, 4, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b, c, d, a));
        }
    }

    @Test
    void shouldHandleLoopConnectingSameBFSLayer() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //     (b)--(d)
            //    /      |
            // (a)       |
            //    \      |
            //     (c)--(e)
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();
            long d = write.nodeCreate();
            long e = write.nodeCreate();
            write.relationshipCreate(a, rel, b);
            write.relationshipCreate(a, rel, c);
            write.relationshipCreate(b, rel, d);
            write.relationshipCreate(c, rel, e);
            write.relationshipCreate(e, rel, d);

            // then
            assertThat(asList(allExpander(a, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEmpty();
            assertThat(asList(allExpander(a, true, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a, b, c));
            assertThat(asList(allExpander(a, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b, c));
            assertThat(asList(allExpander(a, true, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a, b, c, d, e));
            assertThat(asList(allExpander(a, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b, c, d, e));
            assertThat(asList(allExpander(a, true, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a, b, c, d, e));
            assertThat(asList(allExpander(a, false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b, c, d, e));
            assertThat(asList(allExpander(a, false, 4, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b, c, d, e));
            assertThat(asList(allExpander(a, true, 5, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a, b, c, d, e));
            assertThat(asList(allExpander(a, false, 5, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b, c, d, e, a));
        }
    }

    @Test
    void shouldHandleLoopConnectingSameAndDifferentBFSLayer() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //     (b1)-(c1)
            //    /    \
            // (a)-(b2)-(c2)
            //    \ |
            //     (b3)-(c3)
            long a = write.nodeCreate();
            long b1 = write.nodeCreate();
            long b2 = write.nodeCreate();
            long b3 = write.nodeCreate();
            long c1 = write.nodeCreate();
            long c2 = write.nodeCreate();
            long c3 = write.nodeCreate();
            write.relationshipCreate(a, rel, b1);
            write.relationshipCreate(a, rel, b2);
            write.relationshipCreate(a, rel, b3);
            write.relationshipCreate(b1, rel, c1);
            write.relationshipCreate(b1, rel, c2);
            write.relationshipCreate(b2, rel, c2);
            write.relationshipCreate(b2, rel, b3);
            write.relationshipCreate(b3, rel, c3);

            // then
            assertThat(asList(allExpander(a, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEmpty();
            assertThat(asList(allExpander(a, true, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a, b1, b2, b3));
            assertThat(asList(allExpander(a, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b1, b2, b3));
            assertThat(asList(allExpander(a, true, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a, b1, b2, b3, c1, c2, c3));
            assertThat(asList(allExpander(a, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b1, b2, b3, c1, c2, c3));
            assertThat(asList(allExpander(a, true, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a, b1, b2, b3, c1, c2, c3));
            assertThat(asList(allExpander(a, false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b1, b2, b3, c1, c2, c3, a));
        }
    }

    @Test
    void shouldHandleLoopWithContinuation() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //     (b)
            //    /   \
            // (a)     (d) - (e) - (f)
            //    \   /
            //     (c)
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();
            long d = write.nodeCreate();
            long e = write.nodeCreate();
            long f = write.nodeCreate();
            write.relationshipCreate(a, rel, b);
            write.relationshipCreate(a, rel, c);
            write.relationshipCreate(b, rel, d);
            write.relationshipCreate(d, rel, c);
            write.relationshipCreate(d, rel, e);
            write.relationshipCreate(e, rel, f);

            // then
            assertThat(asList(allExpander(a, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEmpty();
            assertThat(asList(allExpander(a, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .hasSameElementsAs(List.of(b, c));
            assertThat(asList(allExpander(a, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .hasSameElementsAs(List.of(b, c, d));
            assertThat(asList(allExpander(a, false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .hasSameElementsAs(List.of(b, c, d, e));
            assertThat(asList(allExpander(a, false, 4, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .hasSameElementsAs(List.of(b, c, d, e, f, a));
        }
    }

    @Test
    void shouldHandleParallelLayers() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //     (b)--(d)--(f)
            //    /
            // (a)
            //    \
            //     (c)--(e)--(g)
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();
            long d = write.nodeCreate();
            long e = write.nodeCreate();
            long f = write.nodeCreate();
            long g = write.nodeCreate();
            write.relationshipCreate(a, rel, b);
            write.relationshipCreate(a, rel, c);
            write.relationshipCreate(b, rel, d);
            write.relationshipCreate(c, rel, e);
            write.relationshipCreate(d, rel, f);
            write.relationshipCreate(e, rel, g);

            // then
            assertThat(asList(allExpander(a, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEmpty();
            assertThat(asList(allExpander(a, true, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a, b, c));
            assertThat(asList(allExpander(a, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b, c));
            assertThat(asList(allExpander(a, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b, c, d, e));
            assertThat(asList(allExpander(a, false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b, c, d, e, f, g));
            assertThat(asList(allExpander(a, false, 4, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b, c, d, e, f, g));
            assertThat(asList(allExpander(a, true, 5, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a, b, c, d, e, f, g));
            assertThat(asList(allExpander(a, false, 5, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b, c, d, e, f, g));
        }
    }

    @Test
    void shouldNotRetraceWhenALoopIsDetectedThatHasNoPathLeftToOrigin() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // (a) -> (b) <=> (c)
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();

            write.relationshipCreate(a, rel, b);
            write.relationshipCreate(b, rel, c);
            write.relationshipCreate(c, rel, b);

            // then
            assertThat(asList(allExpander(a, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEmpty();
            assertThat(asList(allExpander(a, true, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a, b));
            assertThat(asList(allExpander(a, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b));
            assertThat(asList(allExpander(a, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b, c));
            assertThat(asList(allExpander(a, false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b, c));
            assertThat(asList(allExpander(a, true, 4, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a, b, c));
            assertThat(asList(allExpander(a, false, 4, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b, c));
        }
    }

    @Test
    void shouldNotRetraceWhenALoopIsDetectedThatHasNoPathLeftToOriginOutGoing() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // (a) -> (b) <=> (c)
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();

            write.relationshipCreate(a, rel, b);
            write.relationshipCreate(b, rel, c);
            write.relationshipCreate(c, rel, b);

            // then
            assertThat(asList(outgoingExpander(a, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEmpty();
            assertThat(asList(outgoingExpander(a, true, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a, b));
            assertThat(asList(outgoingExpander(a, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b));
            assertThat(asList(outgoingExpander(a, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b, c));
            assertThat(asList(outgoingExpander(a, false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b, c));
            assertThat(asList(outgoingExpander(a, true, 4, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a, b, c));
            assertThat(asList(outgoingExpander(a, false, 4, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b, c));
        }
    }

    @Test
    void shouldNotRetraceWhenALoopIsDetectedThatHasNoPathLeftToOrigin2() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // (a) -> (b) <=> (b)
            long a = write.nodeCreate();
            long b = write.nodeCreate();

            write.relationshipCreate(a, rel, b);
            write.relationshipCreate(b, rel, b);

            // then
            assertThat(asList(allExpander(a, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEmpty();
            assertThat(asList(allExpander(a, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b));
            assertThat(asList(allExpander(a, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b));
            assertThat(asList(allExpander(a, false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b));
            assertThat(asList(allExpander(a, true, 4, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(a, b));
            assertThat(asList(allExpander(a, false, 4, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b));
        }
    }

    @Test
    void shouldRetraceWhenALoopIsDetectedThatHasPathToOrigin() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // (a) <=> (b) <=> (c)
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();

            write.relationshipCreate(a, rel, b);
            write.relationshipCreate(b, rel, a);
            write.relationshipCreate(b, rel, c);
            write.relationshipCreate(c, rel, b);

            // then
            assertThat(asList(allExpander(a, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEmpty();
            assertThat(asList(allExpander(a, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b));
            assertThat(asList(allExpander(a, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b, c, a));
            assertThat(asList(allExpander(a, false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b, c, a));
            assertThat(asList(allExpander(a, false, 4, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b, c, a));
        }
    }

    @Test
    void shouldHandleDoublyConnectedFanOutGraph() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //            (c1)
            //           //
            //       (b1) = (c2)
            //       /   \\
            //      /      (c3)
            //     /
            //    /        (c4)
            //   /       //
            // (a)---(b2) = (c5)
            //    \      \\
            //     \       (c6)
            //      \
            //       \     (c7)
            //        \  //
            //       (b3) = (c8)
            //           \\
            //             (c9)
            long a = write.nodeCreate();
            long b1 = write.nodeCreate();
            long b2 = write.nodeCreate();
            long b3 = write.nodeCreate();
            long c1 = write.nodeCreate();
            long c2 = write.nodeCreate();
            long c3 = write.nodeCreate();
            long c4 = write.nodeCreate();
            long c5 = write.nodeCreate();
            long c6 = write.nodeCreate();
            long c7 = write.nodeCreate();
            long c8 = write.nodeCreate();
            long c9 = write.nodeCreate();

            // from a
            write.relationshipCreate(a, rel, b1);
            write.relationshipCreate(a, rel, b2);
            write.relationshipCreate(a, rel, b3);

            // from b1
            write.relationshipCreate(b1, rel, c1);
            write.relationshipCreate(b1, rel, c1);
            write.relationshipCreate(b1, rel, c2);
            write.relationshipCreate(b1, rel, c2);
            write.relationshipCreate(b1, rel, c3);
            write.relationshipCreate(b1, rel, c3);
            // from b2
            write.relationshipCreate(b2, rel, c4);
            write.relationshipCreate(b2, rel, c4);
            write.relationshipCreate(b2, rel, c5);
            write.relationshipCreate(b2, rel, c5);
            write.relationshipCreate(b2, rel, c6);
            write.relationshipCreate(b2, rel, c6);
            // from b3
            write.relationshipCreate(b3, rel, c7);
            write.relationshipCreate(b3, rel, c7);
            write.relationshipCreate(b3, rel, c8);
            write.relationshipCreate(b3, rel, c8);
            write.relationshipCreate(b3, rel, c9);
            write.relationshipCreate(b3, rel, c9);

            // then
            assertThat(asList(allExpander(a, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEmpty();
            assertThat(asList(allExpander(a, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b1, b2, b3));
            assertThat(asList(allExpander(a, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b1, b2, b3, c1, c2, c3, c4, c5, c6, c7, c8, c9));
            assertThat(asList(allExpander(a, false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b1, b2, b3, c1, c2, c3, c4, c5, c6, c7, c8, c9));
            assertThat(asList(allExpander(a, false, 4, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(List.of(b1, b2, b3, c1, c2, c3, c4, c5, c6, c7, c8, c9));
        }
    }

    @Test
    void shouldHandleComplicatedGraph() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //     _ (b)
            //    /  /|\
            //   /  / | \
            // (e)-(a)-|-(c)
            //       \|/
            //       (d)
            //
            long a = write.nodeCreate(); // 0
            long b = write.nodeCreate(); // 1
            long c = write.nodeCreate(); // 2
            long d = write.nodeCreate(); // 3
            long e = write.nodeCreate(); // 4
            write.relationshipCreate(a, rel, b); // 0
            write.relationshipCreate(c, rel, a); // 1
            write.relationshipCreate(d, rel, a); // 2
            write.relationshipCreate(e, rel, a); // 3

            write.relationshipCreate(b, rel, c); // 4
            write.relationshipCreate(b, rel, d); // 5
            write.relationshipCreate(e, rel, b); // 6

            write.relationshipCreate(c, rel, d); // 7

            // then
            assertThat(asList(allExpander(a, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEmpty();
            assertThat(asList(allExpander(a, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .hasSameElementsAs(List.of(b, c, d, e));
            assertThat(asList(allExpander(a, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .hasSameElementsAs(List.of(b, c, d, e));
            assertThat(asList(allExpander(a, false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .hasSameElementsAs(List.of(b, c, d, e, a));
        }
    }

    private Graph fanOutGraph(int relPerNode, int depth) throws KernelException {
        long start = -1;
        ArrayList<Long> nodes = new ArrayList<>();
        ArrayList<Long> relationships = new ArrayList<>();
        try (KernelTransaction tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            var write = tx.dataWrite();
            var tokenWrite = tx.tokenWrite();
            var type = tokenWrite.relationshipTypeGetOrCreateForName("R");
            int totalNumberOfNodes = (int) ((1L - Math.round(Math.pow(relPerNode, depth + 1))) / (1L - relPerNode));
            start = write.nodeCreate();
            nodes.add(start);
            int nodeCount = 1;
            Queue<Long> queue = new LinkedList<>();
            queue.offer(start);
            while (!queue.isEmpty()) {
                var startNode = queue.poll();
                for (int i = 0; i < relPerNode; i++) {
                    var next = write.nodeCreate();
                    nodes.add(next);
                    queue.offer(next);
                    relationships.add(write.relationshipCreate(startNode, type, next));
                }
                nodeCount += relPerNode;

                if (nodeCount >= totalNumberOfNodes) {
                    break;
                }
            }
            tx.commit();
        }

        return new Graph(nodes, relationships);
    }

    private Graph circleGraph(int numberOfNodes) throws KernelException {
        assert numberOfNodes >= 2;
        long start = -1;
        ArrayList<Long> nodes = new ArrayList<>();
        ArrayList<Long> relationships = new ArrayList<>();
        try (KernelTransaction tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED)) {
            var write = tx.dataWrite();
            var tokenWrite = tx.tokenWrite();
            var type = tokenWrite.relationshipTypeGetOrCreateForName("R");
            start = write.nodeCreate();
            nodes.add(start);
            long begin = start;
            long end = -1;
            for (int i = 1; i < numberOfNodes; i++) {
                end = write.nodeCreate();
                nodes.add(end);
                relationships.add(write.relationshipCreate(begin, type, end));
                begin = end;
            }
            relationships.add(write.relationshipCreate(end, type, start));
            tx.commit();
        }

        return new Graph(nodes, relationships);
    }

    private record Graph(ArrayList<Long> nodes, ArrayList<Long> relationships) {
        public long startNode() {
            return nodes.get(0);
        }

        public List<Long> dropStartNode() {
            return nodes.subList(1, nodes.size());
        }
    }

    private List<Long> asList(BFSPruningVarExpandCursor expander) {
        ArrayList<Long> found = new ArrayList<>();
        while (expander.next()) {
            found.add(expander.endNode());
        }
        return found;
    }
}
