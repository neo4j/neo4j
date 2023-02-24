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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.block.factory.primitive.LongPredicates;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
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
            assertThat(asDepthMap(outgoingExpander(start, true, 26, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .containsOnlyKeys(start, a1, a2, a3, a4, a5, b1, b2, b3, b4, b5);
            assertThat(asDepthMap(
                            outgoingExpander(start, false, 26, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .containsOnlyKeys(a1, a2, a3, a4, a5, b1, b2, b3, b4, b5);
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
            assertThat(asDepthMap(allExpander(start, false, 26, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEqualTo(Map.of(a1, 0, a2, 0, a3, 0, a4, 0, a5, 0, b1, 1, b2, 1, b3, 1, b4, 1, b5, 1));
            assertThat(asDepthMap(allExpander(start, true, 26, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .containsOnlyKeys(List.of(start, a1, a2, a3, a4, a5, b1, b2, b3, b4, b5));
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
            assertThat(asDepthMap(outgoingExpander(
                            start,
                            true,
                            26,
                            tx.dataRead(),
                            nodeCursor,
                            relCursor,
                            n -> n != a3,
                            Predicates.alwaysTrue(),
                            NO_TRACKING)))
                    .containsOnlyKeys(start, a1, a2, a4, a5, b1, b2, b4, b5);
            assertThat(asDepthMap(outgoingExpander(
                            start,
                            false,
                            26,
                            tx.dataRead(),
                            nodeCursor,
                            relCursor,
                            n -> n != a3,
                            Predicates.alwaysTrue(),
                            NO_TRACKING)))
                    .containsOnlyKeys(a1, a2, a4, a5, b1, b2, b4, b5);
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
            assertThat(asDepthMap(allExpander(
                            start,
                            true,
                            26,
                            tx.dataRead(),
                            nodeCursor,
                            relCursor,
                            n -> n != a3,
                            Predicates.alwaysTrue(),
                            NO_TRACKING)))
                    .containsOnlyKeys(List.of(start, a1, a2, a4, a5, b1, b2, b4, b5));
            assertThat(asDepthMap(allExpander(
                            start,
                            false,
                            26,
                            tx.dataRead(),
                            nodeCursor,
                            relCursor,
                            n -> n != a3,
                            Predicates.alwaysTrue(),
                            NO_TRACKING)))
                    .containsOnlyKeys(List.of(a1, a2, a4, a5, b1, b2, b4, b5));
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
            assertThat(asDepthMap(outgoingExpander(
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
                    .containsOnlyKeys(List.of(start, a1, a2, a3, a5, b1, b3, b5));
            assertThat(asDepthMap(outgoingExpander(
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
                    .containsOnlyKeys(List.of(a1, a2, a3, a5, b1, b3, b5));
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
            assertThat(asDepthMap(allExpander(
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
                    .containsOnlyKeys(List.of(start, a1, a2, a3, a5, b1, b3, b5));
            assertThat(asDepthMap(allExpander(
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
                    .containsOnlyKeys(List.of(a1, a2, a3, a5, b1, b3, b5));
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
            assertThat(graph(expander, false))
                    .isEqualTo(graph().add(a1, b1).add(a2, b2).add(b3, end).build());
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
            assertThat(graph(
                            outgoingExpander(
                                    graph.startNode(), true, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING),
                            true))
                    .isEqualTo(graph(graph.nodes.subList(0, 4)));
            assertThat(graph(
                            outgoingExpander(
                                    graph.startNode(), false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING),
                            false))
                    .isEqualTo(graph(graph.nodes.subList(1, 4)));
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
            assertThat(graph(
                            incomingExpander(
                                    graph.startNode(), true, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING),
                            true))
                    .isEqualTo(graph().add(graph.startNode())
                            .add(graph.nodes.get(9))
                            .add(graph.nodes.get(8))
                            .add(graph.nodes.get(7))
                            .build());
            assertThat(graph(
                            incomingExpander(
                                    graph.startNode(), false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING),
                            false))
                    .isEqualTo(graph().add(graph.nodes.get(9))
                            .add(graph.nodes.get(8))
                            .add(graph.nodes.get(7))
                            .build());
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
            assertThat(graph(
                            allExpander(graph.startNode(), true, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING),
                            true))
                    .isEqualTo(graph().add(graph.startNode())
                            .add(graph.nodes.get(1), graph.nodes.get(9))
                            .add(graph.nodes.get(2), graph.nodes.get(8))
                            .add(graph.nodes.get(3), graph.nodes.get(7))
                            .build());
            assertThat(graph(
                            allExpander(graph.startNode(), false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING),
                            false))
                    .isEqualTo(graph().add(graph.nodes.get(1), graph.nodes.get(9))
                            .add(graph.nodes.get(2), graph.nodes.get(8))
                            .add(graph.nodes.get(3), graph.nodes.get(7))
                            .build());
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
            assertThat(graph(
                            outgoingExpander(
                                    start,
                                    new int[] {type1},
                                    true,
                                    3,
                                    tx.dataRead(),
                                    nodeCursor,
                                    relCursor,
                                    NO_TRACKING),
                            true))
                    .isEqualTo(graph().add(start).add(end).build());
            assertThat(graph(
                            outgoingExpander(
                                    start,
                                    new int[] {type1},
                                    false,
                                    3,
                                    tx.dataRead(),
                                    nodeCursor,
                                    relCursor,
                                    NO_TRACKING),
                            false))
                    .isEqualTo(graph().add(end).build());
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
            assertThat(graph(
                            incomingExpander(
                                    start,
                                    new int[] {type1},
                                    true,
                                    3,
                                    tx.dataRead(),
                                    nodeCursor,
                                    relCursor,
                                    NO_TRACKING),
                            true))
                    .isEqualTo(graph().add(start).add(end).build());
            assertThat(graph(
                            incomingExpander(
                                    start,
                                    new int[] {type1},
                                    false,
                                    3,
                                    tx.dataRead(),
                                    nodeCursor,
                                    relCursor,
                                    NO_TRACKING),
                            false))
                    .isEqualTo(graph().add(end).build());
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
            assertThat(graph(
                            allExpander(
                                    start,
                                    new int[] {type1},
                                    true,
                                    3,
                                    tx.dataRead(),
                                    nodeCursor,
                                    relCursor,
                                    NO_TRACKING),
                            true))
                    .isEqualTo(graph().add(start).add(end).build());
            assertThat(graph(
                            allExpander(
                                    start,
                                    new int[] {type1},
                                    false,
                                    3,
                                    tx.dataRead(),
                                    nodeCursor,
                                    relCursor,
                                    NO_TRACKING),
                            false))
                    .isEqualTo(graph().add(end).build());
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
            assertThat(graph(
                            outgoingExpander(
                                    graph.startNode(), true, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING),
                            true))
                    .isEqualTo(graph().add(graph.startNode()).build());
            assertThat(graph(
                            outgoingExpander(
                                    graph.startNode(), false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING),
                            false))
                    .isEqualTo(EMPTY);
            assertThat(graph(
                            incomingExpander(
                                    graph.startNode(), true, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING),
                            true))
                    .isEqualTo(graph().add(graph.startNode()).build());
            assertThat(graph(
                            incomingExpander(
                                    graph.startNode(), false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING),
                            false))
                    .isEqualTo(EMPTY);
            assertThat(graph(
                            allExpander(graph.startNode(), true, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING),
                            true))
                    .isEqualTo(graph().add(graph.startNode()).build());
            assertThat(graph(
                            allExpander(graph.startNode(), false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING),
                            false))
                    .isEqualTo(EMPTY);
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
            assertThat(graph(expander, false))
                    .isEqualTo(graph().add(middleNode, end).build());
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
            assertThat(graph(
                            outgoingExpander(
                                    graph.startNode(), true, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING),
                            true))
                    .isEqualTo(graph().add(graph.nodes.get(0))
                            .add(graph.nodes.subList(1, 4))
                            .add(graph.nodes.subList(4, 13))
                            .add(graph.nodes.subList(13, 40))
                            .build());
            assertThat(graph(
                            outgoingExpander(
                                    graph.startNode(), false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING),
                            false))
                    .isEqualTo(graph().add(graph.nodes.subList(1, 4))
                            .add(graph.nodes.subList(4, 13))
                            .add(graph.nodes.subList(13, 40))
                            .build());
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
            assertThat(graph(expander, false))
                    .isEqualTo(graph().add(graph.nodes.get(1), graph.nodes.get(2))
                            .add(graph.nodes.get(3), graph.nodes.get(4), graph.nodes.get(5), graph.nodes.get(6))
                            .add(
                                    graph.nodes.get(7),
                                    graph.nodes.get(8),
                                    graph.nodes.get(9),
                                    graph.nodes.get(10),
                                    graph.nodes.get(11),
                                    graph.nodes.get(12),
                                    graph.nodes.get(13),
                                    graph.nodes.get(14))
                            .build());
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
            assertThat(graph(
                            outgoingExpander(
                                    graph.startNode(),
                                    true,
                                    11,
                                    tx.dataRead(),
                                    nodeCursor,
                                    relCursor,
                                    value -> value <= graph.nodes.get(5),
                                    Predicates.alwaysTrue(),
                                    NO_TRACKING),
                            true))
                    .isEqualTo(graph(graph.nodes.subList(0, 6)));
            assertThat(graph(
                            outgoingExpander(
                                    graph.startNode(),
                                    false,
                                    11,
                                    tx.dataRead(),
                                    nodeCursor,
                                    relCursor,
                                    value -> value <= graph.nodes.get(5),
                                    Predicates.alwaysTrue(),
                                    NO_TRACKING),
                            false))
                    .isEqualTo(graph(graph.nodes.subList(1, 6)));
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
            assertThat(graph(
                            outgoingExpander(
                                    graph.startNode(),
                                    true,
                                    11,
                                    tx.dataRead(),
                                    nodeCursor,
                                    relCursor,
                                    LongPredicates.alwaysTrue(),
                                    cursor -> cursor.relationshipReference() < graph.relationships.get(9),
                                    NO_TRACKING),
                            true))
                    .isEqualTo(graph(graph.nodes.subList(0, 10)));
            assertThat(graph(
                            outgoingExpander(
                                    graph.startNode(),
                                    false,
                                    11,
                                    tx.dataRead(),
                                    nodeCursor,
                                    relCursor,
                                    LongPredicates.alwaysTrue(),
                                    cursor -> cursor.relationshipReference() < graph.relationships.get(9),
                                    NO_TRACKING),
                            false))
                    .isEqualTo(graph(graph.nodes.subList(1, 10)));
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

            // then
            assertThat(graph(outgoingExpander(node1, true, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(node1).add(node2).build());
            assertThat(graph(
                            outgoingExpander(node1, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING),
                            false))
                    .isEqualTo(graph().add(node2).add(node1).build());
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
            assertThat(graph(allExpander(node1, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(EMPTY);
            assertThat(graph(allExpander(node1, true, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(node1).add(node2).build());
            assertThat(graph(allExpander(node1, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(node2).build());
            assertThat(graph(allExpander(node1, true, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(node1).add(node2).build());
            assertThat(graph(allExpander(node1, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(node2).build());
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
            assertThat(graph(allExpander(node1, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(EMPTY);
            assertThat(graph(allExpander(node1, true, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(node1).build());
            assertThat(graph(allExpander(node1, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(node1).build());
            assertThat(graph(allExpander(node1, true, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(node1).build());
            assertThat(graph(allExpander(node1, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(node1).build());
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
            assertThat(graph(allExpander(node1, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(EMPTY);
            assertThat(graph(allExpander(node1, true, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(node1).add(node2).build());
            assertThat(graph(allExpander(node1, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(node2).build());
            assertThat(graph(allExpander(node1, true, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(node1).add(node2).build());
            assertThat(graph(allExpander(node1, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(node2).add(node1).build());
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
            assertThat(graph(
                            allExpander(
                                    node1,
                                    true,
                                    2,
                                    tx.dataRead(),
                                    nodeCursor,
                                    relCursor,
                                    LongPredicates.alwaysTrue(),
                                    cursor -> cursor.relationshipReference() != dontUse,
                                    NO_TRACKING),
                            true))
                    .isEqualTo(graph().add(node1).add(node2).build());
            assertThat(graph(
                            allExpander(
                                    node1,
                                    false,
                                    2,
                                    tx.dataRead(),
                                    nodeCursor,
                                    relCursor,
                                    LongPredicates.alwaysTrue(),
                                    cursor -> cursor.relationshipReference() != dontUse,
                                    NO_TRACKING),
                            false))
                    .isEqualTo(graph().add(node2).build());
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
            assertThat(graph(allExpander(a, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(EMPTY);
            assertThat(graph(allExpander(a, true, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(a).add(b, c).build());
            assertThat(graph(allExpander(a, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b, c).build());
            assertThat(graph(allExpander(a, true, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(a).add(b, c).build());
            assertThat(graph(allExpander(a, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b, c).build());
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
            assertThat(graph(allExpander(a, true, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(a).build());
            assertThat(graph(allExpander(a, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(EMPTY);
            assertThat(graph(allExpander(a, true, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(a).add(b, c).build());
            assertThat(graph(allExpander(a, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b, c).build());
            assertThat(graph(allExpander(a, true, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(a).add(b, c).build());
            assertThat(graph(allExpander(a, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b, c).add(a).build());
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
            assertThat(graph(allExpander(a, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(EMPTY);
            assertThat(graph(allExpander(a, true, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(a).add(b, c).build());
            assertThat(graph(allExpander(a, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b, c).build());
            assertThat(graph(allExpander(a, true, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(a).add(b, c).build());
            assertThat(graph(allExpander(a, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b, c).add(a).build());
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
            assertThat(graph(allExpander(a, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(EMPTY);
            assertThat(graph(allExpander(a, true, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(a).add(b, c).build());
            assertThat(graph(allExpander(a, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b, c).build());
            assertThat(graph(allExpander(a, true, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(a).add(b, c).build());
            assertThat(graph(allExpander(a, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b, c).build());
            assertThat(graph(allExpander(a, true, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(a).add(b, c).build());
            assertThat(graph(allExpander(a, false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b, c).add(a).build());
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
            assertThat(graph(allExpander(start, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(EMPTY);
            assertThat(graph(allExpander(start, true, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(start).add(a).build());
            assertThat(graph(allExpander(start, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(a).build());
            assertThat(graph(allExpander(start, true, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(start).add(a).add(b, c).build());
            assertThat(graph(allExpander(start, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(a).add(b, c).build());
            assertThat(graph(allExpander(start, true, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(start).add(a).add(b, c).build());
            assertThat(graph(allExpander(start, false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(a).add(b, c).build());
            assertThat(graph(allExpander(start, true, 4, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(start).add(a).add(b, c).build());
            assertThat(graph(allExpander(start, false, 4, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(a).add(b, c).build());
            assertThat(graph(allExpander(start, true, 5, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(start).add(a).add(b, c).build());
            assertThat(graph(allExpander(start, false, 5, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(a).add(b, c).add(start).build());
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
            assertThat(graph(allExpander(a, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(EMPTY);
            assertThat(graph(allExpander(a, true, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(a).add(b, c).build());
            assertThat(graph(allExpander(a, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b, c).build());
            assertThat(graph(allExpander(a, true, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(a).add(b, c).add(d).build());
            assertThat(graph(allExpander(a, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b, c).add(d).build());
            assertThat(graph(allExpander(a, true, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(a).add(b, c).add(d).build());
            assertThat(graph(allExpander(a, false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b, c).add(d).build());
            assertThat(graph(allExpander(a, true, 4, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(a).add(b, c).add(d).build());
            assertThat(graph(allExpander(a, false, 4, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b, c).add(d).add(a).build());
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
            assertThat(graph(allExpander(a, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(EMPTY);
            assertThat(graph(allExpander(a, true, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(a).add(b, c).build());
            assertThat(graph(allExpander(a, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b, c).build());
            assertThat(graph(allExpander(a, true, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(a).add(b, c).add(d, e).build());
            assertThat(graph(allExpander(a, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b, c).add(d, e).build());
            assertThat(graph(allExpander(a, true, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(a).add(b, c).add(d, e).build());
            assertThat(graph(allExpander(a, false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b, c).add(d, e).build());
            assertThat(graph(allExpander(a, true, 4, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(a).add(b, c).add(d, e).build());
            assertThat(graph(allExpander(a, false, 4, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b, c).add(d, e).build());
            assertThat(graph(allExpander(a, true, 5, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(a).add(b, c).add(d, e).build());
            assertThat(graph(allExpander(a, false, 5, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b, c).add(d, e).add(a).build());
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
            assertThat(graph(allExpander(a, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(EMPTY);
            assertThat(graph(allExpander(a, true, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(a).add(b1, b2, b3).build());
            assertThat(graph(allExpander(a, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b1, b2, b3).build());
            assertThat(graph(allExpander(a, true, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(a).add(b1, b2, b3).add(c1, c2, c3).build());
            assertThat(graph(allExpander(a, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b1, b2, b3).add(c1, c2, c3).build());
            assertThat(graph(allExpander(a, true, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(a).add(b1, b2, b3).add(c1, c2, c3).build());
            assertThat(graph(allExpander(a, false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b1, b2, b3).add(c1, c2, c3).add(a).build());
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
            assertThat(graph(allExpander(a, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(EMPTY);
            assertThat(graph(allExpander(a, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b, c).build());
            assertThat(graph(allExpander(a, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b, c).add(d).build());
            assertThat(graph(allExpander(a, false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b, c).add(d).add(e).build());
            assertThat(graph(allExpander(a, false, 4, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b, c).add(d).add(e).add(f, a).build());
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
            assertThat(graph(allExpander(a, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(EMPTY);
            assertThat(graph(allExpander(a, true, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(a).add(b, c).build());
            assertThat(graph(allExpander(a, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b, c).build());
            assertThat(graph(allExpander(a, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b, c).add(d, e).build());
            assertThat(graph(allExpander(a, false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b, c).add(d, e).add(f, g).build());
            assertThat(graph(allExpander(a, false, 4, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b, c).add(d, e).add(f, g).build());
            assertThat(graph(allExpander(a, true, 5, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(a).add(b, c).add(d, e).add(f, g).build());
            assertThat(graph(allExpander(a, false, 5, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b, c).add(d, e).add(f, g).build());
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
            assertThat(graph(allExpander(a, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(EMPTY);
            assertThat(graph(allExpander(a, true, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(a).add(b).build());
            assertThat(graph(allExpander(a, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b).build());
            assertThat(graph(allExpander(a, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b).add(c).build());
            assertThat(graph(allExpander(a, false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b).add(c).build());
            assertThat(graph(allExpander(a, true, 4, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(a).add(b).add(c).build());
            assertThat(graph(allExpander(a, false, 4, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b).add(c).build());
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
            assertThat(graph(outgoingExpander(a, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(EMPTY);
            assertThat(graph(outgoingExpander(a, true, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(a).add(b).build());
            assertThat(graph(outgoingExpander(a, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b).build());
            assertThat(graph(outgoingExpander(a, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b).add(c).build());
            assertThat(graph(outgoingExpander(a, false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b).add(c).build());
            assertThat(graph(outgoingExpander(a, true, 4, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(a).add(b).add(c).build());
            assertThat(graph(outgoingExpander(a, false, 4, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b).add(c).build());
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
            assertThat(graph(allExpander(a, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(EMPTY);
            assertThat(graph(allExpander(a, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b).build());
            assertThat(graph(allExpander(a, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b).build());
            assertThat(graph(allExpander(a, false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b).build());
            assertThat(graph(allExpander(a, true, 4, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), true))
                    .isEqualTo(graph().add(a).add(b).build());
            assertThat(graph(allExpander(a, false, 4, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b).build());
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
            assertThat(graph(allExpander(a, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(EMPTY);
            assertThat(graph(allExpander(a, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b).build());
            assertThat(graph(allExpander(a, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b).add(a, c).build());
            assertThat(graph(allExpander(a, false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b).add(a, c).build());
            assertThat(graph(allExpander(a, false, 4, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b).add(a, c).build());
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
            assertThat(asDepthMap(allExpander(a, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .isEmpty();
            assertThat(asDepthMap(allExpander(a, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .containsOnlyKeys(List.of(b1, b2, b3));
            assertThat(asDepthMap(allExpander(a, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .containsOnlyKeys(List.of(b1, b2, b3, c1, c2, c3, c4, c5, c6, c7, c8, c9));
            assertThat(asDepthMap(allExpander(a, false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .containsOnlyKeys(List.of(b1, b2, b3, c1, c2, c3, c4, c5, c6, c7, c8, c9));
            assertThat(asDepthMap(allExpander(a, false, 4, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING)))
                    .containsOnlyKeys(List.of(b1, b2, b3, c1, c2, c3, c4, c5, c6, c7, c8, c9));
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
            assertThat(graph(allExpander(a, false, 0, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(EMPTY);
            assertThat(graph(allExpander(a, false, 1, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b, c, d, e).build());
            assertThat(graph(allExpander(a, false, 2, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b, c, d, e).build());
            assertThat(graph(allExpander(a, false, 3, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING), false))
                    .isEqualTo(graph().add(b, c, d, e).add(a).build());
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
    }

    private Map<Long, Integer> asDepthMap(BFSPruningVarExpandCursor expander) {
        Map<Long, Integer> depth = new HashMap<>();
        int prevDepth = -1;
        while (expander.next()) {
            assertThat(prevDepth).as("Ensure BFS").isLessThanOrEqualTo(expander.currentDepth());
            assertThat(depth).as("Ensure visited once").doesNotContainKey(expander.endNode());
            depth.put(expander.endNode(), expander.currentDepth());
        }
        return depth;
    }

    private static class BFSGraph {
        private final List<LongList> graph;

        private BFSGraph(List<LongList> graph) {
            this.graph = graph;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            BFSGraph bfsGraph = (BFSGraph) o;

            return graph.equals(bfsGraph.graph);
        }

        @Override
        public int hashCode() {
            return graph.hashCode();
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            for (LongList longList : graph) {
                builder.append(longList.makeString("{", ", ", "}\n"));
            }
            return builder.toString();
        }
    }

    private BFSGraphBuilder graph() {
        return new BFSGraphBuilder();
    }

    private BFSGraph graph(BFSPruningVarExpandCursor cursor, boolean includeStartNode) {
        List<LongList> graph = new ArrayList<>();
        if (includeStartNode && cursor.next()) {
            graph.add(LongLists.mutable.of(cursor.endNode()));
        }
        int d = -1;
        MutableLongList list = null;
        while (cursor.next()) {
            if (d != cursor.currentDepth() || list == null) {
                d = cursor.currentDepth();
                if (list != null) {
                    // sort nodes of same depth by node id, so equality check is not dependent on traversal order /
                    // storage format
                    list.sortThis();
                }
                list = LongLists.mutable.empty();
                graph.add(list);
            }
            list.add(cursor.endNode());
        }

        // in the above loop we will miss sorting the last list
        if (list != null) {
            list.sortThis();
        }
        return new BFSGraph(graph);
    }

    BFSGraph graph(List<Long> elements) {
        var builder = graph();
        for (Long element : elements) {
            builder.add(element);
        }
        return builder.build();
    }

    private static class BFSGraphBuilder {
        private final List<LongList> graph = new ArrayList<>();

        BFSGraphBuilder add(long... elements) {
            LongArrayList list = LongArrayList.newListWith(elements);
            list.sortThis();
            graph.add(list);
            return this;
        }

        BFSGraphBuilder add(List<Long> elements) {
            MutableLongList list = LongLists.mutable.empty();
            for (Long element : elements) {
                list.add(element);
            }
            list.sortThis();
            graph.add(list);
            return this;
        }

        BFSGraph build() {
            return new BFSGraph(graph);
        }
    }

    private static final BFSGraph EMPTY = new BFSGraph(Collections.emptyList());
}
