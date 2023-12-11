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

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.api.KernelTransaction.Type.EXPLICIT;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_ENTITY;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.eclipse.collections.api.list.primitive.IntList;
import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.factory.primitive.IntLists;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
@Timeout(value = 10)
class BFSPruningVarExpandCursorTest {
    private static final EmptyMemoryTracker NO_TRACKING = EmptyMemoryTracker.INSTANCE;

    @Inject
    private Kernel kernel;

    @Test
    void shouldDoBreadthFirstSearch() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
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
            BFSGraph expectedWithStart = graph().add(start)
                    .add(a1, a2, a3, a4, a5)
                    .add(b1, b2, b3, b4, b5)
                    .build();
            assertThat(f.cursor(OUTGOING, start).max(5).toGraph()).isEqualTo(expectedWithStart);
            assertThat(f.cursor(OUTGOING, start).toGraph()).isEqualTo(expectedWithStart);

            BFSGraph expectedWithoutStart = graph().add()
                    .add(a1, a2, a3, a4, a5)
                    .add(b1, b2, b3, b4, b5)
                    .build();
            assertThat(f.cursor(OUTGOING, start).excludeStart().max(5).toGraph())
                    .isEqualTo(expectedWithoutStart);
            assertThat(f.cursor(OUTGOING, start).excludeStart().toGraph()).isEqualTo(expectedWithoutStart);

            BFSGraph expectedInto = nodeAtDepth(b3, 3);

            assertThat(f.cursor(OUTGOING, start).max(5).into(b3).toGraph()).isEqualTo(expectedInto);
            assertThat(f.cursor(OUTGOING, start).into(b3).toGraph()).isEqualTo(expectedInto);
            assertThat(f.cursor(OUTGOING, start).excludeStart().max(5).into(b3).toGraph())
                    .isEqualTo(expectedInto);
            assertThat(f.cursor(OUTGOING, start).excludeStart().into(b3).toGraph())
                    .isEqualTo(expectedInto);
        }
    }

    @Test
    void shouldDoBreadthFirstSearchUndirected() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
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
            BFSGraph expectedWithoutStart = graph().add()
                    .add(a1, a2, a3, a4, a5)
                    .add(b1, b2, b3, b4, b5)
                    .build();
            assertThat(f.cursor(BOTH, start).excludeStart().max(5).toGraph()).isEqualTo(expectedWithoutStart);
            assertThat(f.cursor(BOTH, start).excludeStart().toGraph()).isEqualTo(expectedWithoutStart);

            BFSGraph expectedWithStart = graph().add(start)
                    .add(a1, a2, a3, a4, a5)
                    .add(b1, b2, b3, b4, b5)
                    .build();
            assertThat(f.cursor(BOTH, start).max(5).toGraph()).isEqualTo(expectedWithStart);
            assertThat(f.cursor(BOTH, start).toGraph()).isEqualTo(expectedWithStart);

            BFSGraph expectedInto = nodeAtDepth(b3, 3);

            assertThat(f.cursor(BOTH, start).into(b3).excludeStart().max(5).toGraph())
                    .isEqualTo(expectedInto);
            assertThat(f.cursor(BOTH, start).into(b3).excludeStart().toGraph()).isEqualTo(expectedInto);
            assertThat(f.cursor(BOTH, start).into(b3).max(5).toGraph()).isEqualTo(expectedInto);
            assertThat(f.cursor(BOTH, start).into(b3).toGraph()).isEqualTo(expectedInto);
        }
    }

    @Test
    void shouldDoBreadthFirstSearchWithNodePredicate() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
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
            assertThat(f.cursor(OUTGOING, start).max(26).nodePred(n -> n != a3).toGraph())
                    .isEqualTo(graph().add(start)
                            .add(a1, a2, a4, a5)
                            .add(b1, b2, b4, b5)
                            .build());

            assertThat(f.cursor(OUTGOING, start)
                            .max(26)
                            .nodePred(n -> n != a3)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(graph().add()
                            .add(a1, a2, a4, a5)
                            .add(b1, b2, b4, b5)
                            .build());

            assertThat(f.cursor(OUTGOING, start)
                            .max(26)
                            .nodePred(n -> n != a3)
                            .into(b4)
                            .toGraph())
                    .isEqualTo(nodeAtDepth(b4, 3));

            assertThat(f.cursor(OUTGOING, start)
                            .max(26)
                            .nodePred(n -> n != a3)
                            .into(b4)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(nodeAtDepth(b4, 3));
        }
    }

    @Test
    void shouldDoBreadthFirstSearchWithNodePredicateUndirected() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
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
            assertThat(f.cursor(BOTH, start).max(26).nodePred(n -> n != a3).toGraph())
                    .isEqualTo(graph().add(start)
                            .add(a1, a2, a4, a5)
                            .add(b1, b2, b4, b5)
                            .build());

            assertThat(f.cursor(BOTH, start)
                            .max(26)
                            .nodePred(n -> n != a3)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(graph().add()
                            .add(a1, a2, a4, a5)
                            .add(b1, b2, b4, b5)
                            .build());

            assertThat(f.cursor(BOTH, start)
                            .max(26)
                            .nodePred(n -> n != a3)
                            .into(b4)
                            .toGraph())
                    .isEqualTo(nodeAtDepth(b4, 3));

            assertThat(f.cursor(BOTH, start)
                            .max(26)
                            .nodePred(n -> n != a3)
                            .into(b4)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(nodeAtDepth(b4, 3));
        }
    }

    @Test
    void shouldDoBreadthFirstSearchWithRelationshipPredicate() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
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

            Predicate<RelationshipTraversalCursor> relPred = cursor ->
                    cursor.relationshipReference() != filterThis && cursor.relationshipReference() != andFilterThat;

            // then
            assertThat(f.cursor(OUTGOING, start).max(26).relPred(relPred).toGraph())
                    .isEqualTo(graph().add(start)
                            .add(a1, a2, a3, a5)
                            .add(b1, b3, b5)
                            .build());

            assertThat(f.cursor(OUTGOING, start)
                            .max(26)
                            .relPred(relPred)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(graph().add().add(a1, a2, a3, a5).add(b1, b3, b5).build());

            assertThat(f.cursor(OUTGOING, start)
                            .max(26)
                            .relPred(relPred)
                            .into(b3)
                            .toGraph())
                    .isEqualTo(nodeAtDepth(b3, 3));

            assertThat(f.cursor(OUTGOING, start)
                            .max(26)
                            .relPred(relPred)
                            .into(b3)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(nodeAtDepth(b3, 3));
        }
    }

    @Test
    void shouldDoBreadthFirstSearchWithRelationshipPredicateUndirected() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
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

            Predicate<RelationshipTraversalCursor> relPred = cursor ->
                    cursor.relationshipReference() != filterThis && cursor.relationshipReference() != andFilterThat;

            // then
            assertThat(f.cursor(BOTH, start).max(26).relPred(relPred).toGraph())
                    .isEqualTo(graph().add(start)
                            .add(a1, a2, a3, a5)
                            .add(b1, b3, b5)
                            .build());

            assertThat(f.cursor(BOTH, start)
                            .max(26)
                            .relPred(relPred)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(graph().add().add(a1, a2, a3, a5).add(b1, b3, b5).build());

            assertThat(f.cursor(BOTH, start).max(26).relPred(relPred).into(b3).toGraph())
                    .isEqualTo(nodeAtDepth(b3, 3));

            assertThat(f.cursor(BOTH, start)
                            .max(26)
                            .relPred(relPred)
                            .into(b3)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(nodeAtDepth(b3, 3));
        }
    }

    @Test
    void shouldOnlyTakeShortestPathBetweenNodes() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
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
            var expander = f.cursor(OUTGOING, start).max(5).excludeStart().build();
            var expanderWithMaxDepth = f.cursor(OUTGOING, start).excludeStart().build();

            // then
            BFSGraph expected =
                    graph().add().add(a1, b1).add(a2, b2).add(b3, end).build();
            assertThat(graph(expander)).isEqualTo(expected);
            assertThat(graph(expanderWithMaxDepth)).isEqualTo(expected);
        }
    }

    @Test
    void shouldExpandOutgoing() throws KernelException {
        // given
        var graph = circleGraph(10);
        try (var f = new Fixture()) {
            // then
            assertThat(f.cursor(OUTGOING, graph.startNode()).max(3).toGraph())
                    .isEqualTo(graph(graph.nodes.subList(0, 4)));
            assertThat(f.cursor(OUTGOING, graph.startNode())
                            .max(3)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(graphWithEmptyZeroFrontier(graph.nodes.subList(1, 4)));

            var target = graph.nodes.get(3);

            assertThat(f.cursor(OUTGOING, graph.startNode()).max(3).into(target).toGraph())
                    .isEqualTo(nodeAtDepth(target, 4));
            assertThat(f.cursor(OUTGOING, graph.startNode())
                            .max(3)
                            .into(target)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(nodeAtDepth(target, 4));
        }
    }

    @Test
    void shouldExpandIncoming() throws KernelException {
        // given
        var graph = circleGraph(10);
        try (var f = new Fixture()) {
            // then
            assertThat(f.cursor(INCOMING, graph.startNode()).max(3).toGraph())
                    .isEqualTo(graph().add(graph.startNode())
                            .add(graph.nodes.get(9))
                            .add(graph.nodes.get(8))
                            .add(graph.nodes.get(7))
                            .build());
            assertThat(f.cursor(INCOMING, graph.startNode())
                            .max(3)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(graph().add()
                            .add(graph.nodes.get(9))
                            .add(graph.nodes.get(8))
                            .add(graph.nodes.get(7))
                            .build());

            var target = graph.nodes.get(7);

            assertThat(f.cursor(INCOMING, graph.startNode()).max(3).into(target).toGraph())
                    .isEqualTo(nodeAtDepth(graph.nodes.get(7), 4));
            assertThat(f.cursor(INCOMING, graph.startNode())
                            .max(3)
                            .into(target)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(nodeAtDepth(graph.nodes.get(7), 4));
        }
    }

    @Test
    void shouldExpandAll() throws KernelException {
        // given
        var graph = circleGraph(10);
        try (var f = new Fixture()) {

            // then
            assertThat(f.cursor(BOTH, graph.startNode()).max(3).toGraph())
                    .isEqualTo(graph().add(graph.startNode())
                            .add(graph.nodes.get(1), graph.nodes.get(9))
                            .add(graph.nodes.get(2), graph.nodes.get(8))
                            .add(graph.nodes.get(3), graph.nodes.get(7))
                            .build());
            assertThat(f.cursor(BOTH, graph.startNode()).max(3).excludeStart().toGraph())
                    .isEqualTo(graph().add()
                            .add(graph.nodes.get(1), graph.nodes.get(9))
                            .add(graph.nodes.get(2), graph.nodes.get(8))
                            .add(graph.nodes.get(3), graph.nodes.get(7))
                            .build());

            var target = graph.nodes.get(7);

            assertThat(f.cursor(BOTH, graph.startNode()).max(3).into(target).toGraph())
                    .isEqualTo(nodeAtDepth(graph.nodes.get(7), 4));
            assertThat(f.cursor(BOTH, graph.startNode())
                            .max(3)
                            .into(target)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(nodeAtDepth(graph.nodes.get(7), 4));
        }
    }

    @Test
    void shouldRespectTypesOutgoing() throws KernelException {
        // given
        try (var f = new Fixture()) {
            // given
            Write write = f.tx.dataWrite();
            TokenWrite tokenWrite = f.tx.tokenWrite();
            int type1 = tokenWrite.relationshipTypeGetOrCreateForName("R1");
            int type2 = tokenWrite.relationshipTypeGetOrCreateForName("R2");
            long start = write.nodeCreate();
            long end = write.nodeCreate();
            write.relationshipCreate(start, type1, end);
            write.relationshipCreate(start, type2, write.nodeCreate());

            // then
            assertThat(f.cursor(OUTGOING, start).types(type1).max(3).toGraph())
                    .isEqualTo(graph().add(start).add(end).build());
            assertThat(f.cursor(OUTGOING, start)
                            .types(type1)
                            .max(3)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(graph().add().add(end).build());

            assertThat(f.cursor(OUTGOING, start).types(type1).max(3).into(end).toGraph())
                    .isEqualTo(nodeAtDepth(end, 2));
            assertThat(f.cursor(OUTGOING, start)
                            .types(type1)
                            .max(3)
                            .into(end)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(nodeAtDepth(end, 2));
        }
    }

    @Test
    void shouldRespectTypesIncoming() throws KernelException {
        // given
        try (var f = new Fixture()) {
            // given
            Write write = f.tx.dataWrite();
            TokenWrite tokenWrite = f.tx.tokenWrite();
            int type1 = tokenWrite.relationshipTypeGetOrCreateForName("R1");
            int type2 = tokenWrite.relationshipTypeGetOrCreateForName("R2");
            long start = write.nodeCreate();
            long end = write.nodeCreate();
            write.relationshipCreate(end, type1, start);
            write.relationshipCreate(write.nodeCreate(), type2, start);

            // then
            assertThat(f.cursor(INCOMING, start).types(type1).max(3).toGraph())
                    .isEqualTo(graph().add(start).add(end).build());
            assertThat(f.cursor(INCOMING, start)
                            .types(type1)
                            .max(3)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(graph().add().add(end).build());

            assertThat(f.cursor(INCOMING, start).types(type1).max(3).into(end).toGraph())
                    .isEqualTo(nodeAtDepth(end, 2));
            assertThat(f.cursor(INCOMING, start)
                            .types(type1)
                            .max(3)
                            .excludeStart()
                            .into(end)
                            .toGraph())
                    .isEqualTo(nodeAtDepth(end, 2));
        }
    }

    @Test
    void shouldRespectTypesAll() throws KernelException {
        // given
        try (var f = new Fixture()) {
            // given
            Write write = f.tx.dataWrite();
            TokenWrite tokenWrite = f.tx.tokenWrite();
            int type1 = tokenWrite.relationshipTypeGetOrCreateForName("R1");
            int type2 = tokenWrite.relationshipTypeGetOrCreateForName("R2");
            long start = write.nodeCreate();
            long end = write.nodeCreate();
            write.relationshipCreate(start, type1, end);
            write.relationshipCreate(start, type2, write.nodeCreate());

            // then
            assertThat(f.cursor(BOTH, start).types(type1).max(3).toGraph())
                    .isEqualTo(graph().add(start).add(end).build());
            assertThat(f.cursor(BOTH, start).types(type1).max(3).excludeStart().toGraph())
                    .isEqualTo(graph().add().add(end).build());

            assertThat(f.cursor(BOTH, start).types(type1).max(3).into(end).toGraph())
                    .isEqualTo(nodeAtDepth(end, 2));
            assertThat(f.cursor(BOTH, start)
                            .types(type1)
                            .max(3)
                            .into(end)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(nodeAtDepth(end, 2));
        }
    }

    @Test
    void shouldExpandWithLength0() throws KernelException {
        // given
        var graph = circleGraph(10);
        try (var f = new Fixture()) {
            // then
            for (var dir : Direction.values()) {
                assertThat(f.cursor(dir, graph.startNode()).max(0).toGraph())
                        .isEqualTo(graph().add(graph.startNode()).build());
                assertThat(f.cursor(dir, graph.startNode())
                                .max(0)
                                .excludeStart()
                                .toGraph())
                        .isEqualTo(EMPTY);
                assertThat(f.cursor(dir, graph.startNode())
                                .max(0)
                                .into(graph.startNode())
                                .toGraph())
                        .isEqualTo(graph().add(graph.startNode()).build());
                assertThat(f.cursor(dir, graph.startNode())
                                .max(0)
                                .into(graph.startNode())
                                .excludeStart()
                                .toGraph())
                        .isEqualTo(EMPTY);
            }
        }
    }

    @Test
    void endNodesAreUnique() throws KernelException {
        // given
        try (var f = new Fixture()) {
            // given
            Write write = f.tx.dataWrite();
            TokenWrite tokenWrite = f.tx.tokenWrite();
            int type = tokenWrite.relationshipTypeGetOrCreateForName("R1");
            long start = write.nodeCreate();
            long middleNode = write.nodeCreate();
            long end = write.nodeCreate();
            write.relationshipCreate(start, type, end);
            write.relationshipCreate(start, type, middleNode);
            write.relationshipCreate(middleNode, type, end);

            // then
            assertThat(graph(f.cursor(OUTGOING, start)
                            .types(type)
                            .excludeStart()
                            .max(3)
                            .build()))
                    .isEqualTo(graph().add().add(middleNode, end).build());

            assertThat(graph(f.cursor(OUTGOING, start)
                            .types(type)
                            .excludeStart()
                            .max(3)
                            .into(end)
                            .build()))
                    .isEqualTo(nodeAtDepth(end, 2));
        }
    }

    @Test
    void shouldTraverseFullGraph() throws KernelException {
        // given
        var graph = fanOutGraph(3, 3);
        try (var f = new Fixture()) {

            // then
            assertThat(f.cursor(OUTGOING, graph.startNode()).max(3).toGraph())
                    .isEqualTo(graph().add(graph.nodes.get(0))
                            .add(graph.nodes.subList(1, 4))
                            .add(graph.nodes.subList(4, 13))
                            .add(graph.nodes.subList(13, 40))
                            .build());
            assertThat(f.cursor(OUTGOING, graph.startNode())
                            .max(3)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(graph().add()
                            .add(graph.nodes.subList(1, 4))
                            .add(graph.nodes.subList(4, 13))
                            .add(graph.nodes.subList(13, 40))
                            .build());

            assertThat(f.cursor(OUTGOING, graph.startNode())
                            .max(3)
                            .into(graph.nodes.get(39))
                            .toGraph())
                    .isEqualTo(nodeAtDepth(graph.nodes.get(39), 4));
            assertThat(f.cursor(OUTGOING, graph.startNode())
                            .max(3)
                            .into(graph.nodes.get(39))
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(nodeAtDepth(graph.nodes.get(39), 4));
        }
    }

    @Test
    void shouldStopAtSpecifiedDepth() throws KernelException {
        // given
        var graph = fanOutGraph(2, 5);
        try (var f = new Fixture()) {
            // when

            // then
            assertThat(graph(f.cursor(OUTGOING, graph.startNode())
                            .max(3)
                            .excludeStart()
                            .build()))
                    .isEqualTo(graph().add()
                            .add(graph.nodes.get(1), graph.nodes.get(2))
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

            // then
            assertThat(graph(f.cursor(OUTGOING, graph.startNode())
                            .max(3)
                            .excludeStart()
                            .into(graph.nodes.get(14))
                            .build()))
                    .isEqualTo(nodeAtDepth(graph.nodes.get(14), 4));
        }
    }

    @Test
    void shouldSatisfyPredicateOnNodes() throws KernelException {
        // given
        var graph = circleGraph(100);
        try (var f = new Fixture()) {
            // then
            assertThat(f.cursor(OUTGOING, graph.startNode())
                            .max(11)
                            .nodePred(value -> value <= graph.nodes.get(5))
                            .toGraph())
                    .isEqualTo(graph(graph.nodes.subList(0, 6)));
            assertThat(f.cursor(OUTGOING, graph.startNode())
                            .max(11)
                            .nodePred(value -> value <= graph.nodes.get(5))
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(graphWithEmptyZeroFrontier(graph.nodes.subList(1, 6)));

            assertThat(f.cursor(OUTGOING, graph.startNode())
                            .max(11)
                            .nodePred(value -> value <= graph.nodes.get(5))
                            .into(graph.nodes.get(5))
                            .toGraph())
                    .isEqualTo(graph().add()
                            .add()
                            .add()
                            .add()
                            .add()
                            .add(graph.nodes.get(5))
                            .build());
            assertThat(f.cursor(OUTGOING, graph.startNode())
                            .max(11)
                            .nodePred(value -> value <= graph.nodes.get(5))
                            .into(graph.nodes.get(5))
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(graph().add()
                            .add()
                            .add()
                            .add()
                            .add()
                            .add(graph.nodes.get(5))
                            .build());
        }
    }

    @Test
    void shouldSatisfyPredicateOnRelationships() throws KernelException {
        // given
        var graph = circleGraph(100);
        try (var f = new Fixture()) {
            // then
            assertThat(f.cursor(OUTGOING, graph.startNode())
                            .max(11)
                            .relPred(cursor -> cursor.relationshipReference() < graph.relationships.get(9))
                            .toGraph())
                    .isEqualTo(graph(graph.nodes.subList(0, 10)));
            assertThat(f.cursor(OUTGOING, graph.startNode())
                            .max(11)
                            .relPred(cursor -> cursor.relationshipReference() < graph.relationships.get(9))
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(graphWithEmptyZeroFrontier(graph.nodes.subList(1, 10)));

            assertThat(f.cursor(OUTGOING, graph.startNode())
                            .max(11)
                            .relPred(cursor -> cursor.relationshipReference() < graph.relationships.get(9))
                            .into(graph.nodes.get(5))
                            .toGraph())
                    .isEqualTo(graph().add()
                            .add()
                            .add()
                            .add()
                            .add()
                            .add(graph.nodes.get(5))
                            .build());
            assertThat(f.cursor(OUTGOING, graph.startNode())
                            .max(11)
                            .relPred(cursor -> cursor.relationshipReference() < graph.relationships.get(9))
                            .into(graph.nodes.get(5))
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(graph().add()
                            .add()
                            .add()
                            .add()
                            .add()
                            .add(graph.nodes.get(5))
                            .build());
        }
    }

    @Test
    void shouldHandleSimpleLoopOutgoing() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            long node1 = write.nodeCreate();
            long node2 = write.nodeCreate();
            write.relationshipCreate(node1, rel, node2);
            write.relationshipCreate(node2, rel, node1);

            // then
            assertThat(f.cursor(OUTGOING, node1).max(2).toGraph())
                    .isEqualTo(graph().add(node1).add(node2).build());
            assertThat(f.cursor(OUTGOING, node1).toGraph())
                    .isEqualTo(graph().add(node1).add(node2).build());
            assertThat(f.cursor(OUTGOING, node1).max(2).excludeStart().toGraph())
                    .isEqualTo(graph().add().add(node2).add(node1).build());
            assertThat(f.cursor(OUTGOING, node1).excludeStart().toGraph())
                    .isEqualTo(graph().add().add(node2).add(node1).build());

            assertThat(f.cursor(OUTGOING, node1).max(2).into(node2).toGraph())
                    .isEqualTo(graph().add().add(node2).build());
            assertThat(f.cursor(OUTGOING, node1).into(node2).toGraph())
                    .isEqualTo(graph().add().add(node2).build());
            assertThat(f.cursor(OUTGOING, node1)
                            .max(2)
                            .excludeStart()
                            .into(node1)
                            .toGraph())
                    .isEqualTo(nodeAtDepth(node1, 3));
            assertThat(f.cursor(OUTGOING, node1).excludeStart().into(node1).toGraph())
                    .isEqualTo(nodeAtDepth(node1, 3));
        }
    }

    // tests targeting undirected searches
    @Test
    void shouldNotRetraceSteps() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            long node1 = write.nodeCreate();
            long node2 = write.nodeCreate();
            write.relationshipCreate(node1, rel, node2);

            // then
            assertThat(f.cursor(BOTH, node1).excludeStart().max(0).toGraph()).isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, node1).max(1).toGraph())
                    .isEqualTo(graph().add(node1).add(node2).build());
            assertThat(f.cursor(BOTH, node1).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(node2).build());
            assertThat(f.cursor(BOTH, node1).max(2).toGraph())
                    .isEqualTo(graph().add(node1).add(node2).build());
            assertThat(f.cursor(BOTH, node1).excludeStart().max(2).toGraph())
                    .isEqualTo(graph().add().add(node2).build());

            assertThat(f.cursor(BOTH, node1).excludeStart().max(0).into(node1).toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, node1).max(1).into(node2).toGraph())
                    .isEqualTo(graph().add().add(node2).build());
            assertThat(f.cursor(BOTH, node1).excludeStart().max(1).into(node2).toGraph())
                    .isEqualTo(graph().add().add(node2).build());
            assertThat(f.cursor(BOTH, node1).max(2).into(node2).toGraph())
                    .isEqualTo(graph().add().add(node2).build());
            assertThat(f.cursor(BOTH, node1).excludeStart().max(2).into(node2).toGraph())
                    .isEqualTo(graph().add().add(node2).build());
        }
    }

    @Test
    void shouldHandleSingleSelfLoop() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            long node1 = write.nodeCreate();
            write.relationshipCreate(node1, rel, node1);

            // then
            assertThat(f.cursor(BOTH, node1).excludeStart().max(0).toGraph()).isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, node1).max(1).toGraph())
                    .isEqualTo(graph().add(node1).build());
            assertThat(f.cursor(BOTH, node1).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(node1).build());
            assertThat(f.cursor(BOTH, node1).max(2).toGraph())
                    .isEqualTo(graph().add(node1).build());
            assertThat(f.cursor(BOTH, node1).excludeStart().max(2).toGraph())
                    .isEqualTo(graph().add().add(node1).build());
            assertThat(f.cursor(BOTH, node1).toGraph())
                    .isEqualTo(graph().add(node1).build());
            assertThat(f.cursor(BOTH, node1).excludeStart().toGraph())
                    .isEqualTo(graph().add().add(node1).build());

            assertThat(f.cursor(BOTH, node1).into(node1).excludeStart().max(0).toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, node1).into(node1).max(1).toGraph())
                    .isEqualTo(graph().add(node1).build());
            assertThat(f.cursor(BOTH, node1).into(node1).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(node1).build());
            assertThat(f.cursor(BOTH, node1).into(node1).max(2).toGraph())
                    .isEqualTo(graph().add(node1).build());
            assertThat(f.cursor(BOTH, node1).into(node1).excludeStart().max(2).toGraph())
                    .isEqualTo(graph().add().add(node1).build());
            assertThat(f.cursor(BOTH, node1).into(node1).toGraph())
                    .isEqualTo(graph().add(node1).build());
            assertThat(f.cursor(BOTH, node1).into(node1).excludeStart().toGraph())
                    .isEqualTo(graph().add().add(node1).build());
        }
    }

    @Test
    void shouldHandleSimpleLoop() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            long node1 = write.nodeCreate();
            long node2 = write.nodeCreate();
            write.relationshipCreate(node1, rel, node2);
            write.relationshipCreate(node2, rel, node1);

            // then
            assertThat(f.cursor(BOTH, node1).excludeStart().max(0).toGraph()).isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, node1).max(1).toGraph())
                    .isEqualTo(graph().add(node1).add(node2).build());
            assertThat(f.cursor(BOTH, node1).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(node2).build());
            assertThat(f.cursor(BOTH, node1).max(2).toGraph())
                    .isEqualTo(graph().add(node1).add(node2).build());
            assertThat(f.cursor(BOTH, node1).excludeStart().max(2).toGraph())
                    .isEqualTo(graph().add().add(node2).add(node1).build());
            assertThat(f.cursor(BOTH, node1).toGraph())
                    .isEqualTo(graph().add(node1).add(node2).build());
            assertThat(f.cursor(BOTH, node1).excludeStart().toGraph())
                    .isEqualTo(graph().add().add(node2).add(node1).build());

            assertThat(f.cursor(BOTH, node1).into(node2).excludeStart().max(0).toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, node1).into(node2).max(1).toGraph())
                    .isEqualTo(graph().add().add(node2).build());
            assertThat(f.cursor(BOTH, node1).into(node2).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(node2).build());
            assertThat(f.cursor(BOTH, node1).into(node2).max(2).toGraph())
                    .isEqualTo(graph().add().add(node2).build());
            assertThat(f.cursor(BOTH, node1).into(node1).excludeStart().max(2).toGraph())
                    .isEqualTo(nodeAtDepth(node1, 3));
            assertThat(f.cursor(BOTH, node1).into(node2).toGraph())
                    .isEqualTo(graph().add().add(node2).build());
            assertThat(f.cursor(BOTH, node1).into(node1).excludeStart().toGraph())
                    .isEqualTo(nodeAtDepth(node1, 3));
        }
    }

    @Test
    void shouldHandleSimpleLoopWithPredicate() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            long node1 = write.nodeCreate();
            long node2 = write.nodeCreate();
            write.relationshipCreate(node1, rel, node2);
            long dontUse = write.relationshipCreate(node2, rel, node1);

            // then
            assertThat(f.cursor(BOTH, node1)
                            .max(2)
                            .relPred(cursor -> cursor.relationshipReference() != dontUse)
                            .toGraph())
                    .isEqualTo(graph().add(node1).add(node2).build());
            assertThat(f.cursor(BOTH, node1)
                            .excludeStart()
                            .max(2)
                            .relPred(cursor -> cursor.relationshipReference() != dontUse)
                            .toGraph())
                    .isEqualTo(graph().add().add(node2).build());

            assertThat(f.cursor(BOTH, node1)
                            .into(node2)
                            .max(2)
                            .relPred(cursor -> cursor.relationshipReference() != dontUse)
                            .toGraph())
                    .isEqualTo(nodeAtDepth(node2, 2));
            assertThat(f.cursor(BOTH, node1)
                            .into(node2)
                            .excludeStart()
                            .max(2)
                            .relPred(cursor -> cursor.relationshipReference() != dontUse)
                            .toGraph())
                    .isEqualTo(nodeAtDepth(node2, 2));
        }
    }

    @Test
    void shouldHandleSimpleTriangularPattern() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
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
            assertThat(f.cursor(BOTH, a).excludeStart().max(0).toGraph()).isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, a).max(1).toGraph())
                    .isEqualTo(graph().add(a).add(b, c).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(b, c).build());
            assertThat(f.cursor(BOTH, a).max(2).toGraph())
                    .isEqualTo(graph().add(a).add(b, c).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(2).toGraph())
                    .isEqualTo(graph().add().add(b, c).build());

            assertThat(f.cursor(BOTH, a).into(c).excludeStart().max(0).toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, a).into(c).max(1).toGraph()).isEqualTo(nodeAtDepth(c, 2));
            assertThat(f.cursor(BOTH, a).into(c).excludeStart().max(1).toGraph())
                    .isEqualTo(nodeAtDepth(c, 2));
            assertThat(f.cursor(BOTH, a).into(c).max(2).toGraph()).isEqualTo(nodeAtDepth(c, 2));
            assertThat(f.cursor(BOTH, a).into(c).excludeStart().max(2).toGraph())
                    .isEqualTo(nodeAtDepth(c, 2));
        }
    }

    @Test
    void shouldHandleSimpleTriangularPatternWithBackTrace() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
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
            assertThat(f.cursor(BOTH, a).max(0).toGraph())
                    .isEqualTo(graph().add(a).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(0).toGraph()).isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, a).max(1).toGraph())
                    .isEqualTo(graph().add(a).add(b, c).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(b, c).build());
            assertThat(f.cursor(BOTH, a).max(2).toGraph())
                    .isEqualTo(graph().add(a).add(b, c).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(2).toGraph())
                    .isEqualTo(graph().add().add(b, c).add(a).build());

            assertThat(f.cursor(BOTH, a).into(a).max(0).toGraph())
                    .isEqualTo(graph().add(a).build());
            assertThat(f.cursor(BOTH, a).into(a).excludeStart().max(0).toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, a).into(c).max(1).toGraph()).isEqualTo(nodeAtDepth(c, 2));
            assertThat(f.cursor(BOTH, a).into(c).excludeStart().max(1).toGraph())
                    .isEqualTo(nodeAtDepth(c, 2));
            assertThat(f.cursor(BOTH, a).into(c).max(2).toGraph()).isEqualTo(nodeAtDepth(c, 2));
            assertThat(f.cursor(BOTH, a).into(c).excludeStart().max(2).toGraph())
                    .isEqualTo(nodeAtDepth(c, 2));
        }
    }

    @Test
    void shouldHandleSimpleTriangularPatternWithBackTrace2() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
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
            assertThat(f.cursor(BOTH, a).excludeStart().max(0).toGraph()).isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, a).max(1).toGraph())
                    .isEqualTo(graph().add(a).add(b, c).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(b, c).build());
            assertThat(f.cursor(BOTH, a).max(2).toGraph())
                    .isEqualTo(graph().add(a).add(b, c).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(2).toGraph())
                    .isEqualTo(graph().add().add(b, c).add(a).build());

            assertThat(f.cursor(BOTH, a).into(c).excludeStart().max(0).toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, a).into(c).max(1).toGraph()).isEqualTo(nodeAtDepth(c, 2));
            assertThat(f.cursor(BOTH, a).into(c).excludeStart().max(1).toGraph())
                    .isEqualTo(nodeAtDepth(c, 2));
            assertThat(f.cursor(BOTH, a).into(c).max(2).toGraph()).isEqualTo(nodeAtDepth(c, 2));
            assertThat(f.cursor(BOTH, a).into(c).excludeStart().max(2).toGraph())
                    .isEqualTo(nodeAtDepth(c, 2));
        }
    }

    @Test
    void shouldHandleTriangularLoop() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
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
            assertThat(f.cursor(BOTH, a).excludeStart().max(0).toGraph()).isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, a).max(1).toGraph())
                    .isEqualTo(graph().add(a).add(b, c).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(b, c).build());
            assertThat(f.cursor(BOTH, a).max(2).toGraph())
                    .isEqualTo(graph().add(a).add(b, c).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(2).toGraph())
                    .isEqualTo(graph().add().add(b, c).build());
            assertThat(f.cursor(BOTH, a).max(3).toGraph())
                    .isEqualTo(graph().add(a).add(b, c).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(3).toGraph())
                    .isEqualTo(graph().add().add(b, c).add().add(a).build());

            assertThat(f.cursor(BOTH, a).into(c).excludeStart().max(0).toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, a).into(c).max(1).toGraph()).isEqualTo(nodeAtDepth(c, 2));
            assertThat(f.cursor(BOTH, a).into(c).excludeStart().max(1).toGraph())
                    .isEqualTo(nodeAtDepth(c, 2));
            assertThat(f.cursor(BOTH, a).into(c).max(2).toGraph()).isEqualTo(nodeAtDepth(c, 2));
            assertThat(f.cursor(BOTH, a).into(c).excludeStart().max(2).toGraph())
                    .isEqualTo(nodeAtDepth(c, 2));
            assertThat(f.cursor(BOTH, a).into(c).max(3).toGraph()).isEqualTo(nodeAtDepth(c, 2));
            assertThat(f.cursor(BOTH, a).into(a).excludeStart().max(3).toGraph())
                    .isEqualTo(nodeAtDepth(a, 4));
        }
    }

    @Test
    void shouldHandleTriangularLoop2() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //            (b)
            //           / |
            // (start)-(a) |
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
            assertThat(f.cursor(BOTH, start).excludeStart().max(0).toGraph()).isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, start).max(1).toGraph())
                    .isEqualTo(graph().add(start).add(a).build());
            assertThat(f.cursor(BOTH, start).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(a).build());
            assertThat(f.cursor(BOTH, start).max(2).toGraph())
                    .isEqualTo(graph().add(start).add(a).add(b, c).build());
            assertThat(f.cursor(BOTH, start).excludeStart().max(2).toGraph())
                    .isEqualTo(graph().add().add(a).add(b, c).build());
            assertThat(f.cursor(BOTH, start).max(3).toGraph())
                    .isEqualTo(graph().add(start).add(a).add(b, c).build());
            assertThat(f.cursor(BOTH, start).excludeStart().max(3).toGraph())
                    .isEqualTo(graph().add().add(a).add(b, c).build());
            assertThat(f.cursor(BOTH, start).max(4).toGraph())
                    .isEqualTo(graph().add(start).add(a).add(b, c).build());
            assertThat(f.cursor(BOTH, start).excludeStart().max(4).toGraph())
                    .isEqualTo(graph().add().add(a).add(b, c).build());
            assertThat(f.cursor(BOTH, start).max(5).toGraph())
                    .isEqualTo(graph().add(start).add(a).add(b, c).build());
            assertThat(f.cursor(BOTH, start).excludeStart().max(5).toGraph())
                    .isEqualTo(graph().add().add(a).add(b, c).build());
            assertThat(f.cursor(BOTH, start).toGraph())
                    .isEqualTo(graph().add(start).add(a).add(b, c).build());
            assertThat(f.cursor(BOTH, start).excludeStart().toGraph())
                    .isEqualTo(graph().add().add(a).add(b, c).build());

            assertThat(f.cursor(BOTH, start).into(c).excludeStart().max(0).toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, start).into(a).max(1).toGraph())
                    .isEqualTo(graph().add().add(a).build());
            assertThat(f.cursor(BOTH, start).into(a).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(a).build());
            assertThat(f.cursor(BOTH, start).into(c).max(2).toGraph()).isEqualTo(nodeAtDepth(c, 3));
            assertThat(f.cursor(BOTH, start).into(c).excludeStart().max(2).toGraph())
                    .isEqualTo(nodeAtDepth(c, 3));
            assertThat(f.cursor(BOTH, start).into(c).max(3).toGraph()).isEqualTo(nodeAtDepth(c, 3));
            assertThat(f.cursor(BOTH, start).into(c).excludeStart().max(3).toGraph())
                    .isEqualTo(nodeAtDepth(c, 3));
            assertThat(f.cursor(BOTH, start).into(c).max(4).toGraph()).isEqualTo(nodeAtDepth(c, 3));
            assertThat(f.cursor(BOTH, start).into(c).excludeStart().max(4).toGraph())
                    .isEqualTo(nodeAtDepth(c, 3));
            assertThat(f.cursor(BOTH, start).into(c).max(5).toGraph()).isEqualTo(nodeAtDepth(c, 3));
            assertThat(f.cursor(BOTH, start).into(c).excludeStart().max(5).toGraph())
                    .isEqualTo(nodeAtDepth(c, 3));
            assertThat(f.cursor(BOTH, start).into(c).toGraph()).isEqualTo(nodeAtDepth(c, 3));
            assertThat(f.cursor(BOTH, start).into(c).excludeStart().toGraph()).isEqualTo(nodeAtDepth(c, 3));
        }
    }

    @Test
    void shouldHandleImpossibleSquareLoop() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");

            //         (c)-(d)
            //          |   |
            // (start)-(a)-(b)
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();
            long d = write.nodeCreate();
            write.relationshipCreate(start, rel, a);
            write.relationshipCreate(a, rel, b);
            write.relationshipCreate(a, rel, c);
            write.relationshipCreate(b, rel, d);
            write.relationshipCreate(c, rel, d);

            // then
            assertThat(f.cursor(BOTH, start).max(0).toGraph())
                    .isEqualTo(graph().add(start).build());
            assertThat(f.cursor(BOTH, start).excludeStart().max(0).toGraph()).isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, start).max(1).toGraph())
                    .isEqualTo(graph().add(start).add(a).build());
            assertThat(f.cursor(BOTH, start).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(a).build());
            assertThat(f.cursor(BOTH, start).max(2).toGraph())
                    .isEqualTo(graph().add(start).add(a).add(b, c).build());
            assertThat(f.cursor(BOTH, start).excludeStart().max(2).toGraph())
                    .isEqualTo(graph().add().add(a).add(b, c).build());
            assertThat(f.cursor(BOTH, start).max(3).toGraph())
                    .isEqualTo(graph().add(start).add(a).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, start).excludeStart().max(3).toGraph())
                    .isEqualTo(graph().add().add(a).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, start).max(4).toGraph())
                    .isEqualTo(graph().add(start).add(a).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, start).excludeStart().max(4).toGraph())
                    .isEqualTo(graph().add().add(a).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, start).max(5).toGraph())
                    .isEqualTo(graph().add(start).add(a).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, start).excludeStart().max(5).toGraph())
                    .isEqualTo(graph().add().add(a).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, start).max(6).toGraph())
                    .isEqualTo(graph().add(start).add(a).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, start).excludeStart().max(6).toGraph())
                    .isEqualTo(graph().add().add(a).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, start).toGraph())
                    .isEqualTo(graph().add(start).add(a).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, start).excludeStart().toGraph())
                    .isEqualTo(graph().add().add(a).add(b, c).add(d).build());

            assertThat(f.cursor(BOTH, start).into(start).max(0).toGraph())
                    .isEqualTo(graph().add(start).build());
            assertThat(f.cursor(BOTH, start).into(start).excludeStart().max(0).toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, start).into(a).max(1).toGraph())
                    .isEqualTo(graph().add().add(a).build());
            assertThat(f.cursor(BOTH, start).into(a).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(a).build());
            assertThat(f.cursor(BOTH, start).into(c).max(2).toGraph()).isEqualTo(nodeAtDepth(c, 3));
            assertThat(f.cursor(BOTH, start).into(c).excludeStart().max(2).toGraph())
                    .isEqualTo(nodeAtDepth(c, 3));
            assertThat(f.cursor(BOTH, start).into(d).max(3).toGraph()).isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start).into(d).excludeStart().max(3).toGraph())
                    .isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start).into(d).max(4).toGraph()).isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start).into(d).excludeStart().max(4).toGraph())
                    .isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start).into(d).max(5).toGraph()).isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start).into(d).excludeStart().max(5).toGraph())
                    .isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start).into(d).max(6).toGraph()).isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start).into(d).excludeStart().max(6).toGraph())
                    .isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start).into(d).toGraph()).isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start).into(d).excludeStart().toGraph()).isEqualTo(nodeAtDepth(d, 4));
        }
    }

    @Test
    void shouldHandleImpossibleSquareLoopWithMultipleOutgoingFromSource() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");

            //   (e)   (c)-(d)
            //    |     |   |
            // (start)-(a)-(b)
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();
            long d = write.nodeCreate();
            long e = write.nodeCreate();
            write.relationshipCreate(start, rel, a);
            write.relationshipCreate(start, rel, e);
            write.relationshipCreate(a, rel, b);
            write.relationshipCreate(a, rel, c);
            write.relationshipCreate(b, rel, d);
            write.relationshipCreate(c, rel, d);

            // then
            assertThat(f.cursor(BOTH, start).max(0).toGraph())
                    .isEqualTo(graph().add(start).build());
            assertThat(f.cursor(BOTH, start).excludeStart().max(0).toGraph()).isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, start).max(1).toGraph())
                    .isEqualTo(graph().add(start).add(a, e).build());
            assertThat(f.cursor(BOTH, start).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(a, e).build());
            assertThat(f.cursor(BOTH, start).max(2).toGraph())
                    .isEqualTo(graph().add(start).add(a, e).add(b, c).build());
            assertThat(f.cursor(BOTH, start).excludeStart().max(2).toGraph())
                    .isEqualTo(graph().add().add(a, e).add(b, c).build());
            assertThat(f.cursor(BOTH, start).max(3).toGraph())
                    .isEqualTo(graph().add(start).add(a, e).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, start).excludeStart().max(3).toGraph())
                    .isEqualTo(graph().add().add(a, e).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, start).max(4).toGraph())
                    .isEqualTo(graph().add(start).add(a, e).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, start).excludeStart().max(4).toGraph())
                    .isEqualTo(graph().add().add(a, e).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, start).max(5).toGraph())
                    .isEqualTo(graph().add(start).add(a, e).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, start).excludeStart().max(5).toGraph())
                    .isEqualTo(graph().add().add(a, e).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, start).max(6).toGraph())
                    .isEqualTo(graph().add(start).add(a, e).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, start).excludeStart().max(6).toGraph())
                    .isEqualTo(graph().add().add(a, e).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, start).toGraph())
                    .isEqualTo(graph().add(start).add(a, e).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, start).excludeStart().toGraph())
                    .isEqualTo(graph().add().add(a, e).add(b, c).add(d).build());

            assertThat(f.cursor(BOTH, start).into(start).max(0).toGraph())
                    .isEqualTo(graph().add(start).build());
            assertThat(f.cursor(BOTH, start).into(start).excludeStart().max(0).toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, start).into(e).max(1).toGraph())
                    .isEqualTo(graph().add().add(e).build());
            assertThat(f.cursor(BOTH, start).into(e).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(e).build());
            assertThat(f.cursor(BOTH, start).into(c).max(2).toGraph()).isEqualTo(nodeAtDepth(c, 3));
            assertThat(f.cursor(BOTH, start).into(c).excludeStart().max(2).toGraph())
                    .isEqualTo(nodeAtDepth(c, 3));
            assertThat(f.cursor(BOTH, start).into(d).max(3).toGraph()).isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start).into(d).excludeStart().max(3).toGraph())
                    .isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start).into(d).max(4).toGraph()).isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start).into(d).excludeStart().max(4).toGraph())
                    .isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start).into(d).max(5).toGraph()).isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start).into(d).excludeStart().max(5).toGraph())
                    .isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start).into(d).max(6).toGraph()).isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start).into(d).excludeStart().max(6).toGraph())
                    .isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start).into(d).toGraph()).isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start).into(d).excludeStart().toGraph()).isEqualTo(nodeAtDepth(d, 4));
        }
    }

    @Test
    void shouldHandleSquareLoopWhenNoFirstHopRelationshipsAreFiltered() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");

            //         (c)-(d)
            //          |   |
            // (start)=(a)-(b)
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();
            long d = write.nodeCreate();
            write.relationshipCreate(start, rel, a);
            write.relationshipCreate(start, rel, a);
            write.relationshipCreate(a, rel, b);
            write.relationshipCreate(a, rel, c);
            write.relationshipCreate(b, rel, d);
            write.relationshipCreate(c, rel, d);

            // then
            assertThat(f.cursor(BOTH, start).max(0).toGraph())
                    .isEqualTo(graph().add(start).build());
            assertThat(f.cursor(BOTH, start).excludeStart().max(0).toGraph()).isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, start).max(1).toGraph())
                    .isEqualTo(graph().add(start).add(a).build());
            assertThat(f.cursor(BOTH, start).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(a).build());
            assertThat(f.cursor(BOTH, start).max(2).toGraph())
                    .isEqualTo(graph().add(start).add(a).add(b, c).build());
            assertThat(f.cursor(BOTH, start).excludeStart().max(2).toGraph())
                    .isEqualTo(graph().add().add(a).add(start, b, c).build());
            assertThat(f.cursor(BOTH, start).max(3).toGraph())
                    .isEqualTo(graph().add(start).add(a).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, start).excludeStart().max(3).toGraph())
                    .isEqualTo(graph().add().add(a).add(start, b, c).add(d).build());
            assertThat(f.cursor(BOTH, start).max(4).toGraph())
                    .isEqualTo(graph().add(start).add(a).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, start).excludeStart().max(4).toGraph())
                    .isEqualTo(graph().add().add(a).add(start, b, c).add(d).build());
            assertThat(f.cursor(BOTH, start).max(5).toGraph())
                    .isEqualTo(graph().add(start).add(a).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, start).excludeStart().max(5).toGraph())
                    .isEqualTo(graph().add().add(a).add(start, b, c).add(d).build());
            assertThat(f.cursor(BOTH, start).max(6).toGraph())
                    .isEqualTo(graph().add(start).add(a).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, start).excludeStart().max(6).toGraph())
                    .isEqualTo(graph().add().add(a).add(start, b, c).add(d).build());
            assertThat(f.cursor(BOTH, start).toGraph())
                    .isEqualTo(graph().add(start).add(a).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, start).excludeStart().toGraph())
                    .isEqualTo(graph().add().add(a).add(start, b, c).add(d).build());

            assertThat(f.cursor(BOTH, start).into(start).max(0).toGraph())
                    .isEqualTo(graph().add(start).build());
            assertThat(f.cursor(BOTH, start).into(start).excludeStart().max(0).toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, start).into(a).max(1).toGraph())
                    .isEqualTo(graph().add().add(a).build());
            assertThat(f.cursor(BOTH, start).into(a).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(a).build());
            assertThat(f.cursor(BOTH, start).into(c).max(2).toGraph()).isEqualTo(nodeAtDepth(c, 3));
            assertThat(f.cursor(BOTH, start).into(c).excludeStart().max(2).toGraph())
                    .isEqualTo(nodeAtDepth(c, 3));
            assertThat(f.cursor(BOTH, start).into(d).max(3).toGraph()).isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start).into(d).excludeStart().max(3).toGraph())
                    .isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start).into(d).max(4).toGraph()).isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start).into(d).excludeStart().max(4).toGraph())
                    .isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start).into(d).max(5).toGraph()).isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start).into(d).excludeStart().max(5).toGraph())
                    .isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start).into(d).max(6).toGraph()).isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start).into(d).excludeStart().max(6).toGraph())
                    .isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start).into(d).toGraph()).isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start).into(d).excludeStart().toGraph()).isEqualTo(nodeAtDepth(d, 4));
        }
    }

    @Test
    void shouldHandleSquareLoopWhenAllButOneFirstHopRelationshipsAreFiltered() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");

            //         (c)-(d)
            //          |   |
            // (start)=(a)-(b)
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();
            long d = write.nodeCreate();
            long r1 = write.relationshipCreate(start, rel, a);
            write.relationshipCreate(start, rel, a);
            write.relationshipCreate(a, rel, b);
            write.relationshipCreate(a, rel, c);
            write.relationshipCreate(b, rel, d);
            write.relationshipCreate(c, rel, d);

            Predicate<RelationshipTraversalCursor> relPredicate = r -> r.relationshipReference() != r1;

            // then
            assertThat(f.cursor(BOTH, start).max(0).relPred(relPredicate).toGraph())
                    .isEqualTo(graph().add(start).build());
            assertThat(f.cursor(BOTH, start)
                            .max(0)
                            .relPred(relPredicate)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, start).max(1).relPred(relPredicate).toGraph())
                    .isEqualTo(graph().add(start).add(a).build());
            assertThat(f.cursor(BOTH, start)
                            .max(1)
                            .relPred(relPredicate)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(graph().add().add(a).build());
            assertThat(f.cursor(BOTH, start).max(2).relPred(relPredicate).toGraph())
                    .isEqualTo(graph().add(start).add(a).add(b, c).build());
            assertThat(f.cursor(BOTH, start)
                            .max(2)
                            .relPred(relPredicate)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(graph().add().add(a).add(b, c).build());
            assertThat(f.cursor(BOTH, start).max(3).relPred(relPredicate).toGraph())
                    .isEqualTo(graph().add(start).add(a).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, start)
                            .max(3)
                            .relPred(relPredicate)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(graph().add().add(a).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, start).max(4).relPred(relPredicate).toGraph())
                    .isEqualTo(graph().add(start).add(a).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, start)
                            .max(4)
                            .relPred(relPredicate)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(graph().add().add(a).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, start).max(5).relPred(relPredicate).toGraph())
                    .isEqualTo(graph().add(start).add(a).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, start)
                            .max(5)
                            .relPred(relPredicate)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(graph().add().add(a).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, start).max(6).relPred(relPredicate).toGraph())
                    .isEqualTo(graph().add(start).add(a).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, start)
                            .max(6)
                            .relPred(relPredicate)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(graph().add().add(a).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, start).relPred(relPredicate).toGraph())
                    .isEqualTo(graph().add(start).add(a).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, start)
                            .relPred(relPredicate)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(graph().add().add(a).add(b, c).add(d).build());

            assertThat(f.cursor(BOTH, start)
                            .into(start)
                            .max(0)
                            .relPred(relPredicate)
                            .toGraph())
                    .isEqualTo(graph().add(start).build());
            assertThat(f.cursor(BOTH, start)
                            .into(start)
                            .max(0)
                            .relPred(relPredicate)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, start)
                            .into(a)
                            .max(1)
                            .relPred(relPredicate)
                            .toGraph())
                    .isEqualTo(graph().add().add(a).build());
            assertThat(f.cursor(BOTH, start)
                            .into(a)
                            .max(1)
                            .relPred(relPredicate)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(graph().add().add(a).build());
            assertThat(f.cursor(BOTH, start)
                            .into(c)
                            .max(2)
                            .relPred(relPredicate)
                            .toGraph())
                    .isEqualTo(nodeAtDepth(c, 3));
            assertThat(f.cursor(BOTH, start)
                            .into(c)
                            .max(2)
                            .relPred(relPredicate)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(nodeAtDepth(c, 3));
            assertThat(f.cursor(BOTH, start)
                            .into(d)
                            .max(3)
                            .relPred(relPredicate)
                            .toGraph())
                    .isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start)
                            .into(d)
                            .max(3)
                            .relPred(relPredicate)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start)
                            .into(d)
                            .max(4)
                            .relPred(relPredicate)
                            .toGraph())
                    .isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start)
                            .into(d)
                            .max(4)
                            .relPred(relPredicate)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start)
                            .into(d)
                            .max(5)
                            .relPred(relPredicate)
                            .toGraph())
                    .isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start)
                            .into(d)
                            .max(5)
                            .relPred(relPredicate)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start)
                            .into(d)
                            .max(6)
                            .relPred(relPredicate)
                            .toGraph())
                    .isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start)
                            .into(d)
                            .max(6)
                            .relPred(relPredicate)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start).into(d).relPred(relPredicate).toGraph())
                    .isEqualTo(nodeAtDepth(d, 4));
            assertThat(f.cursor(BOTH, start)
                            .into(d)
                            .relPred(relPredicate)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(nodeAtDepth(d, 4));
        }
    }

    @Test
    void shouldHandleSquareLoopWhenAllFirstHopRelationshipsAreFiltered() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");

            //         (c)-(d)
            //          |   |
            // (start)=(a)-(b)
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();
            long d = write.nodeCreate();
            long r1 = write.relationshipCreate(start, rel, a);
            long r2 = write.relationshipCreate(start, rel, a);
            write.relationshipCreate(a, rel, b);
            write.relationshipCreate(a, rel, c);
            write.relationshipCreate(b, rel, d);
            write.relationshipCreate(c, rel, d);

            Predicate<RelationshipTraversalCursor> relPredicate =
                    r -> r.relationshipReference() != r1 && r.relationshipReference() != r2;

            // then
            assertThat(f.cursor(BOTH, start).max(0).relPred(relPredicate).toGraph())
                    .isEqualTo(graph().add(start).build());
            assertThat(f.cursor(BOTH, start)
                            .max(0)
                            .relPred(relPredicate)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, start).max(1).relPred(relPredicate).toGraph())
                    .isEqualTo(graph().add(start).build());
            assertThat(f.cursor(BOTH, start)
                            .max(1)
                            .relPred(relPredicate)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, start).max(2).relPred(relPredicate).toGraph())
                    .isEqualTo(graph().add(start).build());
            assertThat(f.cursor(BOTH, start)
                            .max(2)
                            .relPred(relPredicate)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, start).max(3).relPred(relPredicate).toGraph())
                    .isEqualTo(graph().add(start).build());
            assertThat(f.cursor(BOTH, start)
                            .max(3)
                            .relPred(relPredicate)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, start).max(4).relPred(relPredicate).toGraph())
                    .isEqualTo(graph().add(start).build());
            assertThat(f.cursor(BOTH, start)
                            .max(4)
                            .relPred(relPredicate)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, start).max(5).relPred(relPredicate).toGraph())
                    .isEqualTo(graph().add(start).build());
            assertThat(f.cursor(BOTH, start)
                            .max(5)
                            .relPred(relPredicate)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, start).max(6).relPred(relPredicate).toGraph())
                    .isEqualTo(graph().add(start).build());
            assertThat(f.cursor(BOTH, start)
                            .max(6)
                            .relPred(relPredicate)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, start).relPred(relPredicate).toGraph())
                    .isEqualTo(graph().add(start).build());
            assertThat(f.cursor(BOTH, start)
                            .relPred(relPredicate)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(EMPTY);

            assertThat(f.cursor(BOTH, start)
                            .into(start)
                            .max(0)
                            .relPred(relPredicate)
                            .toGraph())
                    .isEqualTo(graph().add(start).build());
            assertThat(f.cursor(BOTH, start)
                            .into(start)
                            .max(0)
                            .relPred(relPredicate)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, start)
                            .into(start)
                            .max(1)
                            .relPred(relPredicate)
                            .toGraph())
                    .isEqualTo(graph().add(start).build());
            assertThat(f.cursor(BOTH, start)
                            .into(start)
                            .max(1)
                            .relPred(relPredicate)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, start)
                            .into(start)
                            .max(2)
                            .relPred(relPredicate)
                            .toGraph())
                    .isEqualTo(graph().add(start).build());
            assertThat(f.cursor(BOTH, start)
                            .into(start)
                            .max(2)
                            .relPred(relPredicate)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, start)
                            .into(start)
                            .max(3)
                            .relPred(relPredicate)
                            .toGraph())
                    .isEqualTo(graph().add(start).build());
            assertThat(f.cursor(BOTH, start)
                            .into(start)
                            .max(3)
                            .relPred(relPredicate)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, start)
                            .into(start)
                            .max(4)
                            .relPred(relPredicate)
                            .toGraph())
                    .isEqualTo(graph().add(start).build());
            assertThat(f.cursor(BOTH, start)
                            .into(start)
                            .max(4)
                            .relPred(relPredicate)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, start)
                            .into(start)
                            .max(5)
                            .relPred(relPredicate)
                            .toGraph())
                    .isEqualTo(graph().add(start).build());
            assertThat(f.cursor(BOTH, start)
                            .into(start)
                            .max(5)
                            .relPred(relPredicate)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, start)
                            .into(start)
                            .max(6)
                            .relPred(relPredicate)
                            .toGraph())
                    .isEqualTo(graph().add(start).build());
            assertThat(f.cursor(BOTH, start)
                            .into(start)
                            .max(6)
                            .relPred(relPredicate)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, start).into(start).relPred(relPredicate).toGraph())
                    .isEqualTo(graph().add(start).build());
            assertThat(f.cursor(BOTH, start)
                            .into(start)
                            .relPred(relPredicate)
                            .excludeStart()
                            .toGraph())
                    .isEqualTo(EMPTY);
        }
    }

    @Test
    void shouldHandleLoopBetweenDifferentBFSLayers() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
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
            assertThat(f.cursor(BOTH, a).excludeStart().max(0).toGraph()).isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, a).max(1).toGraph())
                    .isEqualTo(graph().add(a).add(b, c).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(b, c).build());
            assertThat(f.cursor(BOTH, a).max(2).toGraph())
                    .isEqualTo(graph().add(a).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(2).toGraph())
                    .isEqualTo(graph().add().add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, a).max(3).toGraph())
                    .isEqualTo(graph().add(a).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(3).toGraph())
                    .isEqualTo(graph().add().add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, a).max(4).toGraph())
                    .isEqualTo(graph().add(a).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(4).toGraph())
                    .isEqualTo(graph().add().add(b, c).add(d).add().add(a).build());
            assertThat(f.cursor(BOTH, a).toGraph())
                    .isEqualTo(graph().add(a).add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, a).excludeStart().toGraph())
                    .isEqualTo(graph().add().add(b, c).add(d).add().add(a).build());

            assertThat(f.cursor(BOTH, a).into(a).excludeStart().max(0).toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, a).into(c).max(1).toGraph())
                    .isEqualTo(graph().add().add(c).build());
            assertThat(f.cursor(BOTH, a).into(c).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(c).build());
            assertThat(f.cursor(BOTH, a).into(d).max(2).toGraph()).isEqualTo(nodeAtDepth(d, 3));
            assertThat(f.cursor(BOTH, a).into(d).excludeStart().max(2).toGraph())
                    .isEqualTo(nodeAtDepth(d, 3));
            assertThat(f.cursor(BOTH, a).into(d).max(3).toGraph()).isEqualTo(nodeAtDepth(d, 3));
            assertThat(f.cursor(BOTH, a).into(d).excludeStart().max(3).toGraph())
                    .isEqualTo(nodeAtDepth(d, 3));
            assertThat(f.cursor(BOTH, a).into(d).max(4).toGraph()).isEqualTo(nodeAtDepth(d, 3));
            assertThat(f.cursor(BOTH, a).into(a).excludeStart().max(4).toGraph())
                    .isEqualTo(nodeAtDepth(a, 5));
            assertThat(f.cursor(BOTH, a).into(d).toGraph()).isEqualTo(nodeAtDepth(d, 3));
            assertThat(f.cursor(BOTH, a).into(a).excludeStart().toGraph()).isEqualTo(nodeAtDepth(a, 5));
        }
    }

    @Test
    void shouldHandleLoopConnectingSameBFSLayer() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
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
            assertThat(f.cursor(BOTH, a).excludeStart().max(0).toGraph()).isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, a).max(1).toGraph())
                    .isEqualTo(graph().add(a).add(b, c).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(b, c).build());
            assertThat(f.cursor(BOTH, a).max(2).toGraph())
                    .isEqualTo(graph().add(a).add(b, c).add(d, e).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(2).toGraph())
                    .isEqualTo(graph().add().add(b, c).add(d, e).build());
            assertThat(f.cursor(BOTH, a).max(3).toGraph())
                    .isEqualTo(graph().add(a).add(b, c).add(d, e).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(3).toGraph())
                    .isEqualTo(graph().add().add(b, c).add(d, e).build());
            assertThat(f.cursor(BOTH, a).max(4).toGraph())
                    .isEqualTo(graph().add(a).add(b, c).add(d, e).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(4).toGraph())
                    .isEqualTo(graph().add().add(b, c).add(d, e).build());
            assertThat(f.cursor(BOTH, a).max(5).toGraph())
                    .isEqualTo(graph().add(a).add(b, c).add(d, e).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(5).toGraph())
                    .isEqualTo(
                            graph().add().add(b, c).add(d, e).add().add().add(a).build());
            assertThat(f.cursor(BOTH, a).toGraph())
                    .isEqualTo(graph().add(a).add(b, c).add(d, e).build());
            assertThat(f.cursor(BOTH, a).excludeStart().toGraph())
                    .isEqualTo(
                            graph().add().add(b, c).add(d, e).add().add().add(a).build());

            assertThat(f.cursor(BOTH, a).into(a).excludeStart().max(0).toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, a).into(c).max(1).toGraph())
                    .isEqualTo(graph().add().add(c).build());
            assertThat(f.cursor(BOTH, a).into(c).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(c).build());
            assertThat(f.cursor(BOTH, a).into(e).max(2).toGraph()).isEqualTo(nodeAtDepth(e, 3));
            assertThat(f.cursor(BOTH, a).into(e).excludeStart().max(2).toGraph())
                    .isEqualTo(nodeAtDepth(e, 3));
            assertThat(f.cursor(BOTH, a).into(e).max(3).toGraph()).isEqualTo(nodeAtDepth(e, 3));
            assertThat(f.cursor(BOTH, a).into(e).excludeStart().max(3).toGraph())
                    .isEqualTo(nodeAtDepth(e, 3));
            assertThat(f.cursor(BOTH, a).into(e).max(4).toGraph()).isEqualTo(nodeAtDepth(e, 3));
            assertThat(f.cursor(BOTH, a).into(e).excludeStart().max(4).toGraph())
                    .isEqualTo(nodeAtDepth(e, 3));
            assertThat(f.cursor(BOTH, a).into(e).max(5).toGraph()).isEqualTo(nodeAtDepth(e, 3));
            assertThat(f.cursor(BOTH, a).into(a).excludeStart().max(5).toGraph())
                    .isEqualTo(nodeAtDepth(a, 6));
            assertThat(f.cursor(BOTH, a).into(e).toGraph()).isEqualTo(nodeAtDepth(e, 3));
            assertThat(f.cursor(BOTH, a).into(a).excludeStart().toGraph()).isEqualTo(nodeAtDepth(a, 6));
        }
    }

    @Test
    void shouldHandleLoopConnectingSameAndDifferentBFSLayer() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
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
            assertThat(f.cursor(BOTH, a).excludeStart().max(0).toGraph()).isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, a).max(1).toGraph())
                    .isEqualTo(graph().add(a).add(b1, b2, b3).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(b1, b2, b3).build());
            assertThat(f.cursor(BOTH, a).max(2).toGraph())
                    .isEqualTo(graph().add(a).add(b1, b2, b3).add(c1, c2, c3).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(2).toGraph())
                    .isEqualTo(graph().add().add(b1, b2, b3).add(c1, c2, c3).build());
            assertThat(f.cursor(BOTH, a).max(3).toGraph())
                    .isEqualTo(graph().add(a).add(b1, b2, b3).add(c1, c2, c3).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(3).toGraph())
                    .isEqualTo(
                            graph().add().add(b1, b2, b3).add(c1, c2, c3).add(a).build());
            assertThat(f.cursor(BOTH, a).toGraph())
                    .isEqualTo(graph().add(a).add(b1, b2, b3).add(c1, c2, c3).build());
            assertThat(f.cursor(BOTH, a).excludeStart().toGraph())
                    .isEqualTo(
                            graph().add().add(b1, b2, b3).add(c1, c2, c3).add(a).build());

            assertThat(f.cursor(BOTH, a).into(a).excludeStart().max(0).toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, a).into(b3).max(1).toGraph())
                    .isEqualTo(graph().add().add(b3).build());
            assertThat(f.cursor(BOTH, a).into(b3).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(b3).build());
            assertThat(f.cursor(BOTH, a).into(c3).max(2).toGraph()).isEqualTo(nodeAtDepth(c3, 3));
            assertThat(f.cursor(BOTH, a).into(c3).excludeStart().max(2).toGraph())
                    .isEqualTo(nodeAtDepth(c3, 3));
            assertThat(f.cursor(BOTH, a).into(c3).max(3).toGraph()).isEqualTo(nodeAtDepth(c3, 3));
            assertThat(f.cursor(BOTH, a).into(a).excludeStart().max(3).toGraph())
                    .isEqualTo(nodeAtDepth(a, 4));
            assertThat(f.cursor(BOTH, a).into(c3).toGraph()).isEqualTo(nodeAtDepth(c3, 3));
            assertThat(f.cursor(BOTH, a).into(a).excludeStart().toGraph()).isEqualTo(nodeAtDepth(a, 4));
        }
    }

    @Test
    void shouldHandleTwoLoopsOfTheSameLength() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //     (b1)-(c1)
            //    / |    |
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
            write.relationshipCreate(b1, rel, b2);
            write.relationshipCreate(b2, rel, c2);
            write.relationshipCreate(b2, rel, b3);
            write.relationshipCreate(b3, rel, c3);
            write.relationshipCreate(c1, rel, c2);

            // then
            assertThat(f.cursor(BOTH, a).excludeStart().max(0).toGraph()).isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, a).max(1).toGraph())
                    .isEqualTo(graph().add(a).add(b1, b2, b3).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(b1, b2, b3).build());
            assertThat(f.cursor(BOTH, a).max(2).toGraph())
                    .isEqualTo(graph().add(a).add(b1, b2, b3).add(c1, c2, c3).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(2).toGraph())
                    .isEqualTo(graph().add().add(b1, b2, b3).add(c1, c2, c3).build());
            assertThat(f.cursor(BOTH, a).max(3).toGraph())
                    .isEqualTo(graph().add(a).add(b1, b2, b3).add(c1, c2, c3).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(3).toGraph())
                    .isEqualTo(
                            graph().add().add(b1, b2, b3).add(c1, c2, c3).add(a).build());
            assertThat(f.cursor(BOTH, a).toGraph())
                    .isEqualTo(graph().add(a).add(b1, b2, b3).add(c1, c2, c3).build());
            assertThat(f.cursor(BOTH, a).excludeStart().toGraph())
                    .isEqualTo(
                            graph().add().add(b1, b2, b3).add(c1, c2, c3).add(a).build());

            assertThat(f.cursor(BOTH, a).into(a).excludeStart().max(0).toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, a).into(b3).max(1).toGraph())
                    .isEqualTo(graph().add().add(b3).build());
            assertThat(f.cursor(BOTH, a).into(b3).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(b3).build());
            assertThat(f.cursor(BOTH, a).into(c3).max(2).toGraph()).isEqualTo(nodeAtDepth(c3, 3));
            assertThat(f.cursor(BOTH, a).into(c3).excludeStart().max(2).toGraph())
                    .isEqualTo(nodeAtDepth(c3, 3));
            assertThat(f.cursor(BOTH, a).into(c3).max(3).toGraph()).isEqualTo(nodeAtDepth(c3, 3));
            assertThat(f.cursor(BOTH, a).into(a).excludeStart().max(3).toGraph())
                    .isEqualTo(nodeAtDepth(a, 4));
            assertThat(f.cursor(BOTH, a).into(c3).toGraph()).isEqualTo(nodeAtDepth(c3, 3));
            assertThat(f.cursor(BOTH, a).into(a).excludeStart().toGraph()).isEqualTo(nodeAtDepth(a, 4));
        }
    }

    @Test
    void shouldHandleLoopWithContinuation() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
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
            long ff = write.nodeCreate();
            write.relationshipCreate(a, rel, b);
            write.relationshipCreate(a, rel, c);
            write.relationshipCreate(b, rel, d);
            write.relationshipCreate(d, rel, c);
            write.relationshipCreate(d, rel, e);
            write.relationshipCreate(e, rel, ff);

            // then
            assertThat(f.cursor(BOTH, a).excludeStart().max(0).toGraph()).isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, a).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(b, c).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(2).toGraph())
                    .isEqualTo(graph().add().add(b, c).add(d).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(3).toGraph())
                    .isEqualTo(graph().add().add(b, c).add(d).add(e).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(4).toGraph())
                    .isEqualTo(graph().add().add(b, c).add(d).add(e).add(ff, a).build());
            assertThat(f.cursor(BOTH, a).excludeStart().toGraph())
                    .isEqualTo(graph().add().add(b, c).add(d).add(e).add(ff, a).build());

            assertThat(f.cursor(BOTH, a).into(a).excludeStart().max(0).toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, a).into(c).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(c).build());
            assertThat(f.cursor(BOTH, a).into(d).excludeStart().max(2).toGraph())
                    .isEqualTo(nodeAtDepth(d, 3));
            assertThat(f.cursor(BOTH, a).into(e).excludeStart().max(3).toGraph())
                    .isEqualTo(nodeAtDepth(e, 4));
            assertThat(f.cursor(BOTH, a).into(ff).excludeStart().max(4).toGraph())
                    .isEqualTo(nodeAtDepth(ff, 5));
            assertThat(f.cursor(BOTH, a).into(ff).excludeStart().toGraph()).isEqualTo(nodeAtDepth(ff, 5));
            assertThat(f.cursor(BOTH, a).into(a).excludeStart().toGraph()).isEqualTo(nodeAtDepth(a, 5));
        }
    }

    @Test
    void shouldHandleParallelLayers() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
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
            long ff = write.nodeCreate();
            long g = write.nodeCreate();
            write.relationshipCreate(a, rel, b);
            write.relationshipCreate(a, rel, c);
            write.relationshipCreate(b, rel, d);
            write.relationshipCreate(c, rel, e);
            write.relationshipCreate(d, rel, ff);
            write.relationshipCreate(e, rel, g);

            // then
            assertThat(f.cursor(BOTH, a).excludeStart().max(0).toGraph()).isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, a).max(1).toGraph())
                    .isEqualTo(graph().add(a).add(b, c).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(b, c).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(2).toGraph())
                    .isEqualTo(graph().add().add(b, c).add(d, e).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(3).toGraph())
                    .isEqualTo(graph().add().add(b, c).add(d, e).add(ff, g).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(4).toGraph())
                    .isEqualTo(graph().add().add(b, c).add(d, e).add(ff, g).build());
            assertThat(f.cursor(BOTH, a).max(5).toGraph())
                    .isEqualTo(graph().add(a).add(b, c).add(d, e).add(ff, g).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(5).toGraph())
                    .isEqualTo(graph().add().add(b, c).add(d, e).add(ff, g).build());

            assertThat(f.cursor(BOTH, a).into(a).excludeStart().max(0).toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, a).into(c).max(1).toGraph())
                    .isEqualTo(graph().add().add(c).build());
            assertThat(f.cursor(BOTH, a).into(c).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(c).build());
            assertThat(f.cursor(BOTH, a).into(e).excludeStart().max(2).toGraph())
                    .isEqualTo(nodeAtDepth(e, 3));
            assertThat(f.cursor(BOTH, a).into(g).excludeStart().max(3).toGraph())
                    .isEqualTo(nodeAtDepth(g, 4));
            assertThat(f.cursor(BOTH, a).into(g).excludeStart().max(4).toGraph())
                    .isEqualTo(nodeAtDepth(g, 4));
            assertThat(f.cursor(BOTH, a).into(g).max(5).toGraph()).isEqualTo(nodeAtDepth(g, 4));
            assertThat(f.cursor(BOTH, a).into(g).excludeStart().max(5).toGraph())
                    .isEqualTo(nodeAtDepth(g, 4));
        }
    }

    @Test
    void shouldNotRetraceWhenALoopIsDetectedThatHasNoPathLeftToOrigin() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // (a) -> (b) <=> (c)
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();

            write.relationshipCreate(a, rel, b);
            write.relationshipCreate(b, rel, c);
            write.relationshipCreate(c, rel, b);

            // then
            assertThat(f.cursor(BOTH, a).excludeStart().max(0).toGraph()).isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, a).max(1).toGraph())
                    .isEqualTo(graph().add(a).add(b).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(b).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(2).toGraph())
                    .isEqualTo(graph().add().add(b).add(c).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(3).toGraph())
                    .isEqualTo(graph().add().add(b).add(c).build());
            assertThat(f.cursor(BOTH, a).max(4).toGraph())
                    .isEqualTo(graph().add(a).add(b).add(c).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(4).toGraph())
                    .isEqualTo(graph().add().add(b).add(c).build());
            assertThat(f.cursor(BOTH, a).toGraph())
                    .isEqualTo(graph().add(a).add(b).add(c).build());
            assertThat(f.cursor(BOTH, a).excludeStart().toGraph())
                    .isEqualTo(graph().add().add(b).add(c).build());

            assertThat(f.cursor(BOTH, a).into(a).excludeStart().max(0).toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, a).into(b).max(1).toGraph())
                    .isEqualTo(graph().add().add(b).build());
            assertThat(f.cursor(BOTH, a).into(b).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(b).build());
            assertThat(f.cursor(BOTH, a).into(c).excludeStart().max(2).toGraph())
                    .isEqualTo(nodeAtDepth(c, 3));
            assertThat(f.cursor(BOTH, a).into(c).excludeStart().max(3).toGraph())
                    .isEqualTo(nodeAtDepth(c, 3));
            assertThat(f.cursor(BOTH, a).into(c).max(4).toGraph()).isEqualTo(nodeAtDepth(c, 3));
            assertThat(f.cursor(BOTH, a).into(c).excludeStart().max(4).toGraph())
                    .isEqualTo(nodeAtDepth(c, 3));
            assertThat(f.cursor(BOTH, a).into(c).toGraph()).isEqualTo(nodeAtDepth(c, 3));
            assertThat(f.cursor(BOTH, a).into(c).excludeStart().toGraph()).isEqualTo(nodeAtDepth(c, 3));
        }
    }

    @Test
    void shouldNotRetraceWhenALoopIsDetectedThatHasNoPathLeftToOriginOutGoing() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // (a) -> (b) <=> (c)
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();

            write.relationshipCreate(a, rel, b);
            write.relationshipCreate(b, rel, c);
            write.relationshipCreate(c, rel, b);

            // then
            assertThat(f.cursor(OUTGOING, a).excludeStart().max(0).toGraph()).isEqualTo(EMPTY);
            assertThat(f.cursor(OUTGOING, a).max(1).toGraph())
                    .isEqualTo(graph().add(a).add(b).build());
            assertThat(f.cursor(OUTGOING, a).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(b).build());
            assertThat(f.cursor(OUTGOING, a).excludeStart().max(2).toGraph())
                    .isEqualTo(graph().add().add(b).add(c).build());
            assertThat(f.cursor(OUTGOING, a).excludeStart().max(3).toGraph())
                    .isEqualTo(graph().add().add(b).add(c).build());
            assertThat(f.cursor(OUTGOING, a).max(4).toGraph())
                    .isEqualTo(graph().add(a).add(b).add(c).build());
            assertThat(f.cursor(OUTGOING, a).excludeStart().max(4).toGraph())
                    .isEqualTo(graph().add().add(b).add(c).build());
            assertThat(f.cursor(OUTGOING, a).toGraph())
                    .isEqualTo(graph().add(a).add(b).add(c).build());
            assertThat(f.cursor(OUTGOING, a).excludeStart().toGraph())
                    .isEqualTo(graph().add().add(b).add(c).build());

            assertThat(f.cursor(OUTGOING, a).into(a).excludeStart().max(0).toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(OUTGOING, a).into(b).max(1).toGraph())
                    .isEqualTo(graph().add().add(b).build());
            assertThat(f.cursor(OUTGOING, a).into(b).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(b).build());
            assertThat(f.cursor(OUTGOING, a).into(c).excludeStart().max(2).toGraph())
                    .isEqualTo(nodeAtDepth(c, 3));
            assertThat(f.cursor(OUTGOING, a).into(c).excludeStart().max(3).toGraph())
                    .isEqualTo(nodeAtDepth(c, 3));
            assertThat(f.cursor(OUTGOING, a).into(c).max(4).toGraph()).isEqualTo(nodeAtDepth(c, 3));
            assertThat(f.cursor(OUTGOING, a).into(c).excludeStart().max(4).toGraph())
                    .isEqualTo(nodeAtDepth(c, 3));
            assertThat(f.cursor(OUTGOING, a).into(c).toGraph()).isEqualTo(nodeAtDepth(c, 3));
            assertThat(f.cursor(OUTGOING, a).into(c).excludeStart().toGraph()).isEqualTo(nodeAtDepth(c, 3));
        }
    }

    @Test
    void shouldNotRetraceWhenALoopIsDetectedThatHasNoPathLeftToOrigin2() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // (a) -> (b) <=> (b)
            long a = write.nodeCreate();
            long b = write.nodeCreate();

            write.relationshipCreate(a, rel, b);
            write.relationshipCreate(b, rel, b);

            // then
            assertThat(f.cursor(BOTH, a).excludeStart().max(0).toGraph()).isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, a).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(b).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(2).toGraph())
                    .isEqualTo(graph().add().add(b).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(3).toGraph())
                    .isEqualTo(graph().add().add(b).build());
            assertThat(f.cursor(BOTH, a).max(4).toGraph())
                    .isEqualTo(graph().add(a).add(b).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(4).toGraph())
                    .isEqualTo(graph().add().add(b).build());
            assertThat(f.cursor(BOTH, a).toGraph())
                    .isEqualTo(graph().add(a).add(b).build());
            assertThat(f.cursor(BOTH, a).excludeStart().toGraph())
                    .isEqualTo(graph().add().add(b).build());

            assertThat(f.cursor(BOTH, a).into(a).excludeStart().max(0).toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, a).into(b).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(b).build());
            assertThat(f.cursor(BOTH, a).into(b).excludeStart().max(2).toGraph())
                    .isEqualTo(graph().add().add(b).build());
            assertThat(f.cursor(BOTH, a).into(b).excludeStart().max(3).toGraph())
                    .isEqualTo(graph().add().add(b).build());
            assertThat(f.cursor(BOTH, a).into(b).max(4).toGraph())
                    .isEqualTo(graph().add().add(b).build());
            assertThat(f.cursor(BOTH, a).into(b).excludeStart().max(4).toGraph())
                    .isEqualTo(graph().add().add(b).build());
            assertThat(f.cursor(BOTH, a).into(b).toGraph())
                    .isEqualTo(graph().add().add(b).build());
            assertThat(f.cursor(BOTH, a).into(b).excludeStart().toGraph())
                    .isEqualTo(graph().add().add(b).build());
        }
    }

    @Test
    void shouldRetraceWhenALoopIsDetectedThatHasPathToOrigin() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // (a) <=> (b) <=> (c)
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();

            write.relationshipCreate(a, rel, b);
            write.relationshipCreate(b, rel, a);
            write.relationshipCreate(b, rel, c);
            write.relationshipCreate(c, rel, b);

            // then
            assertThat(f.cursor(BOTH, a).excludeStart().max(0).toGraph()).isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, a).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(b).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(2).toGraph())
                    .isEqualTo(graph().add().add(b).add(a, c).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(3).toGraph())
                    .isEqualTo(graph().add().add(b).add(a, c).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(4).toGraph())
                    .isEqualTo(graph().add().add(b).add(a, c).build());
            assertThat(f.cursor(BOTH, a).excludeStart().toGraph())
                    .isEqualTo(graph().add().add(b).add(a, c).build());

            assertThat(f.cursor(BOTH, a).into(a).excludeStart().max(0).toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, a).into(b).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(b).build());
            assertThat(f.cursor(BOTH, a).into(c).excludeStart().max(2).toGraph())
                    .isEqualTo(nodeAtDepth(c, 3));
            assertThat(f.cursor(BOTH, a).into(c).excludeStart().max(3).toGraph())
                    .isEqualTo(nodeAtDepth(c, 3));
            assertThat(f.cursor(BOTH, a).into(c).excludeStart().max(4).toGraph())
                    .isEqualTo(nodeAtDepth(c, 3));
            assertThat(f.cursor(BOTH, a).into(c).excludeStart().toGraph()).isEqualTo(nodeAtDepth(c, 3));
        }
    }

    @Test
    void shouldHandleDoublyConnectedFanOutGraph() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
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
            assertThat(asDepthMap(f.cursor(BOTH, a).excludeStart().max(0).build()))
                    .isEmpty();
            assertThat(asDepthMap(f.cursor(BOTH, a).excludeStart().max(1).build()))
                    .containsOnlyKeys(List.of(b1, b2, b3));
            assertThat(asDepthMap(f.cursor(BOTH, a).excludeStart().max(2).build()))
                    .containsOnlyKeys(List.of(b1, b2, b3, c1, c2, c3, c4, c5, c6, c7, c8, c9));
            assertThat(asDepthMap(f.cursor(BOTH, a).excludeStart().max(3).build()))
                    .containsOnlyKeys(List.of(b1, b2, b3, c1, c2, c3, c4, c5, c6, c7, c8, c9));
            assertThat(asDepthMap(f.cursor(BOTH, a).excludeStart().max(4).build()))
                    .containsOnlyKeys(List.of(b1, b2, b3, c1, c2, c3, c4, c5, c6, c7, c8, c9));
            assertThat(asDepthMap(f.cursor(BOTH, a).excludeStart().build()))
                    .containsOnlyKeys(List.of(b1, b2, b3, c1, c2, c3, c4, c5, c6, c7, c8, c9));
        }
    }

    @Test
    void shouldHandleComplicatedGraph() throws KernelException {
        // given
        try (var f = new Fixture()) {
            Write write = f.tx.dataWrite();
            int rel = f.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
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
            assertThat(f.cursor(BOTH, a).excludeStart().max(0).toGraph()).isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, a).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(b, c, d, e).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(2).toGraph())
                    .isEqualTo(graph().add().add(b, c, d, e).build());
            assertThat(f.cursor(BOTH, a).excludeStart().max(3).toGraph())
                    .isEqualTo(graph().add().add(b, c, d, e).add().add(a).build());
            assertThat(f.cursor(BOTH, a).excludeStart().toGraph())
                    .isEqualTo(graph().add().add(b, c, d, e).add().add(a).build());

            assertThat(f.cursor(BOTH, a).into(a).excludeStart().max(0).toGraph())
                    .isEqualTo(EMPTY);
            assertThat(f.cursor(BOTH, a).into(e).excludeStart().max(1).toGraph())
                    .isEqualTo(graph().add().add(e).build());
            assertThat(f.cursor(BOTH, a).into(e).excludeStart().max(2).toGraph())
                    .isEqualTo(graph().add().add(e).build());
            assertThat(f.cursor(BOTH, a).into(a).excludeStart().max(3).toGraph())
                    .isEqualTo(nodeAtDepth(a, 4));
            assertThat(f.cursor(BOTH, a).into(a).excludeStart().toGraph()).isEqualTo(nodeAtDepth(a, 4));
            assertThat(f.cursor(BOTH, a).into(e).excludeStart().toGraph())
                    .isEqualTo(graph().add().add(e).build());
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
        private final IntList distance;

        private BFSGraph(List<LongList> graph, IntList distance) {
            if (graph.size() != distance.size()) {
                throw new IllegalStateException(format(
                        """
                                Collections 'graph' and 'distance' must have same length but did not.
                                'graph' = %s
                                'distance' = %s""",
                        graph, distance));
            }
            this.graph = graph;
            this.distance = distance;
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

            return graph.equals(bfsGraph.graph) && distance.equals(bfsGraph.distance);
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37).append(graph).append(distance).toHashCode();
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < graph.size(); i++) {
                builder.append("[")
                        .append(distance.get(i))
                        .append("]")
                        .append(graph.get(i).makeString("{", ", ", "}\n"));
            }
            return builder.toString();
        }
    }

    private BFSGraphBuilder graph() {
        return new BFSGraphBuilder();
    }

    private BFSGraph graph(BFSPruningVarExpandCursor cursor) {
        List<LongList> graph = new ArrayList<>();
        MutableIntList distance = new IntArrayList();

        int d = -1;
        MutableLongList list = LongLists.mutable.empty();
        while (cursor.next()) {
            if (d != cursor.currentDepth()) {
                // with loops, distance to next valid node can be more than one greater than previous seen distance
                int emptyFrontiersToInsert = cursor.currentDepth() - d;
                for (int i = 1; i < emptyFrontiersToInsert; i++) {
                    graph.add(LongLists.immutable.empty());
                    distance.add(d + i);
                }
                d = cursor.currentDepth();
                // sort nodes of same depth, so equality check is not dependent on traversal order / storage format
                list.sortThis();
                list = LongLists.mutable.empty();
                graph.add(list);
                distance.add(d);
            }
            list.add(cursor.endNode());
        }
        list.sortThis();
        return new BFSGraph(graph, distance);
    }

    BFSGraph graph(List<Long> elements) {
        return graph(graph(), elements);
    }

    BFSGraph graphWithEmptyZeroFrontier(List<Long> elements) {
        return graph(graph().add(), elements);
    }

    BFSGraph nodeAtDepth(long node, int depth) {
        var g = graph();
        for (int i = 1; i < depth; i++) {
            g.add();
        }
        return g.add(node).build();
    }

    BFSGraph graph(BFSGraphBuilder builder, List<Long> elements) {
        for (Long element : elements) {
            builder.add(element);
        }
        return builder.build();
    }

    private static class BFSGraphBuilder {
        private final List<LongList> graph = new ArrayList<>();
        private final MutableIntList distance = new IntArrayList();

        private int currentDistance = 0;

        BFSGraphBuilder add(long... elements) {
            LongArrayList list = LongArrayList.newListWith(elements);
            list.sortThis();
            graph.add(list);
            distance.add(currentDistance++);
            return this;
        }

        BFSGraphBuilder add(List<Long> elements) {
            MutableLongList list = LongLists.mutable.empty();
            for (Long element : elements) {
                list.add(element);
            }
            list.sortThis();
            graph.add(list);
            distance.add(currentDistance++);
            return this;
        }

        BFSGraph build() {
            return new BFSGraph(graph, distance);
        }
    }

    private static final BFSGraph EMPTY = new BFSGraph(Collections.emptyList(), IntLists.immutable.empty());

    private class Fixture implements AutoCloseable {
        private final KernelTransaction tx;
        private final NodeCursor nodeCursor;
        private final RelationshipTraversalCursor relCursor;

        public Fixture() throws KernelException {
            this.tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
            this.nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
            this.relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT);
        }

        @Override
        public void close() throws KernelException {
            if (this.relCursor != null) this.relCursor.close();
            if (this.nodeCursor != null) this.nodeCursor.close();
            if (this.tx != null) this.tx.close();
        }

        public CursorBuilder cursor(Direction dir, long startNode) {
            return new CursorBuilder(dir, startNode);
        }

        class CursorBuilder {
            private final Direction direction;
            private final long startNode;
            private int[] types = null;
            private boolean includeStartNode = true;
            private int maxDepth = Integer.MAX_VALUE;
            private LongPredicate nodeFilter = Predicates.ALWAYS_TRUE_LONG;
            private Predicate<RelationshipTraversalCursor> relFilter = Predicates.alwaysTrue();
            private long soughtEndNode = NO_SUCH_ENTITY;
            private MemoryTracker memoryTracker = NO_TRACKING;

            public CursorBuilder(Direction direction, long startNode) {
                this.direction = direction;
                this.startNode = startNode;
            }

            BFSPruningVarExpandCursor build() {
                return switch (direction) {
                    case BOTH -> BFSPruningVarExpandCursor.allExpander(
                            startNode,
                            types,
                            includeStartNode,
                            maxDepth,
                            tx.dataRead(),
                            nodeCursor,
                            relCursor,
                            nodeFilter,
                            relFilter,
                            soughtEndNode,
                            memoryTracker);
                    case INCOMING -> BFSPruningVarExpandCursor.incomingExpander(
                            startNode,
                            types,
                            includeStartNode,
                            maxDepth,
                            tx.dataRead(),
                            nodeCursor,
                            relCursor,
                            nodeFilter,
                            relFilter,
                            soughtEndNode,
                            memoryTracker);
                    case OUTGOING -> BFSPruningVarExpandCursor.outgoingExpander(
                            startNode,
                            types,
                            includeStartNode,
                            maxDepth,
                            tx.dataRead(),
                            nodeCursor,
                            relCursor,
                            nodeFilter,
                            relFilter,
                            soughtEndNode,
                            memoryTracker);
                };
            }

            CursorBuilder max(int maxDepth) {
                this.maxDepth = maxDepth;
                return this;
            }

            CursorBuilder excludeStart() {
                this.includeStartNode = false;
                return this;
            }

            CursorBuilder into(long soughtEndNode) {
                this.soughtEndNode = soughtEndNode;
                return this;
            }

            CursorBuilder nodePred(LongPredicate nodeFilter) {
                this.nodeFilter = nodeFilter;
                return this;
            }

            CursorBuilder relPred(Predicate<RelationshipTraversalCursor> relFilter) {
                this.relFilter = relFilter;
                return this;
            }

            CursorBuilder types(int... types) {
                this.types = types;
                return this;
            }

            BFSGraph toGraph() {
                return graph(this.build());
            }
        }
    }
}
