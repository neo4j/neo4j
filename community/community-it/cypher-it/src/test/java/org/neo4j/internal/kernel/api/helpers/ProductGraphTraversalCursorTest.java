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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.api.KernelTransaction.Type.EXPLICIT;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.LongStream;
import org.eclipse.collections.impl.block.factory.primitive.LongPredicates;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.eclipse.collections.impl.stack.mutable.ArrayStack;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.NodeJuxtaposition;
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.PGStateBuilder;
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.ProductGraphTraversalCursor;
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.State;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
@Timeout(value = 10)
public class ProductGraphTraversalCursorTest {
    private static final EmptyMemoryTracker NO_TRACKING = EmptyMemoryTracker.INSTANCE;

    @Inject
    private Kernel kernel;

    @Test
    void shouldTraverseTwoHops() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            Read read = tx.dataRead();

            // (start)-[r1:R1]->(a1)-[r2:R2]->(f1)

            int R1 = tx.tokenWrite().relationshipTypeGetOrCreateForName("R1");
            int R2 = tx.tokenWrite().relationshipTypeGetOrCreateForName("R2");

            long start = write.nodeCreate();
            long a1 = write.nodeCreate();
            long a2 = write.nodeCreate();

            long r1 = write.relationshipCreate(start, R1, a1);
            long r2 = write.relationshipCreate(a1, R2, a2);

            PGStateBuilder builder = new PGStateBuilder();
            var s0 = builder.newStartState();
            var s1 = builder.newState();
            var s2 = builder.newFinalState();

            s0.addRelationshipExpansion(
                    s1, Predicates.alwaysTrue(), new int[] {R1}, Direction.BOTH, LongPredicates.alwaysTrue());

            s1.addRelationshipExpansion(
                    s2, Predicates.alwaysTrue(), new int[] {R2}, Direction.BOTH, LongPredicates.alwaysTrue());

            ProductGraphTraversalCursor pgCursor =
                    new ProductGraphTraversalCursor(read, nodeCursor, relCursor, NO_TRACKING);

            // then
            ProductGraph expected = new ProductGraphBuilder()
                    .addNode(start, s0.state())
                    .addNode(a1, s1.state())
                    .addNode(a2, s2.state())
                    .addRelationship(start, s0.state(), r1, a1, s1.state())
                    .addRelationship(a1, s1.state(), r2, a2, s2.state())
                    .build();

            ProductGraph actual = ProductGraph.fromCursors(start, s0.state(), pgCursor, nodeCursor, read);
            assertThat(actual).isEqualTo(expected);
            actual.assertMultiStateExpansions(pgCursor, nodeCursor, read);

            // Close the tx and redo the test when reading in a different tx from the writing. Tests a different
            // path in the underlying cursors
            nodeCursor.close();
            relCursor.close();
            tx.commit();
            try (var tx2 = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                    var nodeCursor2 = tx2.cursors().allocateNodeCursor(NULL_CONTEXT);
                    var relCursor2 = tx2.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
                Read read2 = tx2.dataRead();
                ProductGraphTraversalCursor pgCursor2 =
                        new ProductGraphTraversalCursor(read2, nodeCursor2, relCursor2, NO_TRACKING);
                ProductGraph actual2 = ProductGraph.fromCursors(start, s0.state(), pgCursor2, nodeCursor2, read2);
                assertThat(actual2).isEqualTo(expected);
                actual2.assertMultiStateExpansions(pgCursor2, nodeCursor2, read2);
            }
        }
    }

    @Test
    void shouldFilterOnType() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            Read read = tx.dataRead();

            // (start)-[r1:R1]->(a1)-[r2:R2]->(f1)

            int R1 = tx.tokenWrite().relationshipTypeGetOrCreateForName("R1");
            int R2 = tx.tokenWrite().relationshipTypeGetOrCreateForName("R2");

            long start = write.nodeCreate();
            long a1 = write.nodeCreate();
            long a2 = write.nodeCreate();

            long r1 = write.relationshipCreate(start, R1, a1);
            long r2 = write.relationshipCreate(a1, R2, a2);

            PGStateBuilder builder = new PGStateBuilder();
            var s0 = builder.newStartState();
            var s1 = builder.newState();
            var s2 = builder.newFinalState();

            s0.addRelationshipExpansion(
                    s1, Predicates.alwaysTrue(), new int[] {R1}, Direction.OUTGOING, LongPredicates.alwaysTrue());

            s1.addRelationshipExpansion(
                    s2,
                    Predicates.alwaysTrue(),
                    new int[] {R1}, // can't be traversed from s1
                    Direction.OUTGOING,
                    LongPredicates.alwaysTrue());

            ProductGraphTraversalCursor pgCursor =
                    new ProductGraphTraversalCursor(read, nodeCursor, relCursor, NO_TRACKING);

            // then
            ProductGraph expected = new ProductGraphBuilder()
                    .addNode(start, s0.state())
                    .addNode(a1, s1.state())
                    .addRelationship(start, s0.state(), r1, a1, s1.state())
                    .build();

            ProductGraph actual = ProductGraph.fromCursors(start, s0.state(), pgCursor, nodeCursor, read);

            actual.assertSame(expected);
            actual.assertMultiStateExpansions(pgCursor, nodeCursor, read);

            // Close the tx and redo the test when reading in a different tx from the writing. Tests a different
            // path in the underlying cursors
            nodeCursor.close();
            relCursor.close();
            tx.commit();
            try (var tx2 = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                    var nodeCursor2 = tx2.cursors().allocateNodeCursor(NULL_CONTEXT);
                    var relCursor2 = tx2.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
                Read read2 = tx2.dataRead();
                ProductGraphTraversalCursor pgCursor2 =
                        new ProductGraphTraversalCursor(read2, nodeCursor2, relCursor2, NO_TRACKING);
                ProductGraph actual2 = ProductGraph.fromCursors(start, s0.state(), pgCursor2, nodeCursor2, read2);
                assertThat(actual2).isEqualTo(expected);
                actual2.assertMultiStateExpansions(pgCursor2, nodeCursor2, read2);
            }
        }
    }

    @Test
    void shouldFilterOnDirection() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            Read read = tx.dataRead();

            // (start)-[r1:R1]->(a1)-[r2:R2]->(f1)

            int R1 = tx.tokenWrite().relationshipTypeGetOrCreateForName("R1");
            int R2 = tx.tokenWrite().relationshipTypeGetOrCreateForName("R2");

            long start = write.nodeCreate();
            long a1 = write.nodeCreate();
            long a2 = write.nodeCreate();

            long r1 = write.relationshipCreate(start, R1, a1);
            long r2 = write.relationshipCreate(a1, R2, a2);

            PGStateBuilder builder = new PGStateBuilder();
            var s0 = builder.newStartState();
            var s1 = builder.newState();
            var s2 = builder.newFinalState();

            s0.addRelationshipExpansion(
                    s1, Predicates.alwaysTrue(), null, Direction.OUTGOING, LongPredicates.alwaysTrue());

            s1.addRelationshipExpansion(
                    s2,
                    Predicates.alwaysTrue(),
                    new int[] {R2},
                    Direction.INCOMING, // can't be traversed from s1 in this direction
                    LongPredicates.alwaysTrue());

            ProductGraphTraversalCursor pgCursor =
                    new ProductGraphTraversalCursor(read, nodeCursor, relCursor, NO_TRACKING);

            // then
            ProductGraph expected = new ProductGraphBuilder()
                    .addNode(start, s0.state())
                    .addNode(a1, s1.state())
                    .addRelationship(start, s0.state(), r1, a1, s1.state())
                    .build();

            ProductGraph actual = ProductGraph.fromCursors(start, s0.state(), pgCursor, nodeCursor, read);
            assertThat(actual).isEqualTo(expected);
            actual.assertMultiStateExpansions(pgCursor, nodeCursor, read);

            // Close the tx and redo the test when reading in a different tx from the writing. Tests a different
            // path in the underlying cursors
            nodeCursor.close();
            relCursor.close();
            tx.commit();
            try (var tx2 = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                    var nodeCursor2 = tx2.cursors().allocateNodeCursor(NULL_CONTEXT);
                    var relCursor2 = tx2.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
                Read read2 = tx2.dataRead();
                ProductGraphTraversalCursor pgCursor2 =
                        new ProductGraphTraversalCursor(read2, nodeCursor2, relCursor2, NO_TRACKING);
                ProductGraph actual2 = ProductGraph.fromCursors(start, s0.state(), pgCursor2, nodeCursor2, read2);
                assertThat(actual2).isEqualTo(expected);
                actual2.assertMultiStateExpansions(pgCursor2, nodeCursor2, read2);
            }
        }
    }

    @Test
    void shouldFilterOnRelPredicate() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            Read read = tx.dataRead();

            // (start)-[r1:R1]->(a1)-[r2:R2]->(f1)

            int R1 = tx.tokenWrite().relationshipTypeGetOrCreateForName("R1");
            int R2 = tx.tokenWrite().relationshipTypeGetOrCreateForName("R2");

            long start = write.nodeCreate();
            long a1 = write.nodeCreate();
            long a2 = write.nodeCreate();

            long r1 = write.relationshipCreate(start, R1, a1);
            long r2 = write.relationshipCreate(a1, R2, a2);

            PGStateBuilder builder = new PGStateBuilder();
            var s0 = builder.newStartState();
            var s1 = builder.newState();
            var s2 = builder.newFinalState();

            s0.addRelationshipExpansion(
                    s1, Predicates.alwaysTrue(), null, Direction.OUTGOING, LongPredicates.alwaysTrue());

            s1.addRelationshipExpansion(
                    s2, Predicates.alwaysFalse(), new int[] {R2}, Direction.INCOMING, LongPredicates.alwaysTrue());

            ProductGraphTraversalCursor pgCursor =
                    new ProductGraphTraversalCursor(read, nodeCursor, relCursor, NO_TRACKING);

            // then
            ProductGraph expected = new ProductGraphBuilder()
                    .addNode(start, s0.state())
                    .addNode(a1, s1.state())
                    .addRelationship(start, s0.state(), r1, a1, s1.state())
                    .build();

            ProductGraph actual = ProductGraph.fromCursors(start, s0.state(), pgCursor, nodeCursor, read);
            assertThat(actual).isEqualTo(expected);
            actual.assertMultiStateExpansions(pgCursor, nodeCursor, read);

            // Close the tx and redo the test when reading in a different tx from the writing. Tests a different
            // path in the underlying cursors
            nodeCursor.close();
            relCursor.close();
            tx.commit();
            try (var tx2 = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                    var nodeCursor2 = tx2.cursors().allocateNodeCursor(NULL_CONTEXT);
                    var relCursor2 = tx2.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
                Read read2 = tx2.dataRead();
                ProductGraphTraversalCursor pgCursor2 =
                        new ProductGraphTraversalCursor(read2, nodeCursor2, relCursor2, NO_TRACKING);
                ProductGraph actual2 = ProductGraph.fromCursors(start, s0.state(), pgCursor2, nodeCursor2, read2);
                assertThat(actual2).isEqualTo(expected);
                actual2.assertMultiStateExpansions(pgCursor2, nodeCursor2, read2);
            }
        }
    }

    @Test
    void shouldFilterOnNodePredicate() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            Read read = tx.dataRead();

            // (start)-[r1:R1]->(a1)-[r2:R2]->(f1)

            int R1 = tx.tokenWrite().relationshipTypeGetOrCreateForName("R1");
            int R2 = tx.tokenWrite().relationshipTypeGetOrCreateForName("R2");

            long start = write.nodeCreate();
            long a1 = write.nodeCreate();
            long a2 = write.nodeCreate();

            long r1 = write.relationshipCreate(start, R1, a1);
            long r2 = write.relationshipCreate(a1, R2, a2);

            PGStateBuilder builder = new PGStateBuilder();
            var s0 = builder.newStartState();
            var s1 = builder.newState();
            var s2 = builder.newFinalState();

            s0.addRelationshipExpansion(
                    s1, Predicates.alwaysTrue(), null, Direction.OUTGOING, LongPredicates.alwaysTrue());

            s1.addRelationshipExpansion(
                    s2, Predicates.alwaysTrue(), new int[] {R2}, Direction.INCOMING, LongPredicates.alwaysFalse());

            ProductGraphTraversalCursor pgCursor =
                    new ProductGraphTraversalCursor(read, nodeCursor, relCursor, NO_TRACKING);

            // then
            ProductGraph expected = new ProductGraphBuilder()
                    .addNode(start, s0.state())
                    .addNode(a1, s1.state())
                    .addRelationship(start, s0.state(), r1, a1, s1.state())
                    .build();

            ProductGraph actual = ProductGraph.fromCursors(start, s0.state(), pgCursor, nodeCursor, read);
            assertThat(actual).isEqualTo(expected);
            actual.assertMultiStateExpansions(pgCursor, nodeCursor, read);

            // Close the tx and redo the test when reading in a different tx from the writing. Tests a different
            // path in the underlying cursors
            nodeCursor.close();
            relCursor.close();
            tx.commit();
            try (var tx2 = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                    var nodeCursor2 = tx2.cursors().allocateNodeCursor(NULL_CONTEXT);
                    var relCursor2 = tx2.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
                Read read2 = tx2.dataRead();
                ProductGraphTraversalCursor pgCursor2 =
                        new ProductGraphTraversalCursor(read2, nodeCursor2, relCursor2, NO_TRACKING);
                ProductGraph actual2 = ProductGraph.fromCursors(start, s0.state(), pgCursor2, nodeCursor2, read2);
                assertThat(actual2).isEqualTo(expected);
                actual2.assertMultiStateExpansions(pgCursor2, nodeCursor2, read2);
            }
        }
    }

    @Test
    void shouldHandleMultipleTypesInMultipleDirections() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            Read read = tx.dataRead();

            //                              -[o1:O1]->
            //       _________<________     -[o2:O3]->
            //     /                   \    -[o3:O2]->
            // [l{1,2,3}:L{1,2,3}]   (start)            (a1)
            //     \_______>___________/    <-[i1:I1]-
            //                              <-[i2:I3]-
            //                              <-[i3:I2]-

            int O1 = tx.tokenWrite().relationshipTypeGetOrCreateForName("O1");
            int O2 = tx.tokenWrite().relationshipTypeGetOrCreateForName("O2");
            int O3 = tx.tokenWrite().relationshipTypeGetOrCreateForName("O3");

            int I1 = tx.tokenWrite().relationshipTypeGetOrCreateForName("I1");
            int I2 = tx.tokenWrite().relationshipTypeGetOrCreateForName("I2");
            int I3 = tx.tokenWrite().relationshipTypeGetOrCreateForName("I3");

            int L1 = tx.tokenWrite().relationshipTypeGetOrCreateForName("L1");
            int L2 = tx.tokenWrite().relationshipTypeGetOrCreateForName("L2");
            int L3 = tx.tokenWrite().relationshipTypeGetOrCreateForName("L3");

            long start = write.nodeCreate();
            long a1 = write.nodeCreate();

            // Outgoing
            long o1 = write.relationshipCreate(start, O1, a1);
            long o2 = write.relationshipCreate(start, O2, a1);
            long o3 = write.relationshipCreate(start, O3, a1);

            // Incoming
            long i1 = write.relationshipCreate(a1, I1, start);
            long i2 = write.relationshipCreate(a1, I2, start);
            long i3 = write.relationshipCreate(a1, I3, start);

            // Loops
            long l1 = write.relationshipCreate(start, L1, start);
            long l2 = write.relationshipCreate(start, L2, start);
            long l3 = write.relationshipCreate(start, L3, start);

            PGStateBuilder builder = new PGStateBuilder();
            var s0 = builder.newStartState();
            var s1 = builder.newState();
            var s2 = builder.newState();
            var s3 = builder.newState();
            var s4 = builder.newFinalState();

            s0.addRelationshipExpansion(
                    s1, Predicates.alwaysTrue(), new int[] {O1}, Direction.OUTGOING, LongPredicates.alwaysTrue());

            s0.addRelationshipExpansion(
                    s2, Predicates.alwaysTrue(), new int[] {I1, I2}, Direction.INCOMING, LongPredicates.alwaysTrue());

            s0.addRelationshipExpansion(
                    s3, Predicates.alwaysTrue(), new int[] {L1, I3}, Direction.BOTH, LongPredicates.alwaysTrue());

            s3.addRelationshipExpansion(
                    s4, Predicates.alwaysTrue(), new int[] {O3, L2}, Direction.INCOMING, LongPredicates.alwaysTrue());

            s4.addRelationshipExpansion(
                    s0, Predicates.alwaysTrue(), new int[] {L3}, Direction.INCOMING, LongPredicates.alwaysTrue());

            ProductGraphTraversalCursor pgCursor =
                    new ProductGraphTraversalCursor(read, nodeCursor, relCursor, NO_TRACKING);

            // then
            ProductGraph expected = new ProductGraphBuilder()
                    .addNode(start, s0.state())
                    .addNode(a1, s1.state())
                    .addNode(a1, s2.state())
                    .addNode(a1, s3.state())
                    .addNode(start, s3.state())
                    .addNode(start, s4.state())
                    .addRelationship(start, s0.state(), o1, a1, s1.state())
                    .addRelationship(start, s0.state(), i1, a1, s2.state())
                    .addRelationship(start, s0.state(), i2, a1, s2.state())
                    .addRelationship(start, s0.state(), i3, a1, s3.state())
                    .addRelationship(start, s0.state(), l1, start, s3.state())
                    .addRelationship(a1, s3.state(), o3, start, s4.state())
                    .addRelationship(start, s3.state(), l2, start, s4.state())
                    .addRelationship(start, s4.state(), l3, start, s0.state())
                    .build();

            ProductGraph actual = ProductGraph.fromCursors(start, s0.state(), pgCursor, nodeCursor, read);
            assertThat(actual).isEqualTo(expected);
            actual.assertMultiStateExpansions(pgCursor, nodeCursor, read);

            // Close the tx and now perform the reading in a different tx from the writing. Tests a different
            // path in the underlying cursors
            nodeCursor.close();
            relCursor.close();
            tx.commit();
            try (var tx2 = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                    var nodeCursor2 = tx2.cursors().allocateNodeCursor(NULL_CONTEXT);
                    var relCursor2 = tx2.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
                Read read2 = tx2.dataRead();
                ProductGraphTraversalCursor pgCursor2 =
                        new ProductGraphTraversalCursor(read2, nodeCursor2, relCursor2, NO_TRACKING);
                ProductGraph actual2 = ProductGraph.fromCursors(start, s0.state(), pgCursor2, nodeCursor2, read2);
                assertThat(actual2).isEqualTo(expected);
                actual2.assertMultiStateExpansions(pgCursor2, nodeCursor2, read2);
            }
        }
    }

    @Test
    void nodeJuxtapositionShouldFilterOnNodePredicate() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            Read read = tx.dataRead();

            long start = write.nodeCreate();

            PGStateBuilder builder = new PGStateBuilder();
            var s0 = builder.newStartState();
            var s1 = builder.newState();
            var s2 = builder.newFinalState();

            s0.addNodeJuxtaposition(s1, LongPredicates.alwaysTrue());
            s0.addNodeJuxtaposition(s2, LongPredicates.alwaysFalse());
            s1.addNodeJuxtaposition(s2, LongPredicates.alwaysFalse());

            ProductGraphTraversalCursor pgCursor =
                    new ProductGraphTraversalCursor(read, nodeCursor, relCursor, NO_TRACKING);

            // then
            ProductGraph expected = new ProductGraphBuilder()
                    .addNode(start, s0.state())
                    .addNode(start, s1.state())
                    .addJuxtaposition(start, s0.state(), s1.state())
                    .build();

            ProductGraph actual = ProductGraph.fromCursors(start, s0.state(), pgCursor, nodeCursor, read);
            assertThat(actual).isEqualTo(expected);
            actual.assertMultiStateExpansions(pgCursor, nodeCursor, read);

            // Close the tx and redo the test when reading in a different tx from the writing. Tests a different
            // path in the underlying cursors
            nodeCursor.close();
            relCursor.close();
            tx.commit();
            try (var tx2 = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                    var nodeCursor2 = tx2.cursors().allocateNodeCursor(NULL_CONTEXT);
                    var relCursor2 = tx2.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
                Read read2 = tx2.dataRead();
                ProductGraphTraversalCursor pgCursor2 =
                        new ProductGraphTraversalCursor(read2, nodeCursor2, relCursor2, NO_TRACKING);
                ProductGraph actual2 = ProductGraph.fromCursors(start, s0.state(), pgCursor2, nodeCursor2, read2);
                assertThat(actual2).isEqualTo(expected);
                actual2.assertMultiStateExpansions(pgCursor2, nodeCursor2, read2);
            }
        }
    }

    @Test
    void complicatedGraphAndAutomataWithNoMixedTransitions() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            Read read = tx.dataRead();

            //          _____________________________________
            //         |                                     |
            //      (start)<-[r2s:R2s]-(n2)                  |
            //         |              /                      |
            //     [rs1:Rs1]    [r21:R21]                    |
            //         |    _____/          _____            |
            //         V  V               /       \          |
            //       (n1)-[r13:R13]->(n3)<-[r33:R33]     [rs5:Rs5]
            //         |               |                     |
            //     [r14:R14]       [r35:R35]                 |
            //         |               |                     |
            //         V       .       V                     |
            //       (n4)-[r45:R45]->(n5)<-------------------|

            long start = write.nodeCreate();
            long n1 = write.nodeCreate();
            long n2 = write.nodeCreate();
            long n3 = write.nodeCreate();
            long n4 = write.nodeCreate();
            long n5 = write.nodeCreate();

            int Rs1 = tx.tokenWrite().relationshipTypeGetOrCreateForName("Rs1");
            long rs1 = write.relationshipCreate(start, Rs1, n1);

            int R2s = tx.tokenWrite().relationshipTypeGetOrCreateForName("R2s");
            long r2s = write.relationshipCreate(n2, R2s, start);

            int Rs5 = tx.tokenWrite().relationshipTypeGetOrCreateForName("Rs5");
            long rs5 = write.relationshipCreate(start, Rs5, n5);

            int R21 = tx.tokenWrite().relationshipTypeGetOrCreateForName("R21");
            long r21 = write.relationshipCreate(n2, R21, n1);

            int R13 = tx.tokenWrite().relationshipTypeGetOrCreateForName("R13");
            long r13 = write.relationshipCreate(n1, R13, n3);

            int R14 = tx.tokenWrite().relationshipTypeGetOrCreateForName("R14");
            long r14 = write.relationshipCreate(n1, R14, n4);

            int R33 = tx.tokenWrite().relationshipTypeGetOrCreateForName("R33");
            long r33 = write.relationshipCreate(n3, R33, n3);

            int R35 = tx.tokenWrite().relationshipTypeGetOrCreateForName("R35");
            long r35 = write.relationshipCreate(n3, R35, n5);

            int R45 = tx.tokenWrite().relationshipTypeGetOrCreateForName("R45");
            long r45 = write.relationshipCreate(n4, R45, n5);

            PGStateBuilder nfaBuilder = new PGStateBuilder();
            var nj0 = nfaBuilder.newStartState();
            var nj1 = nfaBuilder.newState();
            var nj2 = nfaBuilder.newState();
            var nj3 = nfaBuilder.newState();
            var nj4 = nfaBuilder.newState();
            var nj5 = nfaBuilder.newFinalState();

            var re0 = nfaBuilder.newState();
            var re1 = nfaBuilder.newState();
            var re2 = nfaBuilder.newState();
            var re3 = nfaBuilder.newFinalState();
            var re4 = nfaBuilder.newFinalState();

            nj0.addNodeJuxtaposition(nj1, n -> n == n1);
            nj0.addNodeJuxtaposition(nj2, n -> n == start);
            nj1.addNodeJuxtaposition(nj2, LongPredicates.alwaysTrue());
            nj2.addNodeJuxtaposition(re0, n -> n == start);

            re0.addRelationshipExpansion(
                    nj4,
                    (rel) -> rel.sourceNodeReference() == rel.targetNodeReference(),
                    null, // All types
                    Direction.BOTH,
                    LongPredicates.alwaysTrue());

            re0.addRelationshipExpansion(
                    nj3,
                    (rel) -> rel.relationshipReference() == rs5
                            || rel.relationshipReference() == r2s
                            || rel.relationshipReference() == rs1,
                    null, // All types
                    Direction.BOTH,
                    n -> n != n5);

            nj3.addNodeJuxtaposition(re1, n -> n != start);

            re1.addRelationshipExpansion(
                    re2,
                    Predicates.alwaysTrue(),
                    null, // All types
                    Direction.BOTH,
                    LongPredicates.alwaysTrue());

            re2.addRelationshipExpansion(
                    re2,
                    Predicates.alwaysTrue(),
                    null, // All types
                    Direction.OUTGOING,
                    LongPredicates.alwaysTrue());

            re2.addRelationshipExpansion(
                    nj5,
                    (rel) -> rel.sourceNodeReference() == rel.targetNodeReference(),
                    null, // All types
                    Direction.OUTGOING,
                    LongPredicates.alwaysTrue());

            ProductGraphTraversalCursor pgCursor =
                    new ProductGraphTraversalCursor(read, nodeCursor, relCursor, NO_TRACKING);

            // then
            ProductGraph expected = new ProductGraphBuilder()
                    .addNode(start, nj0.state())
                    .addJuxtaposition(start, nj0.state(), nj2.state())
                    .addNode(start, nj2.state())
                    .addJuxtaposition(start, nj2.state(), re0.state())
                    .addNode(start, re0.state())
                    .addRelationship(start, re0.state(), r2s, n2, nj3.state())
                    .addRelationship(start, re0.state(), rs1, n1, nj3.state())
                    .addNode(n2, nj3.state())
                    .addNode(n1, nj3.state())
                    .addJuxtaposition(n2, nj3.state(), re1.state())
                    .addJuxtaposition(n1, nj3.state(), re1.state())
                    .addNode(n2, re1.state())
                    .addNode(n1, re1.state())
                    .addRelationship(n1, re1.state(), rs1, start, re2.state())
                    .addRelationship(n1, re1.state(), r21, n2, re2.state())
                    .addRelationship(n1, re1.state(), r13, n3, re2.state())
                    .addRelationship(n1, re1.state(), r14, n4, re2.state())
                    .addRelationship(n2, re1.state(), r21, n1, re2.state())
                    .addRelationship(n2, re1.state(), r2s, start, re2.state())
                    .addNode(start, re2.state())
                    .addNode(n2, re2.state())
                    .addNode(n3, re2.state())
                    .addNode(n4, re2.state())
                    .addNode(n1, re2.state())
                    .addRelationship(start, re2.state(), rs1, n1, re2.state())
                    .addRelationship(start, re2.state(), rs5, n5, re2.state())
                    .addRelationship(n1, re2.state(), r13, n3, re2.state())
                    .addRelationship(n1, re2.state(), r14, n4, re2.state())
                    .addRelationship(n2, re2.state(), r2s, start, re2.state())
                    .addRelationship(n2, re2.state(), r21, n1, re2.state())
                    .addRelationship(n3, re2.state(), r33, n3, re2.state())
                    .addRelationship(n3, re2.state(), r35, n5, re2.state())
                    .addRelationship(n4, re2.state(), r45, n5, re2.state())
                    .addNode(n5, re2.state())
                    .addRelationship(n3, re2.state(), r33, n3, nj5.state())
                    .addNode(n3, nj5.state())
                    .build();

            ProductGraph actual = ProductGraph.fromCursors(start, nj0.state(), pgCursor, nodeCursor, read);
            assertThat(actual).isEqualTo(expected);
            actual.assertMultiStateExpansions(pgCursor, nodeCursor, read);

            // Close the tx and redo the test when reading in a different tx from the writing. Tests a different
            // path in the underlying cursors
            nodeCursor.close();
            relCursor.close();
            tx.commit();
            try (var tx2 = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                    var nodeCursor2 = tx2.cursors().allocateNodeCursor(NULL_CONTEXT);
                    var relCursor2 = tx2.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
                Read read2 = tx2.dataRead();
                ProductGraphTraversalCursor pgCursor2 =
                        new ProductGraphTraversalCursor(read2, nodeCursor2, relCursor2, NO_TRACKING);
                ProductGraph actual2 = ProductGraph.fromCursors(start, nj0.state(), pgCursor2, nodeCursor2, read2);
                assertThat(actual2).isEqualTo(expected);
                actual2.assertMultiStateExpansions(pgCursor2, nodeCursor2, read2);
            }
        }
    }

    @Test
    void complicatedGraphAndAutomataWithMixedTransitions() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            Read read = tx.dataRead();

            //          _____________________________________
            //         |                                     |
            //      (start)<-[r2s:R2s]-(n2)                  |
            //         |              /                      |
            //     [rs1:Rs1]    [r21:R21]                    |
            //         |    _____/          _____            |
            //         V  V               /       \          |
            //       (n1)-[r13:R13]->(n3)<-[r33:R33]     [rs5:Rs5]
            //         |               |                     |
            //     [r14:R14]       [r35:R35]                 |
            //         |               |                     |
            //         V       .       V                     |
            //       (n4)-[r45:R45]->(n5)<-------------------|

            long start = write.nodeCreate();
            long n1 = write.nodeCreate();
            long n2 = write.nodeCreate();
            long n3 = write.nodeCreate();
            long n4 = write.nodeCreate();
            long n5 = write.nodeCreate();

            int Rs1 = tx.tokenWrite().relationshipTypeGetOrCreateForName("Rs1");
            long rs1 = write.relationshipCreate(start, Rs1, n1);

            int R2s = tx.tokenWrite().relationshipTypeGetOrCreateForName("R2s");
            long r2s = write.relationshipCreate(n2, R2s, start);

            int Rs5 = tx.tokenWrite().relationshipTypeGetOrCreateForName("Rs5");
            long rs5 = write.relationshipCreate(start, Rs5, n5);

            int R21 = tx.tokenWrite().relationshipTypeGetOrCreateForName("R21");
            long r21 = write.relationshipCreate(n2, R21, n1);

            int R13 = tx.tokenWrite().relationshipTypeGetOrCreateForName("R13");
            long r13 = write.relationshipCreate(n1, R13, n3);

            int R14 = tx.tokenWrite().relationshipTypeGetOrCreateForName("R14");
            long r14 = write.relationshipCreate(n1, R14, n4);

            int R33 = tx.tokenWrite().relationshipTypeGetOrCreateForName("R33");
            long r33 = write.relationshipCreate(n3, R33, n3);

            int R35 = tx.tokenWrite().relationshipTypeGetOrCreateForName("R35");
            long r35 = write.relationshipCreate(n3, R35, n5);

            int R45 = tx.tokenWrite().relationshipTypeGetOrCreateForName("R45");
            long r45 = write.relationshipCreate(n4, R45, n5);

            PGStateBuilder nfaBuilder = new PGStateBuilder();
            var s0 = nfaBuilder.newStartState();
            var s1 = nfaBuilder.newState();
            var s2 = nfaBuilder.newState();
            var s3 = nfaBuilder.newState();
            var s4 = nfaBuilder.newState();
            var s5 = nfaBuilder.newFinalState();

            var s6 = nfaBuilder.newState();
            var s7 = nfaBuilder.newState();
            var s8 = nfaBuilder.newState();
            var s9 = nfaBuilder.newFinalState();
            var s10 = nfaBuilder.newFinalState();

            // mixed state
            s0.addNodeJuxtaposition(s1, n -> n == n1);
            s0.addNodeJuxtaposition(s2, n -> n == start);
            s0.addRelationshipExpansion(
                    s4,
                    Predicates.alwaysTrue(),
                    null, // All types
                    Direction.INCOMING,
                    LongPredicates.alwaysTrue());

            s1.addNodeJuxtaposition(s2, LongPredicates.alwaysTrue());
            s2.addNodeJuxtaposition(s6, n -> n == start);

            s4.addNodeJuxtaposition(s5, n -> n == n2);
            s4.addRelationshipExpansion(
                    s3, Predicates.alwaysTrue(), new int[] {R21}, Direction.OUTGOING, LongPredicates.alwaysTrue());

            s6.addRelationshipExpansion(
                    s4,
                    (rel) -> rel.sourceNodeReference() == rel.targetNodeReference(),
                    null, // All types
                    Direction.BOTH,
                    LongPredicates.alwaysTrue());

            s6.addRelationshipExpansion(
                    s3,
                    (r) -> r.relationshipReference() == rs5
                            || r.relationshipReference() == r2s
                            || r.relationshipReference() == rs1,
                    null, // All types
                    Direction.BOTH,
                    n -> n != n5);

            s3.addNodeJuxtaposition(s7, n -> n != start);

            s7.addRelationshipExpansion(
                    s8,
                    Predicates.alwaysTrue(),
                    null, // All types
                    Direction.BOTH,
                    LongPredicates.alwaysTrue());

            s8.addRelationshipExpansion(
                    s8,
                    Predicates.alwaysTrue(),
                    null, // All types
                    Direction.OUTGOING,
                    LongPredicates.alwaysTrue());

            s8.addRelationshipExpansion(
                    s5,
                    (rel) -> rel.sourceNodeReference() == rel.targetNodeReference(),
                    null, // All types
                    Direction.OUTGOING,
                    LongPredicates.alwaysTrue());

            ProductGraphTraversalCursor pgCursor =
                    new ProductGraphTraversalCursor(read, nodeCursor, relCursor, NO_TRACKING);
            ProductGraph actual = ProductGraph.fromCursors(start, s0.state(), pgCursor, nodeCursor, read);

            // then
            ProductGraph expected = new ProductGraphBuilder()
                    .addNode(start, s0.state())
                    .addJuxtaposition(start, s0.state(), s2.state())
                    .addNode(start, s2.state())
                    .addRelationship(start, s0.state(), r2s, n2, s4.state())
                    .addNode(n2, s4.state())
                    .addRelationship(n2, s4.state(), r21, n1, s3.state())
                    .addNode(n1, s3.state())
                    .addJuxtaposition(start, s2.state(), s6.state())
                    .addNode(start, s6.state())
                    .addRelationship(start, s6.state(), r2s, n2, s3.state())
                    .addNode(n2, s3.state())
                    .addRelationship(start, s6.state(), rs1, n1, s3.state())
                    .addJuxtaposition(n2, s4.state(), s5.state())
                    .addNode(n2, s5.state())
                    .addJuxtaposition(n2, s3.state(), s7.state())
                    .addNode(n2, s7.state())
                    .addJuxtaposition(n1, s3.state(), s7.state())
                    .addNode(n1, s7.state())
                    .addRelationship(n1, s7.state(), rs1, start, s8.state())
                    .addNode(start, s8.state())
                    .addRelationship(n1, s7.state(), r21, n2, s8.state())
                    .addNode(n2, s8.state())
                    .addRelationship(n1, s7.state(), r13, n3, s8.state())
                    .addNode(n3, s8.state())
                    .addRelationship(n1, s7.state(), r14, n4, s8.state())
                    .addNode(n4, s8.state())
                    .addRelationship(n2, s7.state(), r21, n1, s8.state())
                    .addNode(n1, s8.state())
                    .addRelationship(n2, s7.state(), r2s, start, s8.state())
                    .addRelationship(start, s8.state(), rs1, n1, s8.state())
                    .addRelationship(start, s8.state(), rs5, n5, s8.state())
                    .addRelationship(n1, s8.state(), r13, n3, s8.state())
                    .addRelationship(n1, s8.state(), r14, n4, s8.state())
                    .addRelationship(n2, s8.state(), r2s, start, s8.state())
                    .addRelationship(n2, s8.state(), r21, n1, s8.state())
                    .addRelationship(n3, s8.state(), r33, n3, s8.state())
                    .addRelationship(n3, s8.state(), r35, n5, s8.state())
                    .addRelationship(n4, s8.state(), r45, n5, s8.state())
                    .addNode(n5, s8.state())
                    .addRelationship(n3, s8.state(), r33, n3, s5.state())
                    .addNode(n3, s5.state())
                    .build();

            assertThat(actual.adjacencyLists).isEqualTo(expected.adjacencyLists);
            actual.assertMultiStateExpansions(pgCursor, nodeCursor, read);

            // Close the tx and redo the test when reading in a different tx from the writing. Tests a different
            // path in the underlying cursors
            nodeCursor.close();
            relCursor.close();
            tx.commit();
            try (var tx2 = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                    var nodeCursor2 = tx2.cursors().allocateNodeCursor(NULL_CONTEXT);
                    var relCursor2 = tx2.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
                Read read2 = tx2.dataRead();
                ProductGraphTraversalCursor pgCursor2 =
                        new ProductGraphTraversalCursor(read2, nodeCursor2, relCursor2, NO_TRACKING);
                ProductGraph actual2 = ProductGraph.fromCursors(start, s0.state(), pgCursor2, nodeCursor2, read2);
                assertThat(actual2).isEqualTo(expected);
                actual2.assertMultiStateExpansions(pgCursor2, nodeCursor2, read2);
            }
        }
    }

    public static class ProductGraphBuilder {
        private final HashMap<ProductGraph.PGNode, Set<ProductGraph.Relationship>> adjacencyLists;

        public ProductGraphBuilder() {
            adjacencyLists = new HashMap<>();
        }

        public ProductGraphBuilder addNode(long nodeId, State state) {
            ProductGraph.PGNode node = new ProductGraph.PGNode(nodeId, state);
            adjacencyLists.put(node, new HashSet<>());
            return this;
        }

        public ProductGraphBuilder addRelationship(
                long sourceNodeId, State sourceNodeState, long relId, long targetNodeId, State targetNodeState) {
            ProductGraph.PGNode sourceNode = new ProductGraph.PGNode(sourceNodeId, sourceNodeState);
            ProductGraph.PGNode targetNode = new ProductGraph.PGNode(targetNodeId, targetNodeState);
            adjacencyLists.get(sourceNode).add(new ProductGraph.Relationship(relId, targetNode));
            return this;
        }

        public ProductGraphBuilder addJuxtaposition(long nodeId, State sourceNodeState, State targetNodeState) {
            ProductGraph.PGNode sourceNode = new ProductGraph.PGNode(nodeId, sourceNodeState);
            ProductGraph.PGNode targetNode = new ProductGraph.PGNode(nodeId, targetNodeState);
            adjacencyLists.get(sourceNode).add(new ProductGraph.Relationship(NO_SUCH_RELATIONSHIP, targetNode));
            return this;
        }

        public ProductGraph build() {
            return new ProductGraph(this.adjacencyLists);
        }
    }

    public static class ProductGraph {

        private record PGNode(long id, State state) {
            public String ids() {
                return "(node:" + id + ",state:" + state.id() + ")";
            }
        }

        private record Relationship(long id, PGNode targetNode) {}

        public final HashMap<PGNode, Set<Relationship>> adjacencyLists;

        @Override
        public boolean equals(Object obj) {
            return obj instanceof ProductGraph && ((ProductGraph) obj).adjacencyLists.equals(adjacencyLists);
        }

        @Override
        public int hashCode() {
            return Objects.hash(adjacencyLists);
        }

        public ProductGraph(HashMap<PGNode, Set<Relationship>> adjacencyLists) {
            this.adjacencyLists = adjacencyLists;
        }

        /**
         * Creates a ProductGraph given a start node/state and cursors by exhausting the connected component containing
         * the source using depth first traversal and recording all traversed relationships/nodes+states in adjacency
         * lists. The expansions in this constructor will always initialize the pgCursor with one (node, state) pair
         * at a time. To assert that multi state expansions work, call
         * {@link #assertMultiStateExpansions(ProductGraphTraversalCursor pgCursor, NodeCursor nodeCursor, Read read)}
         * after the product graph is built.
         */
        public static ProductGraph fromCursors(
                long sourceNodeId,
                State startState,
                ProductGraphTraversalCursor pgCursor,
                NodeCursor nodeCursor,
                Read read) {

            ArrayStack<PGNode> toExpand = new ArrayStack<>();
            Set<PGNode> seen = new HashSet<>();

            PGNode sourceNode = new PGNode(sourceNodeId, startState);
            seen.add(sourceNode);

            toExpand.push(sourceNode);

            var adjacencyLists = new HashMap<PGNode, Set<Relationship>>();
            while (toExpand.notEmpty()) {
                PGNode node = toExpand.pop();

                expandTransition(adjacencyLists, node, pgCursor, toExpand, seen);
            }

            return new ProductGraph(adjacencyLists);
        }

        /**
         * The {@code ProductGraph.fromCursor(long sourceNodeId, State startState, ProductGraphTraversalCursor pgCursor, NodeCursor nodeCursor, Read read)}
         * function runs a DFS which expands every node in the product graph individually. This doesn't test the
         * feature of the ProductGraphTraversalCursor where it can be initialised with one nodeId and multiple states
         * simultaneously. This function expands the pgCursor every single nodeId with every possible subset
         * of states that the nodeId can exist with, and ensures that "multi state"-expansions work as expected.
         *
         * @param pgCursor   product graph cursor
         * @param nodeCursor node cursor
         * @param read       kernel read
         */
        public void assertMultiStateExpansions(ProductGraphTraversalCursor pgCursor, NodeCursor nodeCursor, Read read) {
            LongObjectHashMap<ArrayList<State>> statesOfNodes = new LongObjectHashMap<>();

            for (PGNode node : adjacencyLists.keySet()) {
                if (node.state.getRelationshipExpansions().length > 0) {
                    statesOfNodes.getIfAbsentPut(node.id(), ArrayList::new).add(node.state());
                }
            }

            for (var nodeStates : statesOfNodes.keyValuesView()) {
                long nodeId = nodeStates.getOne();
                ArrayList<State> states = nodeStates.getTwo();

                // Iterate through every possible subset of the states set
                for (var statesSubset : combinations(states)) {
                    Set<Relationship> existingAdjacencyList = new HashSet<>();
                    for (var state : statesSubset) {
                        existingAdjacencyList.addAll(adjacencyLists.get(new PGNode(nodeId, state)).stream()
                                .filter(r -> r.id != NO_SUCH_RELATIONSHIP)
                                .toList());
                    }

                    pgCursor.setNodeAndStates(nodeId, statesSubset);

                    var adjacencyList = new HashSet<>();
                    while (pgCursor.next()) {
                        adjacencyList.add(new Relationship(
                                pgCursor.relationshipReference(),
                                new PGNode(pgCursor.otherNodeReference(), pgCursor.targetState())));
                    }

                    assertThat(adjacencyList).as("Node id " + nodeId).isEqualTo(existingAdjacencyList);
                }
            }
        }

        private static void expandTransition(
                HashMap<PGNode, Set<Relationship>> adjacencyLists,
                PGNode node,
                ProductGraphTraversalCursor pgCursor,
                ArrayStack<PGNode> toExpand,
                Set<PGNode> seen) {
            State state = node.state;
            Set<Relationship> adjacencyList = new HashSet<>();

            // Expand node juxtapositions
            for (NodeJuxtaposition nodeJuxtaposition : state.getNodeJuxtapositions()) {
                if (nodeJuxtaposition.testNode(node.id)) {
                    long nodeId = node.id();
                    State newState = nodeJuxtaposition.targetState();

                    PGNode newNode = new PGNode(nodeId, newState);

                    if (!seen.contains(newNode)) {
                        toExpand.push(newNode);
                        seen.add(newNode);
                    }

                    adjacencyList.add(new Relationship(NO_SUCH_RELATIONSHIP, newNode));
                }
            }

            // Expand relationship expansions
            pgCursor.setNodeAndStates(node.id, Collections.singletonList(state));

            while (pgCursor.next()) {
                long nodeId = pgCursor.otherNodeReference();
                long relId = pgCursor.relationshipReference();
                State newState = pgCursor.targetState();

                PGNode newNode = new PGNode(nodeId, newState);

                if (!seen.contains(newNode)) {
                    toExpand.push(newNode);
                    seen.add(newNode);
                }

                adjacencyList.add(new Relationship(relId, newNode));
            }

            adjacencyLists.put(node, adjacencyList);
        }

        private HashMap<PGNode, Set<Relationship>> diff(ProductGraph other) {
            var missing = new HashMap<PGNode, Set<Relationship>>();
            for (var kv : this.adjacencyLists.entrySet()) {
                var otherSet = other.adjacencyLists.get(kv.getKey());
                if (kv.getValue() != otherSet) {
                    var diff = new HashSet<>(kv.getValue());
                    if (otherSet == null) {
                        missing.put(kv.getKey(), diff);
                    } else {
                        diff.removeAll(otherSet);
                        if (!diff.isEmpty()) {
                            missing.put(kv.getKey(), diff);
                        }
                    }
                }
            }
            return missing;
        }

        public void assertSame(ProductGraph other) {
            if (this.adjacencyLists.equals(other.adjacencyLists)) {
                return;
            }

            var present = this.diff(other);
            var absent = other.diff(this);

            StringBuilder message = new StringBuilder("Product graphs different.");
            if (!present.isEmpty()) {
                message.append("\nPresent in the first but not the second: ");
                for (var kv : present.entrySet()) {
                    message.append("\n").append(kv.getKey().ids());
                    for (var rel : kv.getValue()) {
                        message.append("\n - ").append(rel);
                    }
                }
            }
            if (!absent.isEmpty()) {
                message.append("\nPresent in the second but not the first: ");
                for (var kv : absent.entrySet()) {
                    message.append("\n").append(kv.getKey().ids());
                    for (var rel : kv.getValue()) {
                        message.append("\n - ").append(rel);
                    }
                }
            }
            if (present.isEmpty() && absent.isEmpty()) {
                message.append(" But could not find the difference");
            }
            throw new IllegalStateException(message.toString());
        }

        @Override
        public String toString() {
            return "ProductGraph{" + "adjacencyLists=" + adjacencyLists + '}';
        }
    }

    // uses a bitmask to generate combinations from a source collection
    private static <T> Iterable<ArrayList<T>> combinations(List<T> collection) {
        return LongStream.range(0, 1L << collection.size()).mapToObj(i -> {
            var subset = new ArrayList<T>();

            for (int j = 0; j < collection.size(); j++) {
                if ((i & (1L << j)) != 0) {
                    subset.add(collection.get(j));
                }
            }
            return subset;
        })::iterator;
    }
}
