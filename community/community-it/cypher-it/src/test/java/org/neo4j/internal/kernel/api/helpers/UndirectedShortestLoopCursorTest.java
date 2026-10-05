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
import static org.neo4j.values.virtual.VirtualValues.pathReference;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.block.factory.primitive.LongPredicates;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.Predicates;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalEntities;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.helpers.traversal.shortestloop.UndirectedMultiShortestLoopCursor;
import org.neo4j.internal.kernel.api.helpers.traversal.shortestloop.UndirectedShortestLoopCursor;
import org.neo4j.internal.kernel.api.helpers.traversal.shortestloop.UndirectedShortestLoopWalkCursor;
import org.neo4j.internal.kernel.api.helpers.traversal.shortestloop.UndirectedSingleShortestLoopCursor;
import org.neo4j.internal.kernel.api.helpers.traversal.shortestloop.UndirectedSingleShortestLoopWalkCursor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.virtual.PathReference;

@ImpermanentDbmsExtension
class UndirectedShortestLoopCursorTest {
    @Inject
    private Kernel kernel;

    // Find single loops:
    @Test
    void shouldFindSelfLoop() throws KernelException {
        // given
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // (start) ↔ (start)
            long start = write.nodeCreate();
            long r = write.relationshipCreate(start, rel, start);

            try (var cursor = fixture.undirectedSingleLoopCursor(start)) {
                var iterator = cursor.shortestPathIterator();
                assertThat(iterator).hasNext();
                assertThat(iterator.next()).isEqualTo(pathReference(new long[] {start, start}, new long[] {r}));
                assertThat(iterator).isExhausted();
            }
        }
    }

    @Test
    void shouldFindSingleTwoNodeLoop() throws KernelException {
        // given
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //
            // (start) ↔ (a)
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long startToA = write.relationshipCreate(start, rel, a);
            long aToStart = write.relationshipCreate(a, rel, start);

            try (var cursor = fixture.undirectedSingleLoopCursor(start)) {
                var iterator = cursor.shortestPathIterator();

                assertThat(iterator).hasNext();
                assertThat(iterator.next())
                        .isIn(
                                pathReference(new long[] {start, a, start}, new long[] {startToA, aToStart}),
                                pathReference(new long[] {start, a, start}, new long[] {aToStart, startToA}));
                assertThat(iterator).isExhausted();
            }
        }
    }

    @Test
    void shouldFindSingleTwoNodeLoopWalkSemantics() throws KernelException {
        // given
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // (start) - (a)  (single undirected relationship)
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long startToA = write.relationshipCreate(start, rel, a);

            try (var cursor = fixture.undirectedMultiLoopWalkCursor(start)) {
                var iterator = cursor.shortestPathIterator();

                assertThat(iterator).hasNext();
                assertThat(iterator.next())
                        .isIn(pathReference(new long[] {start, a, start}, new long[] {startToA, startToA}));
                assertThat(iterator).isExhausted();
            }
        }
    }

    @Test
    void shouldFindSingleThreeNodeLoopWalkSemantics() throws KernelException {
        // given
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // (start) - (a)  (single undirected relationship)
            //       \
            //       (b)
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long startToA = write.relationshipCreate(start, rel, a);
            long startToB = write.relationshipCreate(start, rel, b);

            try (var cursor = fixture.undirectedSingleWalkLoopCursor(start)) {
                var iterator = cursor.shortestPathIterator();

                assertThat(iterator).hasNext();
                assertThat(iterator.next())
                        .isIn(
                                pathReference(new long[] {start, a, start}, new long[] {startToA, startToA}),
                                pathReference(new long[] {start, b, start}, new long[] {startToB, startToB}));
                assertThat(iterator).isExhausted();
            }
        }
    }

    @Test
    void shouldFindAllThreeNodeLoopWalkSemantics() throws KernelException {
        // given
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // (start) = (a)
            //       \
            //       (b)
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long startToA = write.relationshipCreate(start, rel, a);
            long startToA2 = write.relationshipCreate(start, rel, a);
            long startToB = write.relationshipCreate(start, rel, b);

            try (var cursor = fixture.undirectedMultiLoopWalkCursor(start)) {
                var iterator = cursor.shortestPathIterator();

                assertThat(toList(iterator))
                        .containsExactlyInAnyOrder(
                                pathReference(new long[] {start, a, start}, new long[] {startToA, startToA}),
                                pathReference(new long[] {start, a, start}, new long[] {startToA, startToA2}),
                                pathReference(new long[] {start, a, start}, new long[] {startToA2, startToA}),
                                pathReference(new long[] {start, a, start}, new long[] {startToA2, startToA2}),
                                pathReference(new long[] {start, b, start}, new long[] {startToB, startToB}));
            }
        }
    }

    @Test
    void shouldFindNothingThreeNodeLoopWalkSemanticsMaxDepthOne() throws KernelException {
        // given
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // (start) = (a)
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            write.relationshipCreate(start, rel, a);
            write.relationshipCreate(start, rel, a);

            try (var cursor = fixture.undirectedMultiLoopWalkCursor(start, 1)) {

                assertThat(cursor.next()).isFalse();
            }
        }
    }

    @Test
    void shouldNotFindNonTrailUndirectedLoop() throws KernelException {
        // given
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //
            // (start) → (a)
            long start = write.nodeCreate();
            long a = write.nodeCreate();

            // layer 1
            write.relationshipCreate(start, rel, a);

            try (var cursor = fixture.undirectedSingleLoopCursor(start)) {
                var iterator = cursor.shortestPathIterator();
                assertThat(iterator).isExhausted();
            }
        }
    }

    @Test
    void shouldFindSingleThreeNodeUndirectedLoop() throws KernelException {
        // given
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //
            //        ↗ (a)
            // (start)   ↓
            //        ↖ (b)
            //
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long startToA = write.relationshipCreate(start, rel, a);
            long aTob = write.relationshipCreate(a, rel, b);
            long bToStart = write.relationshipCreate(b, rel, start);

            try (var cursor = fixture.undirectedSingleLoopCursor(start)) {
                var iterator = cursor.shortestPathIterator();

                assertThat(iterator).hasNext();
                assertThat(iterator.next())
                        .isIn(
                                pathReference(new long[] {start, a, b, start}, new long[] {startToA, aTob, bToStart}),
                                pathReference(new long[] {start, b, a, start}, new long[] {bToStart, aTob, startToA}));
                assertThat(iterator).isExhausted();
            }
        }
    }

    @Test
    void shouldFindSingleFourNodeUndirectedLoop() throws KernelException {
        // given
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //
            //        ↗ (a) ↘
            // (start)      (c)
            //        ↖ (b) ↙
            //
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();
            long startToA = write.relationshipCreate(start, rel, a);
            long aToC = write.relationshipCreate(a, rel, c);
            long cToB = write.relationshipCreate(c, rel, b);
            long bToStart = write.relationshipCreate(b, rel, start);

            try (var cursor = fixture.undirectedSingleLoopCursor(start)) {
                var iterator = cursor.shortestPathIterator();

                assertThat(iterator).hasNext();
                assertThat(iterator.next())
                        .isIn(
                                pathReference(
                                        new long[] {start, a, c, b, start},
                                        new long[] {startToA, aToC, cToB, bToStart}),
                                pathReference(
                                        new long[] {start, b, c, a, start},
                                        new long[] {bToStart, cToB, aToC, startToA}));
                assertThat(iterator).isExhausted();
            }
        }
    }

    @Test
    void shouldFindSingleFourNodeUndirectedLoopWithDanglingNodes() throws KernelException {
        // given
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //           ()
            //           |
            //        ↗ (a) ↘
            // (start)   -  (b)
            //
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            var startToA = write.relationshipCreate(start, rel, a);
            write.relationshipCreate(a, rel, b);
            write.relationshipCreate(b, rel, start);
            // Create dangling relationships
            long dangling_a = write.nodeCreate();
            write.relationshipCreate(a, rel, dangling_a);

            try (var cursor = fixture.undirectedSingleLoopCursor(dangling_a)) {
                var iterator = cursor.shortestPathIterator();

                assertThat(iterator).isExhausted();
            } // We should have no loops for the dangling node
        }
    }

    @Test
    void shouldNotFindSingleThreeNodeUndirectedLoop() throws KernelException {
        // given
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //
            //        ↗ (a)
            // (start)
            //        ↖ (b) ↗↙
            //
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long startToA = write.relationshipCreate(start, rel, a);
            long bToB = write.relationshipCreate(b, rel, b);
            long bToStart = write.relationshipCreate(b, rel, start);

            try (var cursor = fixture.undirectedSingleLoopCursor(start)) {
                var iterator = cursor.shortestPathIterator();
                assertThat(iterator).isExhausted();
            }
        }
    }

    @Test
    void shouldNotFindSingleFourNodeUndirectedLoop() throws KernelException {
        // given
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //
            //        ↗ (a)
            // (start)       (c)
            //        ↖ (b) ↗↙
            //
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();

            write.relationshipCreate(start, rel, a);
            write.relationshipCreate(c, rel, b);
            write.relationshipCreate(c, rel, b);
            write.relationshipCreate(b, rel, start);

            try (var cursor = fixture.undirectedSingleLoopCursor(start)) {
                var iterator = cursor.shortestPathIterator();
                assertThat(iterator).isExhausted();
            }
        }
    }

    @Test
    void shouldNotFindLoopThatDoesNotGoBackToStart() throws KernelException {
        // given
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            int otherRel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("S");
            //
            //             ↗ (b) → (c)
            // (start) → (a)     ↖  ↓
            //                     (d)
            //
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();
            long d = write.nodeCreate();
            long startToA = write.relationshipCreate(start, rel, a);
            long aTob = write.relationshipCreate(a, rel, b);
            long bToC = write.relationshipCreate(c, rel, c);
            long cToD = write.relationshipCreate(c, rel, d);
            long dToB = write.relationshipCreate(d, rel, b);

            // with type R
            var typedCursor = fixture.undirectedSingleLoopCursor(start, new int[] {rel});

            assertThat(typedCursor.next()).isFalse();
        }
    }

    @Test
    void shouldFindSingleFiveNodeUndirectedLoop() throws KernelException {
        // given
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //
            //        ↗ (a) → (c)
            // (start)         ↓
            //        ↖ (b) → (d)
            //
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();
            long d = write.nodeCreate();
            long startToA = write.relationshipCreate(start, rel, a);
            long aToC = write.relationshipCreate(a, rel, c);
            long cToD = write.relationshipCreate(c, rel, d);
            long bToD = write.relationshipCreate(b, rel, d);
            long bToStart = write.relationshipCreate(b, rel, start);

            try (var cursor = fixture.undirectedSingleLoopCursor(start)) {
                var iterator = cursor.shortestPathIterator();

                assertThat(iterator).hasNext();
                assertThat(iterator.next())
                        .isIn(
                                pathReference(
                                        new long[] {start, a, c, d, b, start},
                                        new long[] {startToA, aToC, cToD, bToD, bToStart}),
                                pathReference(
                                        new long[] {start, b, d, c, a, start},
                                        new long[] {bToStart, bToD, cToD, aToC, startToA}));
                assertThat(iterator).isExhausted();
            }
        }
    }

    @Test
    void shouldFindSingleSixNodeLoop() throws KernelException {
        // given
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //
            //        ↗ (a) → (c) ↘
            // (start)            (e)
            //        ↖ (b) → (d) ↙
            //
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();
            long d = write.nodeCreate();
            long e = write.nodeCreate();
            long startToA = write.relationshipCreate(start, rel, a);
            long aToC = write.relationshipCreate(a, rel, c);
            long cToE = write.relationshipCreate(c, rel, e);
            long eToD = write.relationshipCreate(e, rel, d);
            long bToD = write.relationshipCreate(b, rel, d);
            long bToStart = write.relationshipCreate(b, rel, start);

            try (var cursor = fixture.undirectedSingleLoopCursor(start)) {
                var iterator = cursor.shortestPathIterator();

                assertThat(iterator).hasNext();
                assertThat(iterator.next())
                        .isIn(
                                pathReference(
                                        new long[] {start, a, c, e, d, b, start},
                                        new long[] {startToA, aToC, cToE, eToD, bToD, bToStart}),
                                pathReference(
                                        new long[] {start, b, d, e, c, a, start},
                                        new long[] {bToStart, bToD, eToD, cToE, aToC, startToA}));
                assertThat(iterator).isExhausted();
            }
        }
    }

    @Test
    void shouldRespectMaxDepthInSingleUndirectedLoop() throws KernelException {
        // given
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //
            //        ↗ (a) → (c) ↘
            // (start)            (e)
            //        ↖ (b) → (d) ↙
            //
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();
            long d = write.nodeCreate();
            long e = write.nodeCreate();
            long startToA = write.relationshipCreate(start, rel, a);
            long aToC = write.relationshipCreate(a, rel, c);
            long cToE = write.relationshipCreate(c, rel, e);
            long eToD = write.relationshipCreate(e, rel, d);
            long bToD = write.relationshipCreate(b, rel, d);
            long bToStart = write.relationshipCreate(b, rel, start);

            try (var cursor = fixture.undirectedSingleLoopCursor(start, 0)) {
                assertThat(cursor.next()).isFalse();
            }
            try (var cursor = fixture.undirectedSingleLoopCursor(start, 1)) {
                assertThat(cursor.next()).isFalse();
            }
            try (var cursor = fixture.undirectedSingleLoopCursor(start, 2)) {
                assertThat(cursor.next()).isFalse();
            }
            try (var cursor = fixture.undirectedSingleLoopCursor(start, 3)) {
                assertThat(cursor.next()).isFalse();
            }
            try (var cursor = fixture.undirectedSingleLoopCursor(start, 4)) {
                assertThat(cursor.next()).isFalse();
            }
            try (var cursor = fixture.undirectedSingleLoopCursor(start, 5)) {
                assertThat(cursor.next()).isFalse();
            }
            try (var cursor = fixture.undirectedSingleLoopCursor(start, 6)) {

                assertThat(cursor.next()).isTrue();
                assertThat(cursor.path())
                        .isIn(
                                pathReference(
                                        new long[] {start, a, c, e, d, b, start},
                                        new long[] {startToA, aToC, cToE, eToD, bToD, bToStart}),
                                pathReference(
                                        new long[] {start, b, d, e, c, a, start},
                                        new long[] {bToStart, bToD, eToD, cToE, aToC, startToA}));
                assertThat(cursor.next()).isFalse();
            }
        }
    }

    @Test
    void shouldOnlyFindShortestUndirectedLoop() throws KernelException {
        // given
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //
            //    ↙ (f)  → (d) ↘      ↗ (a) ↘
            // (h)              (start)       (c)
            //    ↖ (g) → (e) ↙       ↖ (b) ↙
            //
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();
            long d = write.nodeCreate();
            long e = write.nodeCreate();
            long f = write.nodeCreate();
            long g = write.nodeCreate();
            long h = write.nodeCreate();

            // short loop
            long startToA = write.relationshipCreate(start, rel, a);
            long aToC = write.relationshipCreate(a, rel, c);
            long cToB = write.relationshipCreate(c, rel, b);
            long bToStart = write.relationshipCreate(b, rel, start);
            // long loop
            write.relationshipCreate(start, rel, e);
            write.relationshipCreate(g, rel, e);
            write.relationshipCreate(g, rel, h);
            write.relationshipCreate(f, rel, h);
            write.relationshipCreate(f, rel, d);
            write.relationshipCreate(d, rel, start);

            try (var cursor = fixture.undirectedSingleLoopCursor(start)) {

                assertThat(cursor.next()).isTrue();
                assertThat(cursor.path())
                        .isIn(
                                pathReference(
                                        new long[] {start, a, c, b, start},
                                        new long[] {startToA, aToC, cToB, bToStart}),
                                pathReference(
                                        new long[] {start, b, c, a, start},
                                        new long[] {bToStart, cToB, aToC, startToA}));
                assertThat(cursor.next()).isFalse();
            }
        }
    }

    @Test
    void shouldFindSingleShortestUndirectedDoubleLoop() throws KernelException {
        // given
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //       (k) ↘
            //    ↙        (l) → (start) ↘    ↗ (b) ↘
            // (i) -> (j) ↗                (a)       (c)
            //    ↖ (h) → (g) → (f) → (e) ↗   ↖ (d) ↙
            //
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();
            long d = write.nodeCreate();
            long e = write.nodeCreate();
            long f = write.nodeCreate();
            long g = write.nodeCreate();
            long h = write.nodeCreate();
            long i = write.nodeCreate();
            long j = write.nodeCreate();
            long k = write.nodeCreate();
            long l = write.nodeCreate();

            // short loop
            write.relationshipCreate(a, rel, b);
            write.relationshipCreate(a, rel, d);
            write.relationshipCreate(c, rel, b);
            write.relationshipCreate(c, rel, d);
            // long loop
            long startToA = write.relationshipCreate(start, rel, a);
            long aToE = write.relationshipCreate(a, rel, e);
            long eToF = write.relationshipCreate(e, rel, f);
            long fToG = write.relationshipCreate(f, rel, g);
            long gToH = write.relationshipCreate(g, rel, h);
            long hToI = write.relationshipCreate(h, rel, i);
            long iToJ = write.relationshipCreate(i, rel, j);
            long iToK = write.relationshipCreate(i, rel, k);
            long kToL = write.relationshipCreate(k, rel, l);
            long jToL = write.relationshipCreate(j, rel, l);
            long lToStart = write.relationshipCreate(l, rel, start);

            try (var cursor = fixture.undirectedSingleLoopCursor(start)) {

                assertThat(cursor.next()).isTrue();
                assertThat(cursor.path())
                        .isIn(
                                pathReference(
                                        new long[] {start, a, e, f, g, h, i, j, l, start},
                                        new long[] {startToA, aToE, eToF, fToG, gToH, hToI, iToJ, jToL, lToStart}),
                                pathReference(
                                        new long[] {start, a, e, f, g, h, i, k, l, start},
                                        new long[] {startToA, aToE, eToF, fToG, gToH, hToI, iToK, kToL, lToStart}));
                assertThat(cursor.next()).isFalse();
            }
        }
    }

    @Test
    void shouldRespectTypesInSingleUndirectedLoop() throws KernelException {
        // given
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            int otherRel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("S");
            //
            //        ↗ (a) → (c) ↘
            // (start)   ↓[S]    (e)
            //        ↖ (b) → (d) ↙
            //
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();
            long d = write.nodeCreate();
            long e = write.nodeCreate();
            long startToA = write.relationshipCreate(start, rel, a);
            long aToC = write.relationshipCreate(a, rel, c);
            long cToE = write.relationshipCreate(c, rel, e);
            long eToD = write.relationshipCreate(e, rel, d);
            long bToD = write.relationshipCreate(b, rel, d);
            long bToStart = write.relationshipCreate(b, rel, start);
            // different relType
            long aToB = write.relationshipCreate(a, otherRel, b);

            // with type R
            try (var typedCursor = fixture.undirectedSingleLoopCursor(start, new int[] {rel})) {

                assertThat(typedCursor.next()).isTrue();
                assertThat(typedCursor.path())
                        .isIn(
                                pathReference(
                                        new long[] {start, a, c, e, d, b, start},
                                        new long[] {startToA, aToC, cToE, eToD, bToD, bToStart}),
                                pathReference(
                                        new long[] {start, b, d, e, c, a, start},
                                        new long[] {bToStart, bToD, eToD, cToE, aToC, startToA}));
                assertThat(typedCursor.next()).isFalse();
            }

            // with any type
            try (var untypedCursor = fixture.undirectedSingleLoopCursor(start)) {

                assertThat(untypedCursor.next()).isTrue();
                assertThat(untypedCursor.path())
                        .isIn(
                                pathReference(new long[] {start, a, b, start}, new long[] {startToA, aToB, bToStart}),
                                pathReference(new long[] {start, b, a, start}, new long[] {bToStart, aToB, startToA}));
                assertThat(untypedCursor.next()).isFalse();
            }

            // with type R or S
            try (var multiTypedCursor = fixture.undirectedSingleLoopCursor(start, new int[] {rel, otherRel})) {

                assertThat(multiTypedCursor.next()).isTrue();
                assertThat(multiTypedCursor.path())
                        .isIn(
                                pathReference(new long[] {start, a, b, start}, new long[] {startToA, aToB, bToStart}),
                                pathReference(new long[] {start, b, a, start}, new long[] {bToStart, aToB, startToA}));
                assertThat(multiTypedCursor.next()).isFalse();
            }
        }
    }

    @Test
    void shouldRespectNodeFilterInSingleUndirectedLoop() throws KernelException {
        // given
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //
            //    ↙ (f)  → (d) ↘      ↗ (a) ↘
            // (h)              (start)       (X) <- this node is being filtered
            //    ↖ (g) → (e) ↙       ↖ (b) ↙
            //
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long x = write.nodeCreate();
            long d = write.nodeCreate();
            long e = write.nodeCreate();
            long f = write.nodeCreate();
            long g = write.nodeCreate();
            long h = write.nodeCreate();

            // short loop
            write.relationshipCreate(start, rel, a);
            write.relationshipCreate(a, rel, x);
            write.relationshipCreate(x, rel, b);
            write.relationshipCreate(b, rel, start);
            // long loop
            long startToE = write.relationshipCreate(start, rel, e);
            long gToE = write.relationshipCreate(g, rel, e);
            long gToH = write.relationshipCreate(g, rel, h);
            long fToH = write.relationshipCreate(f, rel, h);
            long fToD = write.relationshipCreate(f, rel, d);
            long dToStart = write.relationshipCreate(d, rel, start);
            LongPredicate nodeFilter = (n) -> n != x;
            try (var cursor = fixture.undirectedSingleLoopCursor(start, nodeFilter)) {

                assertThat(cursor.next()).isTrue();
                assertThat(cursor.path())
                        .isIn(
                                pathReference(
                                        new long[] {start, d, f, h, g, e, start},
                                        new long[] {dToStart, fToD, fToH, gToH, gToE, startToE}),
                                pathReference(
                                        new long[] {start, e, g, h, f, d, start},
                                        new long[] {startToE, gToE, gToH, fToH, fToD, dToStart}));
                assertThat(cursor.next()).isFalse();
            }
        }
    }

    @Test
    void shouldRespectRelationshipFilterInSingleUndirectedLoop() throws KernelException {
        // given
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //
            //    ↙ (f)  → (d) ↘      ↗ (a) ↘
            // (h)              (start)       (c)
            //    ↖ (g) → (e) ↙       ↖ (b) ↙ <--This relationship is being filtered
            //
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();
            long d = write.nodeCreate();
            long e = write.nodeCreate();
            long f = write.nodeCreate();
            long g = write.nodeCreate();
            long h = write.nodeCreate();

            // short loop
            write.relationshipCreate(start, rel, a);
            write.relationshipCreate(a, rel, c);
            long removeMe = write.relationshipCreate(c, rel, b);
            write.relationshipCreate(b, rel, start);
            // long loop
            long startToE = write.relationshipCreate(start, rel, e);
            long gToE = write.relationshipCreate(g, rel, e);
            long gToH = write.relationshipCreate(g, rel, h);
            long fToH = write.relationshipCreate(f, rel, h);
            long fToD = write.relationshipCreate(f, rel, d);
            long dToStart = write.relationshipCreate(d, rel, start);

            Predicate<RelationshipTraversalEntities> relationshipFilter = r -> r.relationshipReference() != removeMe;
            try (var cursor = fixture.undirectedSingleLoopCursor(start, relationshipFilter)) {
                var iterator = cursor.shortestPathIterator();

                assertThat(iterator).hasNext();
                assertThat(iterator.next())
                        .isIn(
                                pathReference(
                                        new long[] {start, d, f, h, g, e, start},
                                        new long[] {dToStart, fToD, fToH, gToH, gToE, startToE}),
                                pathReference(
                                        new long[] {start, e, g, h, f, d, start},
                                        new long[] {startToE, gToE, gToH, fToH, fToD, dToStart}));
                assertThat(iterator).isExhausted();
            }
        }
    }

    // all undirected loops
    @Test
    void shouldFindMultiTwoNodeLoop() throws KernelException {
        // given
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //
            // (start) ↔ (a)
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long r1 = write.relationshipCreate(start, rel, a);
            long r2 = write.relationshipCreate(start, rel, a);
            long r3 = write.relationshipCreate(start, rel, a);

            var cursor = fixture.undirectedMultiLoopCursor(start);
            var iterator = cursor.shortestPathIterator();

            assertThat(toList(iterator))
                    .containsExactlyInAnyOrder(
                            pathReference(new long[] {start, a, start}, new long[] {r1, r2}),
                            pathReference(new long[] {start, a, start}, new long[] {r1, r3}),
                            pathReference(new long[] {start, a, start}, new long[] {r2, r1}),
                            pathReference(new long[] {start, a, start}, new long[] {r2, r3}),
                            pathReference(new long[] {start, a, start}, new long[] {r3, r1}),
                            pathReference(new long[] {start, a, start}, new long[] {r3, r2}));
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void shouldFindMultiTwoNodeLoopsWhenThereAreLongerLoopsAsWell() throws KernelException {
        // given
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //
            //         ↗  (a)
            // (start)    ↓↓
            //         ↖↖ (b)
            //
            long start = write.nodeCreate(); // 0
            long a = write.nodeCreate(); // 1
            long b = write.nodeCreate(); // 2
            long startToA1 = write.relationshipCreate(start, rel, a);
            long aTob1 = write.relationshipCreate(a, rel, b);
            long aTob2 = write.relationshipCreate(a, rel, b);
            long bToStart1 = write.relationshipCreate(b, rel, start);
            long bToStart2 = write.relationshipCreate(b, rel, start);

            var cursor = fixture.undirectedMultiLoopCursor(start);
            var iterator = cursor.shortestPathIterator();

            assertThat(toList(iterator))
                    .containsExactlyInAnyOrder(
                            pathReference(new long[] {start, b, start}, new long[] {bToStart1, bToStart2}),
                            pathReference(new long[] {start, b, start}, new long[] {bToStart2, bToStart1}));
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void shouldFindMultiOneNodeLoop() throws KernelException {
        // given
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            long start = write.nodeCreate();
            long r1 = write.relationshipCreate(start, rel, start);
            long r2 = write.relationshipCreate(start, rel, start);
            long r3 = write.relationshipCreate(start, rel, start);

            var cursor = fixture.undirectedMultiLoopCursor(start);
            var iterator = cursor.shortestPathIterator();

            assertThat(toList(iterator))
                    .containsExactlyInAnyOrder(
                            pathReference(new long[] {start, start}, new long[] {r1}),
                            pathReference(new long[] {start, start}, new long[] {r2}),
                            pathReference(new long[] {start, start}, new long[] {r3}));
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void shouldFindMultiOneNodeLoopWalk() throws KernelException {
        // given
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            long start = write.nodeCreate();
            long r1 = write.relationshipCreate(start, rel, start);
            long r2 = write.relationshipCreate(start, rel, start);
            long r3 = write.relationshipCreate(start, rel, start);

            var cursor = fixture.undirectedMultiLoopWalkCursor(start);
            var iterator = cursor.shortestPathIterator();

            assertThat(toList(iterator))
                    .containsExactlyInAnyOrder(
                            pathReference(new long[] {start, start}, new long[] {r1}),
                            pathReference(new long[] {start, start}, new long[] {r2}),
                            pathReference(new long[] {start, start}, new long[] {r3}));
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void shouldFindAllThreeNodeMultiUndirectedLoop() throws KernelException {
        // given
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //
            //         ↗  (a)
            // (start)    ↓↓
            //         ↖  (b)
            //
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long startToA = write.relationshipCreate(start, rel, a);
            long aTob1 = write.relationshipCreate(a, rel, b);
            long aTob2 = write.relationshipCreate(a, rel, b);
            long bToStart = write.relationshipCreate(b, rel, start);

            var cursor = fixture.undirectedMultiLoopCursor(start);
            var iterator = cursor.shortestPathIterator();

            assertThat(toList(iterator))
                    .containsExactlyInAnyOrder(
                            pathReference(new long[] {start, a, b, start}, new long[] {startToA, aTob1, bToStart}),
                            pathReference(new long[] {start, b, a, start}, new long[] {bToStart, aTob1, startToA}),
                            pathReference(new long[] {start, a, b, start}, new long[] {startToA, aTob2, bToStart}),
                            pathReference(new long[] {start, b, a, start}, new long[] {bToStart, aTob2, startToA}));
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void shouldFindAllFourNodeMultiUndirectedLoop() throws KernelException {
        // given
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //          (a)
            //         /  \\
            //  (start)    (c)
            //         \  //
            //         (b)
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();

            write.relationshipCreate(start, rel, a);
            write.relationshipCreate(a, rel, c);
            write.relationshipCreate(a, rel, c);
            write.relationshipCreate(b, rel, c);
            write.relationshipCreate(b, rel, c);
            write.relationshipCreate(b, rel, start);

            var cursor = fixture.undirectedMultiLoopCursor(start);
            var iterator = cursor.shortestPathIterator();

            assertThat(toList(iterator)).hasSize(8);
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void shouldFindAllLargeMultiUndirectedLoop() throws Exception {
        // given
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //           (a)
            //         /  |  \
            // (start) - (c) (d)
            //         \ || /
            //          (b)
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();
            long d = write.nodeCreate();

            write.relationshipCreate(start, rel, a);
            write.relationshipCreate(start, rel, b);
            write.relationshipCreate(start, rel, c);

            write.relationshipCreate(a, rel, c);
            write.relationshipCreate(b, rel, c);
            write.relationshipCreate(b, rel, c);

            write.relationshipCreate(b, rel, d);
            write.relationshipCreate(a, rel, d);

            var cursor = fixture.undirectedMultiLoopCursor(start);
            var iterator = cursor.shortestPathIterator();

            assertThat(toList(iterator)).hasSize(6);
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void shouldFindAllComplexMultiUndirectedLoop() throws KernelException {
        // given
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //          (a) - (b)
            //         /  \     \
            //  (start)   (c) - (d)
            //         \          \
            //         (e) = (f) - (g)
            long start = write.nodeCreate();

            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();
            long d = write.nodeCreate();

            write.relationshipCreate(start, rel, a);
            write.relationshipCreate(a, rel, b);
            write.relationshipCreate(a, rel, c);
            write.relationshipCreate(c, rel, d);
            write.relationshipCreate(b, rel, d);

            long e = write.nodeCreate();
            long f = write.nodeCreate();
            long g = write.nodeCreate();

            write.relationshipCreate(start, rel, e);
            write.relationshipCreate(e, rel, f);
            write.relationshipCreate(e, rel, f);
            write.relationshipCreate(f, rel, g);

            write.relationshipCreate(d, rel, g);

            var cursor = fixture.undirectedMultiLoopCursor(start);
            var iterator = cursor.shortestPathIterator();

            assertThat(toList(iterator)).hasSize(8);
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void shouldFindAllWithoutRunningOutOfHeapHugeMultiUndirectedLoop() throws KernelException {
        // given
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            long start = write.nodeCreate();
            int nodeCount = 24;
            int neighbours = 2;
            int leaves = 1;
            int i = 0;
            long temp = write.nodeCreate();
            write.relationshipCreate(start, rel, temp);
            while (i <= nodeCount - 2) {
                long next = write.nodeCreate();
                for (int j = 0; j < neighbours; j++) {
                    write.relationshipCreate(temp, rel, next);
                }
                for (int j = 0; j < leaves; j++) {
                    long s = write.nodeCreate();
                    write.relationshipCreate(temp, rel, s);
                }
                i++;
                temp = next;
            }
            write.relationshipCreate(temp, rel, start);

            var cursor = fixture.undirectedMultiLoopCursor(start);

            int paths = 0;
            while (cursor.next()) {
                paths++;
            }

            assertThat(paths == (int) Math.pow(neighbours, nodeCount));
        }
    }

    @Test
    void shouldFindAllTwoNodeLoopsWhenSeveralNeighboursFormThem() throws KernelException {
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // (a) ↔ (start) ↔ (b), and additionally (b) - (a), so that a longer loop exists as well
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long startToA = write.relationshipCreate(start, rel, a);
            long aToStart = write.relationshipCreate(a, rel, start);
            long startToB = write.relationshipCreate(start, rel, b);
            long bToStart = write.relationshipCreate(b, rel, start);
            write.relationshipCreate(b, rel, a);

            var cursor = fixture.undirectedMultiLoopCursor(start);
            var iterator = cursor.shortestPathIterator();

            assertThat(toList(iterator))
                    .containsExactlyInAnyOrder(
                            pathReference(new long[] {start, a, start}, new long[] {startToA, aToStart}),
                            pathReference(new long[] {start, a, start}, new long[] {aToStart, startToA}),
                            pathReference(new long[] {start, b, start}, new long[] {startToB, bToStart}),
                            pathReference(new long[] {start, b, start}, new long[] {bToStart, startToB}));
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void shouldFindAllTwoNodeLoopsWhenALongerLoopStartsWithTheSameRelationship() throws KernelException {
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // (start) ↔ (a) - (b) - (start)
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long startToA = write.relationshipCreate(start, rel, a);
            long aToStart = write.relationshipCreate(a, rel, start);
            write.relationshipCreate(a, rel, b);
            write.relationshipCreate(b, rel, start);

            var cursor = fixture.undirectedMultiLoopCursor(start);
            var iterator = cursor.shortestPathIterator();

            assertThat(toList(iterator))
                    .containsExactlyInAnyOrder(
                            pathReference(new long[] {start, a, start}, new long[] {startToA, aToStart}),
                            pathReference(new long[] {start, a, start}, new long[] {aToStart, startToA}));
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void shouldFindTwoNodeLoopWhenMaxDepthIsExactlyTwo() throws KernelException {
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // (start) ↔ (a)
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long startToA = write.relationshipCreate(start, rel, a);
            long aToStart = write.relationshipCreate(a, rel, start);

            var cursor = fixture.undirectedMultiLoopCursor(start, 2);
            var iterator = cursor.shortestPathIterator();

            assertThat(toList(iterator))
                    .containsExactlyInAnyOrder(
                            pathReference(new long[] {start, a, start}, new long[] {startToA, aToStart}),
                            pathReference(new long[] {start, a, start}, new long[] {aToStart, startToA}));
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void shouldNotFindTwoNodeLoopWhenMaxDepthIsOne() throws KernelException {
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // (start) ↔ (a), the only loop has length two and so exceeds the max depth
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            write.relationshipCreate(start, rel, a);
            write.relationshipCreate(a, rel, start);

            var cursor = fixture.undirectedMultiLoopCursor(start, 1);

            assertThat(cursor.shortestPathIterator()).isExhausted();
        }
    }

    @Test
    void shouldFindEvenLengthLoopWhenMaxDepthIsExactlyTheLoopLength() throws KernelException {
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // (start) - (a) - (b) - (c) - (start), the only loop has length four
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();
            long startToA = write.relationshipCreate(start, rel, a);
            long aToB = write.relationshipCreate(a, rel, b);
            long bToC = write.relationshipCreate(b, rel, c);
            long cToStart = write.relationshipCreate(c, rel, start);

            var cursor = fixture.undirectedMultiLoopCursor(start, 4);
            var iterator = cursor.shortestPathIterator();

            assertThat(toList(iterator))
                    .containsExactlyInAnyOrder(
                            pathReference(
                                    new long[] {start, a, b, c, start}, new long[] {startToA, aToB, bToC, cToStart}),
                            pathReference(
                                    new long[] {start, c, b, a, start}, new long[] {cToStart, bToC, aToB, startToA}));
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void shouldKeepLoopsFoundBeforeALevelThatCannotExtendThem() throws KernelException {
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // (start) - (a) - (b) - (start), with (a) ↔ (c) hanging off the loop. Expanding the third level
            // cannot extend the length three loop, which must still be returned.
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();
            long startToA = write.relationshipCreate(start, rel, a);
            long aToB = write.relationshipCreate(a, rel, b);
            long bToStart = write.relationshipCreate(b, rel, start);
            write.relationshipCreate(a, rel, c);
            write.relationshipCreate(c, rel, a);

            var cursor = fixture.undirectedMultiLoopCursor(start, 6);
            var iterator = cursor.shortestPathIterator();

            assertThat(toList(iterator))
                    .containsExactlyInAnyOrder(
                            pathReference(new long[] {start, a, b, start}, new long[] {startToA, aToB, bToStart}),
                            pathReference(new long[] {start, b, a, start}, new long[] {bToStart, aToB, startToA}));
            assertThat(iterator).isExhausted();
        }
    }

    @Test
    void shouldFindOddLoopWhenMaxDepthIsExactlyTheLoopLengthInSingleUndirectedLoop() throws KernelException {
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // (start) - (a) - (b) - (start), the only loop has length three
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long startToA = write.relationshipCreate(start, rel, a);
            long aToB = write.relationshipCreate(a, rel, b);
            long bToStart = write.relationshipCreate(b, rel, start);

            try (var cursor = fixture.undirectedSingleLoopCursor(start, 3)) {
                assertThat(cursor.next()).isTrue();
                assertThat(cursor.path())
                        .isIn(
                                pathReference(new long[] {start, a, b, start}, new long[] {startToA, aToB, bToStart}),
                                pathReference(new long[] {start, b, a, start}, new long[] {bToStart, aToB, startToA}));
                assertThat(cursor.next()).isFalse();
            }
        }
    }

    @Test
    void shouldNotFindOddLoopWhenMaxDepthIsBelowTheLoopLengthInSingleUndirectedLoop() throws KernelException {
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // (start) - (a) - (b) - (start), the only loop has length three and so exceeds the max depth
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            write.relationshipCreate(start, rel, a);
            write.relationshipCreate(a, rel, b);
            write.relationshipCreate(b, rel, start);

            try (var cursor = fixture.undirectedSingleLoopCursor(start, 2)) {
                assertThat(cursor.next()).isFalse();
            }
        }
    }

    @Test
    void shouldFindSelfLoopWhenMaxDepthIsOneInSingleUndirectedLoop() throws KernelException {
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // (start) ↔ (start)
            long start = write.nodeCreate();
            long r = write.relationshipCreate(start, rel, start);

            try (var cursor = fixture.undirectedSingleLoopCursor(start, 1)) {
                assertThat(cursor.next()).isTrue();
                assertThat(cursor.path()).isEqualTo(pathReference(new long[] {start, start}, new long[] {r}));
                assertThat(cursor.next()).isFalse();
            }
        }
    }

    @Test
    void shouldFindAllLoopsWhenAnOriginSeesOnlyItsOwnRoot() throws KernelException {
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // (start) - (a) - (c) - (e) - (d) - (b) - (start), plus a detour (a) - (y) - (c).
            // y and c hang off the same start relationship at the same depth, so an origin expanded
            // early in a level sees a single root although the level as a whole still has two.
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();
            long d = write.nodeCreate();
            long e = write.nodeCreate();
            long y = write.nodeCreate();
            long startToA = write.relationshipCreate(start, rel, a);
            long aToC = write.relationshipCreate(a, rel, c);
            long cToE = write.relationshipCreate(c, rel, e);
            long eToD = write.relationshipCreate(e, rel, d);
            long bToD = write.relationshipCreate(b, rel, d);
            long bToStart = write.relationshipCreate(b, rel, start);
            write.relationshipCreate(a, rel, y);
            write.relationshipCreate(y, rel, c);

            try (var cursor = fixture.undirectedMultiLoopCursor(start)) {
                assertThat(toList(cursor.shortestPathIterator()))
                        .containsExactlyInAnyOrder(
                                pathReference(
                                        new long[] {start, a, c, e, d, b, start},
                                        new long[] {startToA, aToC, cToE, eToD, bToD, bToStart}),
                                pathReference(
                                        new long[] {start, b, d, e, c, a, start},
                                        new long[] {bToStart, bToD, eToD, cToE, aToC, startToA}));
            }
        }
    }

    @Test
    void shouldNotDuplicateLoopsAfterASameRootMeet() throws KernelException {
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // (start) - (a) - (c) - (d) - (b) - (start), plus a detour (a) - (y) - (c).
            // y reaches c at the same depth on the same start relationship: not a loop, and c must
            // not be expanded a second time, which would record its traces twice.
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();
            long d = write.nodeCreate();
            long y = write.nodeCreate();
            long startToA = write.relationshipCreate(start, rel, a);
            long startToB = write.relationshipCreate(start, rel, b);
            long aToC = write.relationshipCreate(a, rel, c);
            long bToD = write.relationshipCreate(b, rel, d);
            long cToD = write.relationshipCreate(c, rel, d);
            write.relationshipCreate(a, rel, y);
            write.relationshipCreate(y, rel, c);

            try (var cursor = fixture.undirectedMultiLoopCursor(start)) {
                assertThat(toList(cursor.shortestPathIterator()))
                        .containsExactlyInAnyOrder(
                                pathReference(
                                        new long[] {start, a, c, d, b, start},
                                        new long[] {startToA, aToC, cToD, bToD, startToB}),
                                pathReference(
                                        new long[] {start, b, d, c, a, start},
                                        new long[] {startToB, bToD, cToD, aToC, startToA}));
            }
        }
    }

    @Test
    void shouldReturnLoopThroughStartAfterASameRootMeetInSingleUndirectedLoop() throws KernelException {
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // (start) - (a) - (c) - (d) - (b) - (start), plus a detour (a) - (y) - (c). y is created
            // before c so it is expanded first at depth two and reaches c, already at depth two on the
            // same start relationship. c's trace must keep pointing at a, or the path reconstructed
            // through c no longer starts at start.
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long y = write.nodeCreate();
            long c = write.nodeCreate();
            long d = write.nodeCreate();
            long startToA = write.relationshipCreate(start, rel, a);
            long startToB = write.relationshipCreate(start, rel, b);
            long aToC = write.relationshipCreate(a, rel, c);
            long bToD = write.relationshipCreate(b, rel, d);
            long cToD = write.relationshipCreate(c, rel, d);
            write.relationshipCreate(a, rel, y);
            write.relationshipCreate(y, rel, c);

            try (var cursor = fixture.undirectedSingleLoopCursor(start)) {
                assertThat(cursor.next()).isTrue();
                assertThat(cursor.path())
                        .isIn(
                                pathReference(
                                        new long[] {start, a, c, d, b, start},
                                        new long[] {startToA, aToC, cToD, bToD, startToB}),
                                pathReference(
                                        new long[] {start, b, d, c, a, start},
                                        new long[] {startToB, bToD, cToD, aToC, startToA}));
                assertThat(cursor.next()).isFalse();
            }
        }
    }

    @Test
    void shouldPreferOddLoopOverEvenLoopMetEarlierInTheSameLevelInSingleUndirectedLoop() throws KernelException {
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // (start) - (a) - (m) - (b) - (start) has length four, (start) - (p) - (q) - (start) length three.
            // Both are detected at depth one. a and b are created before p and q, so b meets m in the next
            // frontier before p or q is expanded; the cursor must not settle for the even loop.
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long p = write.nodeCreate();
            long q = write.nodeCreate();
            long m = write.nodeCreate();
            write.relationshipCreate(start, rel, a);
            write.relationshipCreate(start, rel, b);
            write.relationshipCreate(a, rel, m);
            write.relationshipCreate(b, rel, m);
            long startToP = write.relationshipCreate(start, rel, p);
            long startToQ = write.relationshipCreate(start, rel, q);
            long pToQ = write.relationshipCreate(p, rel, q);

            try (var cursor = fixture.undirectedSingleLoopCursor(start)) {
                assertThat(cursor.next()).isTrue();
                assertThat(cursor.path())
                        .isIn(
                                pathReference(new long[] {start, p, q, start}, new long[] {startToP, pToQ, startToQ}),
                                pathReference(new long[] {start, q, p, start}, new long[] {startToQ, pToQ, startToP}));
                assertThat(cursor.next()).isFalse();
            }
        }
    }

    @Test
    void shouldPreferSelfLoopOverTwoNodeLoopMetEarlierInSingleUndirectedLoop() throws KernelException {
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // (start) ↔ (start) created first, then (start) = (a) twice. Transaction state iterates outgoing
            // relationships before self relationships, so the two-node loop is met first; the self
            // relationship must still be the result.
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long self = write.relationshipCreate(start, rel, start);
            write.relationshipCreate(start, rel, a);
            write.relationshipCreate(start, rel, a);

            try (var cursor = fixture.undirectedSingleLoopCursor(start)) {
                assertThat(cursor.next()).isTrue();
                assertThat(cursor.path()).isEqualTo(pathReference(new long[] {start, start}, new long[] {self}));
                assertThat(cursor.next()).isFalse();
            }
        }
    }

    @Test
    void shouldFindLoneSelfLoopInMultiUndirectedLoop() throws KernelException {
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // (start) ↔ (start) and nothing else: a single relationship that is a loop
            long start = write.nodeCreate();
            long r = write.relationshipCreate(start, rel, start);

            try (var cursor = fixture.undirectedMultiLoopCursor(start)) {
                assertThat(toList(cursor.shortestPathIterator()))
                        .containsExactly(pathReference(new long[] {start, start}, new long[] {r}));
            }
        }
    }

    @Test
    void shouldFindTwoNodeLoopWhenItsFirstRelationshipHasIdZeroInSingleUndirectedLoop() throws KernelException {
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // (a) -> (start) is the first relationship in the database, id 0, and incoming relationships
            // iterate first from transaction state; its id must not read as the start node's absent root.
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long aToStart = write.relationshipCreate(a, rel, start);
            long startToA = write.relationshipCreate(start, rel, a);

            try (var cursor = fixture.undirectedSingleLoopCursor(start)) {
                assertThat(cursor.next()).isTrue();
                assertThat(cursor.path())
                        .isIn(
                                pathReference(new long[] {start, a, start}, new long[] {aToStart, startToA}),
                                pathReference(new long[] {start, a, start}, new long[] {startToA, aToStart}));
                assertThat(cursor.next()).isFalse();
            }
        }
    }

    @Test
    void shouldReturnOnlyTheOddLoopWhenAnEvenLoopIsMetEarlierInTheSameLevelInMultiUndirectedLoop()
            throws KernelException {
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // Same graph as the single-cursor test above. The multi cursor finishes the level and keeps the
            // minimum, so the length-four loop it meets first must not be emitted.
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long p = write.nodeCreate();
            long q = write.nodeCreate();
            long m = write.nodeCreate();
            write.relationshipCreate(start, rel, a);
            write.relationshipCreate(start, rel, b);
            write.relationshipCreate(a, rel, m);
            write.relationshipCreate(b, rel, m);
            long startToP = write.relationshipCreate(start, rel, p);
            long startToQ = write.relationshipCreate(start, rel, q);
            long pToQ = write.relationshipCreate(p, rel, q);

            try (var cursor = fixture.undirectedMultiLoopCursor(start)) {
                assertThat(toList(cursor.shortestPathIterator()))
                        .containsExactlyInAnyOrder(
                                pathReference(new long[] {start, p, q, start}, new long[] {startToP, pToQ, startToQ}),
                                pathReference(new long[] {start, q, p, start}, new long[] {startToQ, pToQ, startToP}));
            }
        }
    }

    @Test
    void shouldReturnOnlyTheSelfLoopWhenATwoNodeLoopIsMetEarlierInMultiUndirectedLoop() throws KernelException {
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // (start) ↔ (start) created first, then (start) = (a) twice; the two-node loop is met first.
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long self = write.relationshipCreate(start, rel, start);
            write.relationshipCreate(start, rel, a);
            write.relationshipCreate(start, rel, a);

            try (var cursor = fixture.undirectedMultiLoopCursor(start)) {
                assertThat(toList(cursor.shortestPathIterator()))
                        .containsExactly(pathReference(new long[] {start, start}, new long[] {self}));
            }
        }
    }

    @Test
    void shouldStopExpandingOnceALevelHangsOffASingleStartRelationshipInMultiUndirectedLoop() throws KernelException {
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // (start) - (a) is a dead end and (start) - (b) - (c) - (d) - (e) - (f) a chain. From depth two on
            // every node hangs off (start)-(b), so no loop can close and the search must end after c.
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();
            long d = write.nodeCreate();
            long e = write.nodeCreate();
            long f = write.nodeCreate();
            write.relationshipCreate(start, rel, a);
            write.relationshipCreate(start, rel, b);
            write.relationshipCreate(b, rel, c);
            write.relationshipCreate(c, rel, d);
            write.relationshipCreate(d, rel, e);
            write.relationshipCreate(e, rel, f);

            var reads = new NodeReadRecorder();
            fixture.nodeCursor.setTracer(reads);
            try (var cursor = fixture.undirectedMultiLoopCursor(start)) {
                assertThat(toList(cursor.shortestPathIterator())).isEmpty();
            }
            assertThat(reads.nodes.toArray()).containsExactlyInAnyOrder(start, a, b, c);
        }
    }

    @Test
    void shouldFindOnlyTheTriangleWhenAPentagonSharesTheStartNodeInMultiUndirectedLoop() throws KernelException {
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // Triangle (start) - (p) - (q) - (start) and pentagon (start) - (a) - (c) - (d) - (b) - (start). The
            // triangle is found one level before the pentagon's cross edge c - d; that edge must not be recorded as
            // an intersection, or the DFS turns sideways at d and walks a five-relationship loop into three slots.
            long start = write.nodeCreate();
            long p = write.nodeCreate();
            long q = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long c = write.nodeCreate();
            long d = write.nodeCreate();
            long startToP = write.relationshipCreate(start, rel, p);
            long pToQ = write.relationshipCreate(p, rel, q);
            long qToStart = write.relationshipCreate(q, rel, start);
            write.relationshipCreate(start, rel, a);
            write.relationshipCreate(a, rel, c);
            write.relationshipCreate(c, rel, d);
            write.relationshipCreate(d, rel, b);
            write.relationshipCreate(b, rel, start);

            try (var cursor = fixture.undirectedMultiLoopCursor(start)) {
                assertThat(toList(cursor.shortestPathIterator()))
                        .containsExactlyInAnyOrder(
                                pathReference(new long[] {start, p, q, start}, new long[] {startToP, pToQ, qToStart}),
                                pathReference(new long[] {start, q, p, start}, new long[] {qToStart, pToQ, startToP}));
            }
        }
    }

    @Test
    void shouldFindOnlyTheFourCyclesWhenTheirFarNodesAreAdjacentInMultiUndirectedLoop() throws KernelException {
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // Two 4-cycles (start) - (a) - (i) - (b) - (start) and (start) - (c) - (j) - (d) - (start), plus (i) - (j).
            // i and j are both intersections at the last level; the edge between them can only lengthen a loop
            // and must not be recorded, or the DFS steps across it and emits five-relationship rows in four slots.
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long i = write.nodeCreate();
            long c = write.nodeCreate();
            long d = write.nodeCreate();
            long j = write.nodeCreate();
            long startToA = write.relationshipCreate(start, rel, a);
            long aToI = write.relationshipCreate(a, rel, i);
            long iToB = write.relationshipCreate(i, rel, b);
            long bToStart = write.relationshipCreate(b, rel, start);
            long startToC = write.relationshipCreate(start, rel, c);
            long cToJ = write.relationshipCreate(c, rel, j);
            long jToD = write.relationshipCreate(j, rel, d);
            long dToStart = write.relationshipCreate(d, rel, start);
            write.relationshipCreate(i, rel, j);

            try (var cursor = fixture.undirectedMultiLoopCursor(start)) {
                assertThat(toList(cursor.shortestPathIterator()))
                        .containsExactlyInAnyOrder(
                                pathReference(
                                        new long[] {start, a, i, b, start},
                                        new long[] {startToA, aToI, iToB, bToStart}),
                                pathReference(
                                        new long[] {start, b, i, a, start},
                                        new long[] {bToStart, iToB, aToI, startToA}),
                                pathReference(
                                        new long[] {start, c, j, d, start},
                                        new long[] {startToC, cToJ, jToD, dToStart}),
                                pathReference(
                                        new long[] {start, d, j, c, start},
                                        new long[] {dToStart, jToD, cToJ, startToC}));
            }
        }
    }

    @Test
    void shouldFindOnlyTheFourCycleWhenItsFarNodeHasASelfRelationshipInMultiUndirectedLoop() throws KernelException {
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // (start) - (a) - (m) - (b) - (start) plus (m) ↔ (m): a self-relationship on the intersection is the
            // one-node form of the adjacent-intersections case.
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long m = write.nodeCreate();
            long startToA = write.relationshipCreate(start, rel, a);
            long startToB = write.relationshipCreate(start, rel, b);
            long aToM = write.relationshipCreate(a, rel, m);
            long bToM = write.relationshipCreate(b, rel, m);
            write.relationshipCreate(m, rel, m);

            try (var cursor = fixture.undirectedMultiLoopCursor(start)) {
                assertThat(toList(cursor.shortestPathIterator()))
                        .containsExactlyInAnyOrder(
                                pathReference(
                                        new long[] {start, a, m, b, start},
                                        new long[] {startToA, aToM, bToM, startToB}),
                                pathReference(
                                        new long[] {start, b, m, a, start},
                                        new long[] {startToB, bToM, aToM, startToA}));
            }
        }
    }

    @Test
    void shouldFindBothSelfLoopsWhenATwoNodeLoopIsMetFirstInMultiUndirectedLoop() throws KernelException {
        try (var fixture = new Fixture()) {
            Write write = fixture.tx.dataWrite();
            int rel = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            // (start) = (a) created first, then (start) ↔ (start) twice. Transaction state iterates outgoing
            // relationships before self relationships, so the two-node loop is met first and the guard that
            // protects the last level is armed when the second self relationship arrives; it must still be kept.
            long start = write.nodeCreate();
            long a = write.nodeCreate();
            write.relationshipCreate(start, rel, a);
            write.relationshipCreate(start, rel, a);
            long self1 = write.relationshipCreate(start, rel, start);
            long self2 = write.relationshipCreate(start, rel, start);

            try (var cursor = fixture.undirectedMultiLoopCursor(start)) {
                assertThat(toList(cursor.shortestPathIterator()))
                        .containsExactlyInAnyOrder(
                                pathReference(new long[] {start, start}, new long[] {self1}),
                                pathReference(new long[] {start, start}, new long[] {self2}));
            }
        }
    }

    private static Stream<Arguments> singleCycleBounds() {
        return IntStream.rangeClosed(1, 6)
                .boxed()
                .flatMap(length -> IntStream.rangeClosed(length - 1, length + 2)
                        .mapToObj(maxDepth -> Arguments.of(length, maxDepth)));
    }

    @ParameterizedTest(name = "cycle of length {0}, maxDepth {1}")
    @MethodSource("singleCycleBounds")
    void shouldRespectBoundsAroundTheCycleLengthInSingleUndirectedLoop(int length, int maxDepth)
            throws KernelException {
        try (var fixture = new Fixture()) {
            Cycle cycle = createCycle(fixture, length);
            try (var cursor = fixture.undirectedSingleLoopCursor(cycle.start(), maxDepth)) {
                if (maxDepth < length) {
                    assertThat(cursor.next()).isFalse();
                } else {
                    assertThat(cursor.next()).isTrue();
                    assertThat(cursor.path()).isIn((Object[]) cycle.orientations());
                    assertThat(cursor.next()).isFalse();
                }
            }
        }
    }

    @ParameterizedTest(name = "cycle of length {0}, maxDepth {1}")
    @MethodSource("singleCycleBounds")
    void shouldRespectBoundsAroundTheCycleLengthInMultiUndirectedLoop(int length, int maxDepth) throws KernelException {
        try (var fixture = new Fixture()) {
            Cycle cycle = createCycle(fixture, length);
            try (var cursor = fixture.undirectedMultiLoopCursor(cycle.start(), maxDepth)) {
                if (maxDepth < length) {
                    assertThat(toList(cursor.shortestPathIterator())).isEmpty();
                } else {
                    assertThat(toList(cursor.shortestPathIterator())).containsExactlyInAnyOrder(cycle.orientations());
                }
            }
        }
    }

    private record Cycle(long start, PathReference... orientations) {}

    // (n0) -> (n1) -> ... -> (n_length-1) -> (n0), n0 being the start node; a single self relationship at length one
    private static Cycle createCycle(Fixture fixture, int length) throws KernelException {
        Write write = fixture.tx.dataWrite();
        int type = fixture.tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
        long[] nodes = new long[length];
        for (int i = 0; i < length; i++) {
            nodes[i] = write.nodeCreate();
        }
        long[] rels = new long[length];
        for (int i = 0; i < length; i++) {
            rels[i] = write.relationshipCreate(nodes[i], type, nodes[(i + 1) % length]);
        }
        long[] forwardNodes = new long[length + 1];
        long[] backwardNodes = new long[length + 1];
        long[] backwardRels = new long[length];
        for (int i = 0; i <= length; i++) {
            forwardNodes[i] = nodes[i % length];
            backwardNodes[i] = nodes[(length - i) % length];
        }
        for (int i = 0; i < length; i++) {
            backwardRels[i] = rels[length - 1 - i];
        }
        PathReference forward = pathReference(forwardNodes, rels);
        PathReference backward = pathReference(backwardNodes, backwardRels);
        return length == 1 ? new Cycle(nodes[0], forward) : new Cycle(nodes[0], forward, backward);
    }

    private static final class NodeReadRecorder implements KernelReadTracer {
        private final MutableLongList nodes = LongLists.mutable.empty();

        @Override
        public void onNode(long nodeReference) {
            nodes.add(nodeReference);
        }

        @Override
        public void onAllNodesScan() {}

        @Override
        public void onLabelScan(int label) {}

        @Override
        public void onRelationshipTypeScan(int type) {}

        @Override
        public void onIndexSeek(IndexDescriptor index) {}

        @Override
        public void onRelationship(long relationshipReference) {}

        @Override
        public void onProperty(int propertyKey) {}

        @Override
        public void onHasLabel(int label) {}

        @Override
        public void onHasLabel() {}

        @Override
        public void dbHit() {}
    }

    private class Fixture implements AutoCloseable {
        private final KernelTransaction tx;
        private final NodeCursor nodeCursor;
        private final RelationshipTraversalCursor relCursor;

        public Fixture() throws KernelException {
            this.tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
            this.nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
            this.relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT);
        }

        public UndirectedShortestLoopCursor undirectedSingleLoopCursor(long start) {
            return new UndirectedSingleShortestLoopCursor(
                    start,
                    null,
                    Integer.MAX_VALUE,
                    tx.dataRead(),
                    nodeCursor,
                    relCursor,
                    LongPredicates.alwaysTrue(),
                    Predicates.alwaysTrue(),
                    EmptyMemoryTracker.INSTANCE);
        }

        public UndirectedShortestLoopCursor undirectedSingleLoopCursor(long start, int maxDepth) {
            return new UndirectedSingleShortestLoopCursor(
                    start,
                    null,
                    maxDepth,
                    tx.dataRead(),
                    nodeCursor,
                    relCursor,
                    LongPredicates.alwaysTrue(),
                    Predicates.alwaysTrue(),
                    EmptyMemoryTracker.INSTANCE);
        }

        public UndirectedShortestLoopCursor undirectedSingleLoopCursor(long start, int[] types) {
            return new UndirectedSingleShortestLoopCursor(
                    start,
                    types,
                    Integer.MAX_VALUE,
                    tx.dataRead(),
                    nodeCursor,
                    relCursor,
                    LongPredicates.alwaysTrue(),
                    Predicates.alwaysTrue(),
                    EmptyMemoryTracker.INSTANCE);
        }

        public UndirectedShortestLoopCursor undirectedSingleLoopCursor(long start, LongPredicate nodeFilter) {
            return new UndirectedSingleShortestLoopCursor(
                    start,
                    null,
                    Integer.MAX_VALUE,
                    tx.dataRead(),
                    nodeCursor,
                    relCursor,
                    nodeFilter,
                    Predicates.alwaysTrue(),
                    EmptyMemoryTracker.INSTANCE);
        }

        public UndirectedShortestLoopCursor undirectedSingleLoopCursor(long start, Predicate relFilter) {
            return new UndirectedSingleShortestLoopCursor(
                    start,
                    null,
                    Integer.MAX_VALUE,
                    tx.dataRead(),
                    nodeCursor,
                    relCursor,
                    LongPredicates.alwaysTrue(),
                    relFilter,
                    EmptyMemoryTracker.INSTANCE);
        }

        public UndirectedShortestLoopCursor undirectedSingleWalkLoopCursor(long start) {
            return new UndirectedSingleShortestLoopWalkCursor(
                    start,
                    null,
                    Integer.MAX_VALUE,
                    tx.dataRead(),
                    nodeCursor,
                    relCursor,
                    LongPredicates.alwaysTrue(),
                    Predicates.alwaysTrue(),
                    EmptyMemoryTracker.INSTANCE);
        }

        public UndirectedShortestLoopCursor undirectedMultiLoopCursor(long start) {
            return new UndirectedMultiShortestLoopCursor(
                    start,
                    null,
                    Integer.MAX_VALUE,
                    tx.dataRead(),
                    nodeCursor,
                    relCursor,
                    LongPredicates.alwaysTrue(),
                    Predicates.alwaysTrue(),
                    EmptyMemoryTracker.INSTANCE);
        }

        public UndirectedShortestLoopCursor undirectedMultiLoopCursor(long start, int maxDepth) {
            return new UndirectedMultiShortestLoopCursor(
                    start,
                    null,
                    maxDepth,
                    tx.dataRead(),
                    nodeCursor,
                    relCursor,
                    LongPredicates.alwaysTrue(),
                    Predicates.alwaysTrue(),
                    EmptyMemoryTracker.INSTANCE);
        }

        public UndirectedShortestLoopCursor undirectedMultiLoopWalkCursor(long start) {
            return new UndirectedShortestLoopWalkCursor(
                    start,
                    null,
                    Integer.MAX_VALUE,
                    tx.dataRead(),
                    nodeCursor,
                    relCursor,
                    LongPredicates.alwaysTrue(),
                    Predicates.alwaysTrue(),
                    EmptyMemoryTracker.INSTANCE);
        }

        public UndirectedShortestLoopCursor undirectedMultiLoopWalkCursor(long start, int maxDepth) {
            return new UndirectedShortestLoopWalkCursor(
                    start,
                    null,
                    maxDepth,
                    tx.dataRead(),
                    nodeCursor,
                    relCursor,
                    LongPredicates.alwaysTrue(),
                    Predicates.alwaysTrue(),
                    EmptyMemoryTracker.INSTANCE);
        }

        @Override
        public void close() throws KernelException {
            if (this.relCursor != null) this.relCursor.close();
            if (this.nodeCursor != null) this.nodeCursor.close();
            if (this.tx != null) this.tx.close();
        }
    }

    List<PathReference> toList(Iterator<PathReference> iterator) {
        var list = new ArrayList<PathReference>();
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        return list;
    }
}
