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
import org.eclipse.collections.impl.block.factory.primitive.LongPredicates;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.Predicates;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalEntities;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.helpers.traversal.shortestloop.UndirectedMultiShortestLoopCursor;
import org.neo4j.internal.kernel.api.helpers.traversal.shortestloop.UndirectedShortestLoopCursor;
import org.neo4j.internal.kernel.api.helpers.traversal.shortestloop.UndirectedShortestLoopWalkCursor;
import org.neo4j.internal.kernel.api.helpers.traversal.shortestloop.UndirectedSingleShortestLoopCursor;
import org.neo4j.internal.kernel.api.helpers.traversal.shortestloop.UndirectedSingleShortestLoopWalkCursor;
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
                                        new long[] {startToA, aToE, eToF, fToG, gToH, hToI, iToK, kToL, lToStart}),
                                assertThat(cursor.next()).isFalse());
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
