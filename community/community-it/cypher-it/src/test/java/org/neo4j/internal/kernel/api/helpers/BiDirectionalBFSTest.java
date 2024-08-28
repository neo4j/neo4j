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
import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.internal.helpers.collection.Iterators.single;
import static org.neo4j.internal.kernel.api.helpers.traversal.BiDirectionalBFS.newEmptyBiDirectionalBFS;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.api.KernelTransaction.Type.EXPLICIT;
import static org.neo4j.values.virtual.VirtualValues.pathReference;

import org.eclipse.collections.impl.block.factory.primitive.LongPredicates;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
@Timeout(value = 10)
class BiDirectionalBFSTest {

    private static final EmptyMemoryTracker NO_TRACKING = EmptyMemoryTracker.INSTANCE;

    @Inject
    private Kernel kernel;

    @Test
    void shouldFindSimpleOutgoingPaths() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //           (a1) → (b1)
            //        ↗  (a2) → (b2)
            // (start) → (a3) → (b3)
            //        ↘  (a4) → (b4)
            //           (a5) → (b5)
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
            long startToA1 = write.relationshipCreate(start, rel, a1);
            long startToA2 = write.relationshipCreate(start, rel, a2);
            long startToA3 = write.relationshipCreate(start, rel, a3);
            long startToA4 = write.relationshipCreate(start, rel, a4);
            long startToA5 = write.relationshipCreate(start, rel, a5);
            // layer 2
            long a1ToB1 = write.relationshipCreate(a1, rel, b1);
            long a2ToB2 = write.relationshipCreate(a2, rel, b2);
            long a3ToB3 = write.relationshipCreate(a3, rel, b3);
            long a4ToB4 = write.relationshipCreate(a4, rel, b4);
            long a5ToB5 = write.relationshipCreate(a5, rel, b5);

            var bfs = newEmptyBiDirectionalBFS(
                    null, Direction.OUTGOING, 10, true, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING, true, false);

            // (start)-->(a1)-->(b1)
            bfs.resetForNewRow(start, b1, LongPredicates.alwaysTrue(), Predicates.alwaysTrue());
            assertThat(single(bfs.shortestPathIterator()))
                    .isEqualTo(pathReference(new long[] {start, a1, b1}, new long[] {startToA1, a1ToB1}));
            // (start)-->(a2)-->(b2)
            bfs.resetForNewRow(start, b2, LongPredicates.alwaysTrue(), Predicates.alwaysTrue());
            assertThat(single(bfs.shortestPathIterator()))
                    .isEqualTo(pathReference(new long[] {start, a2, b2}, new long[] {startToA2, a2ToB2}));
            // (start)-->(a3)-->(b3)
            bfs.resetForNewRow(start, b3, LongPredicates.alwaysTrue(), Predicates.alwaysTrue());
            assertThat(single(bfs.shortestPathIterator()))
                    .isEqualTo(pathReference(new long[] {start, a3, b3}, new long[] {startToA3, a3ToB3}));
            // (start)-->(a4)-->(b4)
            bfs.resetForNewRow(start, b4, LongPredicates.alwaysTrue(), Predicates.alwaysTrue());
            assertThat(single(bfs.shortestPathIterator()))
                    .isEqualTo(pathReference(new long[] {start, a4, b4}, new long[] {startToA4, a4ToB4}));
            // (start)-->(a5)-->(b5)
            bfs.resetForNewRow(start, b5, LongPredicates.alwaysTrue(), Predicates.alwaysTrue());
            assertThat(single(bfs.shortestPathIterator()))
                    .isEqualTo(pathReference(new long[] {start, a5, b5}, new long[] {startToA5, a5ToB5}));
        }
    }

    @Test
    void shouldNotFindSimpleIncomingPaths() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //           (a1) → (b1)
            //        ↗  (a2) → (b2)
            // (start) → (a3) → (b3)
            //        ↘  (a4) → (b4)
            //           (a5) → (b5)
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

            var bfs = newEmptyBiDirectionalBFS(
                    null, Direction.INCOMING, 10, true, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING, true, false);

            // (start)-->(a1)-->(b1)
            bfs.resetForNewRow(start, b1, LongPredicates.alwaysTrue(), Predicates.alwaysTrue());
            assertThat(bfs.shortestPathIterator()).isExhausted();
            // (start)-->(a2)-->(b2)
            bfs.resetForNewRow(start, b2, LongPredicates.alwaysTrue(), Predicates.alwaysTrue());
            assertThat(bfs.shortestPathIterator()).isExhausted();
            // (start)-->(a3)-->(b3)
            bfs.resetForNewRow(start, b3, LongPredicates.alwaysTrue(), Predicates.alwaysTrue());
            assertThat(bfs.shortestPathIterator()).isExhausted();
            // (start)-->(a4)-->(b4)
            bfs.resetForNewRow(start, b4, LongPredicates.alwaysTrue(), Predicates.alwaysTrue());
            assertThat(bfs.shortestPathIterator()).isExhausted();
            // (start)-->(a5)-->(b5)
            bfs.resetForNewRow(start, b5, LongPredicates.alwaysTrue(), Predicates.alwaysTrue());
            assertThat(bfs.shortestPathIterator()).isExhausted();
        }
    }

    @Test
    void shouldFindSimpleBiDirectionalPaths() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //           (a1) → (b1)
            //        ↗  (a2) → (b2)
            // (start) → (a3) → (b3)
            //        ↘  (a4) → (b4)
            //           (a5) → (b5)
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
            long startToA1 = write.relationshipCreate(start, rel, a1);
            long startToA2 = write.relationshipCreate(start, rel, a2);
            long startToA3 = write.relationshipCreate(start, rel, a3);
            long startToA4 = write.relationshipCreate(start, rel, a4);
            long startToA5 = write.relationshipCreate(start, rel, a5);
            // layer 2
            long a1ToB1 = write.relationshipCreate(a1, rel, b1);
            long a2ToB2 = write.relationshipCreate(a2, rel, b2);
            long a3ToB3 = write.relationshipCreate(a3, rel, b3);
            long a4ToB4 = write.relationshipCreate(a4, rel, b4);
            long a5ToB5 = write.relationshipCreate(a5, rel, b5);

            var bfs = newEmptyBiDirectionalBFS(
                    null, Direction.BOTH, 10, true, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING, true, false);

            // (start)-->(a1)-->(b1)
            bfs.resetForNewRow(start, b1, LongPredicates.alwaysTrue(), Predicates.alwaysTrue());
            assertThat(single(bfs.shortestPathIterator()))
                    .isEqualTo(pathReference(new long[] {start, a1, b1}, new long[] {startToA1, a1ToB1}));
            // (start)-->(a2)-->(b2)
            bfs.resetForNewRow(start, b2, LongPredicates.alwaysTrue(), Predicates.alwaysTrue());
            assertThat(single(bfs.shortestPathIterator()))
                    .isEqualTo(pathReference(new long[] {start, a2, b2}, new long[] {startToA2, a2ToB2}));
            // (start)-->(a3)-->(b3)
            bfs.resetForNewRow(start, b3, LongPredicates.alwaysTrue(), Predicates.alwaysTrue());
            assertThat(single(bfs.shortestPathIterator()))
                    .isEqualTo(pathReference(new long[] {start, a3, b3}, new long[] {startToA3, a3ToB3}));
            // (start)-->(a4)-->(b4)
            bfs.resetForNewRow(start, b4, LongPredicates.alwaysTrue(), Predicates.alwaysTrue());
            assertThat(single(bfs.shortestPathIterator()))
                    .isEqualTo(pathReference(new long[] {start, a4, b4}, new long[] {startToA4, a4ToB4}));
            // (start)-->(a5)-->(b5)
            bfs.resetForNewRow(start, b5, LongPredicates.alwaysTrue(), Predicates.alwaysTrue());
            assertThat(single(bfs.shortestPathIterator()))
                    .isEqualTo(pathReference(new long[] {start, a5, b5}, new long[] {startToA5, a5ToB5}));
        }
    }

    @Test
    void shouldFilterNode() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //           (a1) → (b1)
            //        ↗  (a2) → (b2)
            // (start) → (a3) → (b3)
            //        ↘  (a4) → (b4)
            //           (a5) → (b5)
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
            long startToA1 = write.relationshipCreate(start, rel, a1);
            write.relationshipCreate(start, rel, a2);
            write.relationshipCreate(start, rel, a3);
            write.relationshipCreate(start, rel, a4);
            write.relationshipCreate(start, rel, a5);
            // layer 2
            long a1ToB1 = write.relationshipCreate(a1, rel, b1);
            write.relationshipCreate(a2, rel, b2);
            write.relationshipCreate(a3, rel, b3);
            write.relationshipCreate(a4, rel, b4);
            write.relationshipCreate(a5, rel, b5);

            var bfs = newEmptyBiDirectionalBFS(
                    null, Direction.OUTGOING, 10, true, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING, true, false);

            // (start)-->(a1)-->(b1)
            bfs.resetForNewRow(start, b1, n -> n != a1, Predicates.alwaysTrue());
            assertThat(bfs.shortestPathIterator()).isExhausted();
            bfs.resetForNewRow(start, b1, n -> n != b5, Predicates.alwaysTrue());
            assertThat(single(bfs.shortestPathIterator()))
                    .isEqualTo(pathReference(new long[] {start, a1, b1}, new long[] {startToA1, a1ToB1}));
        }
    }

    @Test
    void shouldFilterRelationship() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //           (a1) → (b1)
            //        ↗  (a2) → (b2)
            // (start) → (a3) → (b3)
            //        ↘  (a4) → (b4)
            //           (a5) → (b5)
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
            long startToA1 = write.relationshipCreate(start, rel, a1);
            write.relationshipCreate(start, rel, a2);
            write.relationshipCreate(start, rel, a3);
            write.relationshipCreate(start, rel, a4);
            write.relationshipCreate(start, rel, a5);
            // layer 2
            long a1ToB1 = write.relationshipCreate(a1, rel, b1);
            long a2ToB2 = write.relationshipCreate(a2, rel, b2);
            write.relationshipCreate(a3, rel, b3);
            write.relationshipCreate(a4, rel, b4);
            write.relationshipCreate(a5, rel, b5);

            var bfs = newEmptyBiDirectionalBFS(
                    null, Direction.BOTH, 10, true, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING, true, false);

            // (start)-->(a1)-->(b1)
            bfs.resetForNewRow(start, b1, LongPredicates.alwaysTrue(), r -> r.relationshipReference() != a1ToB1);
            assertThat(bfs.shortestPathIterator()).isExhausted();
            bfs.resetForNewRow(start, b1, LongPredicates.alwaysTrue(), r -> r.relationshipReference() != startToA1);
            assertThat(bfs.shortestPathIterator()).isExhausted();
            bfs.resetForNewRow(start, b1, LongPredicates.alwaysTrue(), r -> r.relationshipReference() != a2ToB2);
            assertThat(single(bfs.shortestPathIterator()))
                    .isEqualTo(pathReference(new long[] {start, a1, b1}, new long[] {startToA1, a1ToB1}));
        }
    }

    @Test
    void shouldRespectMaxLength() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //           (a1) → (b1)
            //        ↗  (a2) → (b2)
            // (start) → (a3) → (b3)
            //        ↘  (a4) → (b4)
            //           (a5) → (b5)
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
            long startToA1 = write.relationshipCreate(start, rel, a1);
            write.relationshipCreate(start, rel, a2);
            write.relationshipCreate(start, rel, a3);
            write.relationshipCreate(start, rel, a4);
            write.relationshipCreate(start, rel, a5);
            // layer 2
            long a1ToB1 = write.relationshipCreate(a1, rel, b1);
            long a2ToB2 = write.relationshipCreate(a2, rel, b2);
            write.relationshipCreate(a3, rel, b3);
            write.relationshipCreate(a4, rel, b4);
            write.relationshipCreate(a5, rel, b5);

            var bfs = newEmptyBiDirectionalBFS(
                    null, Direction.BOTH, 1, true, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING, true, false);

            // (start)-->(a1)-->(b1)
            bfs.resetForNewRow(start, b1, LongPredicates.alwaysTrue(), Predicates.alwaysTrue());
            assertThat(bfs.shortestPathIterator()).isExhausted();
        }
    }

    @Test
    void shouldOnlyFindShortestPath() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //           (a1) → (b1)
            //        ↗           ↓
            // (start) → (a2) → (b2)
            //        ↘           ↑
            //           (a3) → (b3)
            long start = write.nodeCreate();
            // layer 1
            long a1 = write.nodeCreate();
            long a2 = write.nodeCreate();
            long a3 = write.nodeCreate();
            // layer 2
            long b1 = write.nodeCreate();
            long b2 = write.nodeCreate();
            long b3 = write.nodeCreate();

            // layer 1
            write.relationshipCreate(start, rel, a1);
            long startToA2 = write.relationshipCreate(start, rel, a2);
            write.relationshipCreate(start, rel, a3);
            // layer 2
            write.relationshipCreate(a1, rel, b1);
            long a2ToB2 = write.relationshipCreate(a2, rel, b2);
            write.relationshipCreate(a3, rel, b3);
            // layer 3
            write.relationshipCreate(b1, rel, b2);
            write.relationshipCreate(b3, rel, b1);

            var bfs = newEmptyBiDirectionalBFS(
                    null, Direction.OUTGOING, 10, true, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING, true, false);

            bfs.resetForNewRow(start, b2, LongPredicates.alwaysTrue(), Predicates.alwaysTrue());
            assertThat(single(bfs.shortestPathIterator()))
                    .isEqualTo(pathReference(new long[] {start, a2, b2}, new long[] {startToA2, a2ToB2}));

            bfs = newEmptyBiDirectionalBFS(
                    null, Direction.INCOMING, 10, true, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING, true, false);
            bfs.resetForNewRow(start, b2, LongPredicates.alwaysTrue(), Predicates.alwaysTrue());
            assertThat(bfs.shortestPathIterator()).isExhausted();
            bfs = newEmptyBiDirectionalBFS(
                    null, Direction.BOTH, 10, true, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING, true, false);
            bfs.resetForNewRow(start, b2, LongPredicates.alwaysTrue(), Predicates.alwaysTrue());
            assertThat(single(bfs.shortestPathIterator()))
                    .isEqualTo(pathReference(new long[] {start, a2, b2}, new long[] {startToA2, a2ToB2}));
        }
    }

    @Test
    void shouldFindMultiplePaths() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //           (a1)
            //        ↗       ↘
            // (start) → (a2) → (end)
            //        ↘       ↗
            //           (a3)
            long start = write.nodeCreate();
            // layer 1
            long a1 = write.nodeCreate();
            long a2 = write.nodeCreate();
            long a3 = write.nodeCreate();
            // layer 2
            long end = write.nodeCreate();

            // layer 1
            long startToA1 = write.relationshipCreate(start, rel, a1);
            long startToA2 = write.relationshipCreate(start, rel, a2);
            long startToA3 = write.relationshipCreate(start, rel, a3);
            // layer 2
            long a1ToEnd = write.relationshipCreate(a1, rel, end);
            long a2ToEnd = write.relationshipCreate(a2, rel, end);
            long a3ToEnd = write.relationshipCreate(a3, rel, end);

            var bfsMulti = newEmptyBiDirectionalBFS(
                    null,
                    Direction.OUTGOING,
                    10,
                    true,
                    tx.dataRead(),
                    nodeCursor,
                    relCursor,
                    NO_TRACKING,
                    false,
                    false);

            bfsMulti.resetForNewRow(start, end, LongPredicates.alwaysTrue(), Predicates.alwaysTrue());
            assertThat(asList(bfsMulti.shortestPathIterator()))
                    .containsExactlyInAnyOrder(
                            pathReference(new long[] {start, a1, end}, new long[] {startToA1, a1ToEnd}),
                            pathReference(new long[] {start, a2, end}, new long[] {startToA2, a2ToEnd}),
                            pathReference(new long[] {start, a3, end}, new long[] {startToA3, a3ToEnd}));

            var bfsSingle = newEmptyBiDirectionalBFS(
                    null, Direction.OUTGOING, 10, true, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING, true, false);

            bfsSingle.resetForNewRow(start, end, LongPredicates.alwaysTrue(), Predicates.alwaysTrue());
            assertThat(single(bfsSingle.shortestPathIterator()))
                    .isIn(
                            pathReference(new long[] {start, a1, end}, new long[] {startToA1, a1ToEnd}),
                            pathReference(new long[] {start, a2, end}, new long[] {startToA2, a2ToEnd}),
                            pathReference(new long[] {start, a3, end}, new long[] {startToA3, a3ToEnd}));
        }
    }

    @Test
    void shouldRespectTypes() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel1 = tx.tokenWrite().relationshipTypeGetOrCreateForName("R1");
            int rel2 = tx.tokenWrite().relationshipTypeGetOrCreateForName("R2");
            int rel3 = tx.tokenWrite().relationshipTypeGetOrCreateForName("R3");
            //           (a1)
            //        ↗       ↘
            // (start) → (a2) → (end)
            //        ↘       ↗
            //           (a3)
            long start = write.nodeCreate();
            // layer 1
            long a1 = write.nodeCreate();
            long a2 = write.nodeCreate();
            long a3 = write.nodeCreate();
            // layer 2
            long end = write.nodeCreate();

            // layer 1
            long startToA1 = write.relationshipCreate(start, rel1, a1);
            long startToA2 = write.relationshipCreate(start, rel2, a2);
            long startToA3 = write.relationshipCreate(start, rel3, a3);
            // layer 2
            long a1ToEnd = write.relationshipCreate(a1, rel1, end);
            long a2ToEnd = write.relationshipCreate(a2, rel2, end);
            long a3ToEnd = write.relationshipCreate(a3, rel3, end);

            var bfsR1 = newEmptyBiDirectionalBFS(
                    new int[] {rel1},
                    Direction.OUTGOING,
                    10,
                    true,
                    tx.dataRead(),
                    nodeCursor,
                    relCursor,
                    NO_TRACKING,
                    false,
                    false);

            bfsR1.resetForNewRow(start, end, LongPredicates.alwaysTrue(), Predicates.alwaysTrue());
            assertThat(single(bfsR1.shortestPathIterator()))
                    .isEqualTo(pathReference(new long[] {start, a1, end}, new long[] {startToA1, a1ToEnd}));

            var bfsR2 = newEmptyBiDirectionalBFS(
                    new int[] {rel2},
                    Direction.OUTGOING,
                    10,
                    true,
                    tx.dataRead(),
                    nodeCursor,
                    relCursor,
                    NO_TRACKING,
                    false,
                    false);
            bfsR2.resetForNewRow(start, end, LongPredicates.alwaysTrue(), Predicates.alwaysTrue());
            assertThat(single(bfsR2.shortestPathIterator()))
                    .isEqualTo(pathReference(new long[] {start, a2, end}, new long[] {startToA2, a2ToEnd}));

            var bfsR3 = newEmptyBiDirectionalBFS(
                    new int[] {rel3},
                    Direction.OUTGOING,
                    10,
                    true,
                    tx.dataRead(),
                    nodeCursor,
                    relCursor,
                    NO_TRACKING,
                    false,
                    false);
            bfsR3.resetForNewRow(start, end, LongPredicates.alwaysTrue(), Predicates.alwaysTrue());
            assertThat(single(bfsR3.shortestPathIterator()))
                    .isEqualTo(pathReference(new long[] {start, a3, end}, new long[] {startToA3, a3ToEnd}));
        }
    }

    @Test
    void shouldHandleDirectedSelfLoops() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            long a = write.nodeCreate();
            long r = write.relationshipCreate(a, rel, a);

            var outgoingBFS = newEmptyBiDirectionalBFS(
                    null,
                    Direction.OUTGOING,
                    10,
                    true,
                    tx.dataRead(),
                    nodeCursor,
                    relCursor,
                    NO_TRACKING,
                    false,
                    false);
            outgoingBFS.resetForNewRow(a, a, LongPredicates.alwaysTrue(), Predicates.alwaysTrue());
            assertThat(single(outgoingBFS.shortestPathIterator()))
                    .isEqualTo(pathReference(new long[] {a, a}, new long[] {r}));

            var incomingBFS = newEmptyBiDirectionalBFS(
                    null,
                    Direction.INCOMING,
                    10,
                    true,
                    tx.dataRead(),
                    nodeCursor,
                    relCursor,
                    NO_TRACKING,
                    false,
                    false);
            incomingBFS.resetForNewRow(a, a, LongPredicates.alwaysTrue(), Predicates.alwaysTrue());
            assertThat(single(incomingBFS.shortestPathIterator()))
                    .isEqualTo(pathReference(new long[] {a, a}, new long[] {r}));

            // This special case works
            var undirectedBFS = newEmptyBiDirectionalBFS(
                    null, Direction.BOTH, 10, true, tx.dataRead(), nodeCursor, relCursor, NO_TRACKING, false, false);
            undirectedBFS.resetForNewRow(a, a, LongPredicates.alwaysTrue(), Predicates.alwaysTrue());
            assertThat(single(undirectedBFS.shortestPathIterator()))
                    .isEqualTo(pathReference(new long[] {a, a}, new long[] {r}));
        }
    }

    @Test
    void shouldHandleDirectedTwoNodeLoops() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            long a = write.nodeCreate();
            long b = write.nodeCreate();
            long aToB = write.relationshipCreate(a, rel, b);
            long bToA = write.relationshipCreate(b, rel, a);

            var outgoingBFS = newEmptyBiDirectionalBFS(
                    null,
                    Direction.OUTGOING,
                    10,
                    true,
                    tx.dataRead(),
                    nodeCursor,
                    relCursor,
                    NO_TRACKING,
                    false,
                    false);
            outgoingBFS.resetForNewRow(a, a, LongPredicates.alwaysTrue(), Predicates.alwaysTrue());
            assertThat(single(outgoingBFS.shortestPathIterator()))
                    .isEqualTo(pathReference(new long[] {a, b, a}, new long[] {aToB, bToA}));

            var incomingBFS = newEmptyBiDirectionalBFS(
                    null,
                    Direction.INCOMING,
                    10,
                    true,
                    tx.dataRead(),
                    nodeCursor,
                    relCursor,
                    NO_TRACKING,
                    false,
                    false);
            incomingBFS.resetForNewRow(a, a, LongPredicates.alwaysTrue(), Predicates.alwaysTrue());
            assertThat(single(incomingBFS.shortestPathIterator()))
                    .isEqualTo(pathReference(new long[] {a, b, a}, new long[] {bToA, aToB}));
        }
    }

    @Test
    void shouldFindDirectedLoops() throws KernelException {
        // given
        try (var tx = kernel.beginTransaction(EXPLICIT, AUTH_DISABLED);
                var nodeCursor = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                var relCursor = tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            int rel = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            //           (a1)
            //         ↗       ↘
            // (start) → (a2) → (b1)
            //         ↖       ↙
            //           (a3)
            long start = write.nodeCreate();
            // layer 1
            long a1 = write.nodeCreate();
            long a2 = write.nodeCreate();
            long a3 = write.nodeCreate();
            // layer 2
            long b1 = write.nodeCreate();

            // layer 1
            long startToA1 = write.relationshipCreate(start, rel, a1);
            long startToA2 = write.relationshipCreate(start, rel, a2);
            long a3ToStart = write.relationshipCreate(a3, rel, start);
            // layer 2
            long a1ToB1 = write.relationshipCreate(a1, rel, b1);
            long a2ToB1 = write.relationshipCreate(a2, rel, b1);
            long b1ToA3 = write.relationshipCreate(b1, rel, a3);

            var outgoingBFS = newEmptyBiDirectionalBFS(
                    null,
                    Direction.OUTGOING,
                    10,
                    true,
                    tx.dataRead(),
                    nodeCursor,
                    relCursor,
                    NO_TRACKING,
                    false,
                    false);

            outgoingBFS.resetForNewRow(start, start, LongPredicates.alwaysTrue(), Predicates.alwaysTrue());
            assertThat(asList(outgoingBFS.shortestPathIterator()))
                    .containsExactlyInAnyOrder(
                            pathReference(
                                    new long[] {start, a1, b1, a3, start},
                                    new long[] {startToA1, a1ToB1, b1ToA3, a3ToStart}),
                            pathReference(
                                    new long[] {start, a2, b1, a3, start},
                                    new long[] {startToA2, a2ToB1, b1ToA3, a3ToStart}));

            var incomingBFS = newEmptyBiDirectionalBFS(
                    null,
                    Direction.INCOMING,
                    10,
                    true,
                    tx.dataRead(),
                    nodeCursor,
                    relCursor,
                    NO_TRACKING,
                    false,
                    false);

            incomingBFS.resetForNewRow(start, start, LongPredicates.alwaysTrue(), Predicates.alwaysTrue());
            assertThat(asList(incomingBFS.shortestPathIterator()))
                    .containsExactlyInAnyOrder(
                            pathReference(
                                    new long[] {start, a3, b1, a1, start},
                                    new long[] {a3ToStart, b1ToA3, a1ToB1, startToA1}),
                            pathReference(
                                    new long[] {start, a3, b1, a2, start},
                                    new long[] {a3ToStart, b1ToA3, a2ToB1, startToA2}));
        }
    }
}
