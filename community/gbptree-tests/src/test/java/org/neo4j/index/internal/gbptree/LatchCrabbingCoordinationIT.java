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
package org.neo4j.index.internal.gbptree;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.test.Barrier;
import org.neo4j.test.OtherThreadExecutor;

public class LatchCrabbingCoordinationIT {
    private static final int AVAILABLE_SPACE = 1_000;
    private static final int MERGE_THRESHOLD = AVAILABLE_SPACE / 2;

    @Test
    void shouldNotNeedResetIfNoOtherWritersDoPessimisticOperations() throws Exception {
        // given
        var latchService = new TreeNodeLatchService();
        try (var mainCoordination = coordination(latchService)) {
            mainCoordination.beginOperation();
            assertThat(goDownTree(mainCoordination, 10, 11, 12)).isTrue();

            try (var t2 = new OtherThreadExecutor("T2")) {
                // when
                var barrier = new Barrier.Control();
                var otherWrite = t2.executeDontWait(() -> {
                    try (var otherCoordination = coordination(latchService)) {
                        otherCoordination.beginOperation();
                        assertThat(goDownTree(otherCoordination, 10, 11, 13 /*a different leaf than main*/))
                                .isTrue();
                        barrier.reached();
                    }
                    return null;
                });

                barrier.await();

                // then
                assertThat(mainCoordination.checkForceReset()).isFalse();
                barrier.release();
                otherWrite.get();
            }
        }
    }

    @Test
    void shouldResetIfOtherWritersDoPessimisticOperations() throws Exception {
        var latchService = new TreeNodeLatchService();
        try (var mainCoordination = coordination(latchService)) {
            mainCoordination.beginOperation();
            assertThat(goDownTree(mainCoordination, 10, 11, 12)).isTrue();

            try (var t2 = new OtherThreadExecutor("T2")) {
                // when
                var barrier = new Barrier.Control();
                var otherWrite = t2.executeDontWait(() -> {
                    try (var otherCoordination = coordination(latchService)) {
                        otherCoordination.beginOperation();
                        otherCoordination.flipToPessimisticMode();
                        barrier.reached();
                        assertThat(goDownTree(otherCoordination, 10, 11, 13)).isTrue();
                    }
                    return null;
                });

                barrier.await();
                barrier.release();
                // And additionally wait for the other thread awaiting the write latch
                t2.waitUntilWaiting(details -> details.isAt(LongSpinLatch.class, "acquireWrite"));

                // then
                assertThat(mainCoordination.checkForceReset()).isTrue();
                mainCoordination.reset();
                otherWrite.get();
            }
        }
    }

    private boolean goDownTree(LatchCrabbingCoordination coordination, long... path) {
        for (int i = 0; i < path.length; i++) {
            coordination.beforeTraversingToChild(path[i], 1);
            if (!coordination.arrivedAtChild(i + 1 < path.length, AVAILABLE_SPACE, false, 100)) {
                return false;
            }
        }
        return true;
    }

    private LatchCrabbingCoordination coordination(TreeNodeLatchService latchService) {
        var coordination = new LatchCrabbingCoordination(latchService, MERGE_THRESHOLD);
        coordination.initialize(mock(PageCursor.class));
        return coordination;
    }
}
