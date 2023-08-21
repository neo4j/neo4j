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
package org.neo4j.io.pagecache.impl.muninn;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntLists;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.tracing.DummyPageSwapper;
import org.neo4j.util.concurrent.Futures;
import org.opentest4j.AssertionFailedError;

class SwapperSetTest {
    private SwapperSet set;

    @BeforeEach
    void setUp() {
        set = new SwapperSet();
    }

    @Test
    void mustReturnAllocationWithSwapper() {
        DummyPageSwapper a = new DummyPageSwapper("a", 42);
        DummyPageSwapper b = new DummyPageSwapper("b", 43);
        int idA = set.allocate(a);
        int idB = set.allocate(b);
        SwapperSet.SwapperMapping allocA = set.getAllocation(idA);
        SwapperSet.SwapperMapping allocB = set.getAllocation(idB);
        assertThat(allocA.swapper).isEqualTo(a);
        assertThat(allocB.swapper).isEqualTo(b);
    }

    @Test
    void accessingFreedAllocationMustReturnNull() {
        int id = set.allocate(new DummyPageSwapper("a", 42));
        set.free(id);
        assertNull(set.getAllocation(id));
    }

    @Test
    void doubleFreeMustThrow() {
        int id = set.allocate(new DummyPageSwapper("a", 42));
        set.free(id);
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> set.free(id));
        assertThat(exception.getMessage()).contains("double free");
    }

    @Test
    void freedCandidateIdsMustNotBeReusedBeforeVacuum() {
        PageSwapper swapper = new DummyPageSwapper("a", 42);
        MutableIntSet ids = new IntHashSet(10_000);
        for (int i = 0; i < 10_000; i++) {
            allocateFreeAndAssertNotReused(swapper, ids, i);
        }
    }

    @Test
    void freeCandidatesAllocationsMustBecomeAvailableAfterSweep() {
        MutableIntSet allocated = new IntHashSet();
        MutableIntSet freedCandidates = new IntHashSet();
        MutableIntSet reused = new IntHashSet();
        PageSwapper swapper = new DummyPageSwapper("a", 42);

        allocateAndAddTwentyThousand(allocated, swapper);

        allocated.forEach(id -> {
            set.postponedFree(id);
            freedCandidates.add(id);
        });
        set.sweep(any -> {});

        allocateAndAddTwentyThousand(reused, swapper);

        assertThat(allocated).isEqualTo(freedCandidates);
        assertThat(allocated).isEqualTo(reused);
    }

    @Test
    void sweepMustNotDustOffAnyIdsWhenNoneHaveBeenFreed() {
        PageSwapper swapper = new DummyPageSwapper("a", 42);
        for (int i = 0; i < 100; i++) {
            set.allocate(swapper);
        }
        MutableIntSet sweedIds = new IntHashSet();
        set.sweep(sweedIds::addAll);
        if (!sweedIds.isEmpty()) {
            throw new AssertionError("Vacuum found id " + sweedIds + " when it should have found nothing");
        }
    }

    @Test
    void mustNotUseZeroAsSwapperId() {
        PageSwapper swapper = new DummyPageSwapper("a", 42);
        for (int i = 0; i < 10_000; i++) {
            assertThat(set.allocate(swapper)).isNotZero();
        }
    }

    @Test
    void gettingAllocationZeroMustThrow() {
        assertThrows(IllegalArgumentException.class, () -> set.getAllocation((short) 0));
    }

    @Test
    void freeOfIdZeroMustThrow() {
        assertThrows(IllegalArgumentException.class, () -> set.free(0));
    }

    @Test
    void eventuallyBeEligableForIdSweep() {
        int id = 1;
        while (set.skipSweep()) {
            set.postponedFree(id++);
        }
        // and no influence on ids
        assertEquals(1, set.allocate(new DummyPageSwapper("any", 8)));
    }

    @Test
    void allocateDeallocateReuseSameIds() {
        var allocationIds = IntLists.mutable.withInitialCapacity(10 * 100);
        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 100; i++) {
                allocationIds.add(set.allocate(new DummyPageSwapper("b", 43)));
            }
            allocationIds.forEach(id -> set.free(id));
            allocationIds.clear();
        }
        // perfect reuse
        assertEquals(1, set.allocate(new DummyPageSwapper("c", 43)));
    }

    @Test
    void sweepReuseDelayedIds() {
        DummyPageSwapper swapper = new DummyPageSwapper("b", 43);
        while (set.skipSweep()) {
            set.postponedFree(set.allocate(swapper));
        }
        MutableIntSet observedIds = IntSets.mutable.empty();
        set.sweep(observedIds::addAll);

        for (int i = 1; i <= observedIds.size(); i++) {
            assertTrue(observedIds.contains(i), "Should contain whole range of ids");
        }

        assertEquals(1, set.allocate(swapper));
    }

    @RepeatedTest(10)
    void concurrentSweepAttemptsShouldNotFreeIdsMultipleTimes() throws ExecutionException {
        int numberOfExecutors = 20;
        ExecutorService executors = Executors.newFixedThreadPool(numberOfExecutors);
        try {
            DummyPageSwapper swapper = new DummyPageSwapper("b", 43);
            while (set.skipSweep()) {
                set.postponedFree(set.allocate(swapper));
            }

            var latch = new CountDownLatch(1);
            var results = new ArrayList<Future<MutableIntSet>>();
            for (int i = 0; i < numberOfExecutors; i++) {
                results.add(executors.submit(() -> {
                    latch.await();
                    MutableIntSet observedIds = IntSets.mutable.empty();
                    set.sweep(observedIds::addAll);
                    return observedIds;
                }));
                latch.countDown();
            }
            Futures.getAll(results);

            assertThat(results).satisfies(futures -> {
                MutableIntSet nonEmptyResult = null;
                for (Future<MutableIntSet> future : futures) {
                    try {
                        MutableIntSet ids = future.get();
                        if (!ids.isEmpty()) {
                            if (nonEmptyResult == null) {
                                nonEmptyResult = ids;
                            } else {
                                throw new AssertionFailedError(
                                        "Expected to see only single non empty result set but got at least 2: "
                                                + nonEmptyResult + ", and " + ids);
                            }
                        }
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
                assertNotNull(nonEmptyResult);
            });
        } finally {
            executors.shutdown();
        }
    }

    private void allocateAndAddTwentyThousand(MutableIntSet allocated, PageSwapper swapper) {
        for (int i = 0; i < 20_000; i++) {
            allocateAndAdd(allocated, swapper);
        }
    }

    private void allocateAndAdd(MutableIntSet allocated, PageSwapper swapper) {
        int id = set.allocate(swapper);
        allocated.add(id);
    }

    private void allocateFreeAndAssertNotReused(PageSwapper swapper, MutableIntSet ids, int i) {
        int id = set.allocate(swapper);
        set.postponedFree(id);
        if (!ids.add(id)) {
            fail("Expected ids.add( id ) to return true for id " + id + " in iteration " + i
                    + " but it instead returned false");
        }
    }
}
