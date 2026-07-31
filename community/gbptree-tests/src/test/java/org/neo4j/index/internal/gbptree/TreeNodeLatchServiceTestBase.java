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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.test.Race.throwing;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.Test;
import org.neo4j.test.Race;

abstract class TreeNodeLatchServiceTestBase<SERVICE extends TreeNodeLatchService> extends LatchTestBase {
    abstract SERVICE newService();

    abstract int size(SERVICE service);

    @Test
    void shouldReturnSameLatchInstanceForStillLivingLatch() {
        var service = newService();
        long treeNodeId = 456L;
        TreeNodeLatch latch = service.latch(treeNodeId);

        TreeNodeLatch again = service.latch(treeNodeId);

        assertSame(again, latch);
    }

    @Test
    void shouldReturnNewLatchInstanceForDeadLatch() {
        var service = newService();
        long treeNodeId = 123L;
        TreeNodeLatch first = service.latch(treeNodeId);
        first.deref();

        TreeNodeLatch second = service.latch(treeNodeId);

        assertNotSame(second, first);
        second.deref();
        assertEquals(0, size(service));
    }

    @Test
    void shouldRefOnOneThreadAndDerefOnAnother() throws Exception {
        // given a long-lived reference, like the root layer's, released by whichever thread replaces it
        var service = newService();
        TreeNodeLatch held = service.latch(42);

        t2.execute(() -> {
            held.deref();
            return null;
        });
        assertEquals(0, size(service));
    }

    @Test
    void shouldMaintainWriteExclusion() {
        var service = newService();
        var race = new Race().withMaxDuration(2, TimeUnit.SECONDS);
        var idRange = 512;
        var concurrent = new AtomicInteger[idRange];
        for (var id = 0; id < idRange; id++) {
            concurrent[id] = new AtomicInteger();
        }
        race.addContestants(Runtime.getRuntime().availableProcessors(), throwing(() -> {
            var id = ThreadLocalRandom.current().nextInt(idRange);
            var latch = service.latch(id);
            latch.acquireWrite();
            assertThat(concurrent[id].incrementAndGet()).isOne();
            concurrent[id].decrementAndGet();
            latch.releaseWrite();
            latch.deref();
        }));

        race.goUnchecked();
    }

    @Test
    void shouldMaintainLatchProtocols() {
        var service = newService();
        var race = new Race().withMaxDuration(2, TimeUnit.SECONDS);
        var idRange = 512;
        var writersById = new AtomicInteger[idRange];
        for (var id = 0; id < idRange; id++) {
            writersById[id] = new AtomicInteger();
        }
        race.addContestants(2, throwing(() -> {
            var id = ThreadLocalRandom.current().nextInt(idRange);
            var latch = service.latch(id);
            latch.acquireRead();
            assertThat(writersById[id].get()).isZero();
            latch.releaseRead();
            latch.deref();
        }));
        race.addContestants(2, throwing(() -> {
            var id = ThreadLocalRandom.current().nextInt(idRange);
            var latch = service.latch(id);
            latch.acquireRead();
            if (latch.tryUpgradeToWrite()) {
                assertThat(writersById[id].incrementAndGet()).isOne();
                writersById[id].decrementAndGet();
                latch.releaseWrite();
            } else {
                latch.releaseRead();
            }
            latch.deref();
        }));
        race.addContestants(2, throwing(() -> {
            var id = ThreadLocalRandom.current().nextInt(idRange);
            var latch = service.latch(id);
            latch.acquireWrite();
            assertThat(writersById[id].incrementAndGet()).isOne();
            writersById[id].decrementAndGet();
            latch.releaseWrite();
            latch.deref();
        }));
        race.addContestants(2, throwing(() -> {
            var id = ThreadLocalRandom.current().nextInt(idRange);
            var latch = service.latch(id);
            latch.deref();
        }));

        race.goUnchecked();
    }

    @Test
    void shouldAcquireReadStressfully() throws Throwable {
        var service = newService();
        Race race = new Race().withMaxDuration(500, TimeUnit.MILLISECONDS);
        long treeNodeId = 5;
        LongAdder count = new LongAdder();
        race.addContestants(Runtime.getRuntime().availableProcessors(), () -> {
            TreeNodeLatch latch = service.latch(treeNodeId);
            latch.acquireRead();
            latch.releaseRead();
            latch.deref();
            count.add(1);
        });

        race.go();

        assertTrue(count.sum() > 0);
        assertEquals(0, size(service));
    }

    @Test
    void shouldAcquireWriteStressfully() throws Throwable {
        var service = newService();
        Race race = new Race().withMaxDuration(500, TimeUnit.MILLISECONDS);
        long treeNodeId = 5;
        LongAdder count = new LongAdder();
        race.addContestants(Runtime.getRuntime().availableProcessors(), () -> {
            TreeNodeLatch latch = service.latch(treeNodeId);
            latch.acquireWrite();
            latch.releaseWrite();
            latch.deref();
            count.add(1);
        });

        race.go();

        assertTrue(count.sum() > 0);
        assertEquals(0, size(service));
    }

    @Test
    void shouldAcquireAndReleaseReadsAndWritesStressfully() throws Throwable {
        var service = newService();
        Race race = new Race().withMaxDuration(500, TimeUnit.MILLISECONDS);
        AtomicLong reads = new AtomicLong();
        AtomicLong writes = new AtomicLong();
        race.addContestants(2, new Runnable() {
            private final ThreadLocalRandom random = ThreadLocalRandom.current();

            @Override
            public void run() {
                TreeNodeLatch latch = service.latch(random.nextLong(1, 100));
                latch.acquireRead();
                latch.releaseRead();
                latch.deref();
                reads.incrementAndGet();
            }
        });
        race.addContestants(2, new Runnable() {
            private final ThreadLocalRandom random = ThreadLocalRandom.current();

            @Override
            public void run() {
                TreeNodeLatch latch = service.latch(random.nextLong(1, 100));
                latch.acquireWrite();
                latch.releaseWrite();
                latch.deref();
                writes.incrementAndGet();
            }
        });
        race.go();

        assertEquals(0, size(service));
        assertTrue(reads.get() > 0);
        assertTrue(writes.get() > 0);
    }

    @Test
    void shouldAcquireSameWriteLatchConcurrently() {
        var service = newService();
        long id = 999;
        Race race = new Race().withEndCondition(() -> false);
        AtomicInteger concurrent = new AtomicInteger();
        race.addContestants(
                4,
                throwing(() -> {
                    TreeNodeLatch latch = service.latch(id);
                    latch.acquireWrite();
                    assertThat(concurrent.incrementAndGet()).isOne();
                    concurrent.decrementAndGet();
                    latch.releaseWrite();
                    latch.deref();
                }),
                1_000);
        race.goUnchecked();
    }

    @Test
    void shouldStressRandomAcquisitionsAndReleases() {
        var service = newService();
        var treeNodeId = 99;
        var reads = new AtomicInteger();
        var upgrades = new AtomicInteger();
        var writes = new AtomicInteger();
        var race = new Race()
                .withMaxDuration(5, TimeUnit.SECONDS)
                .withEndCondition(() -> reads.get() > 200_000 && writes.get() > 200_000 && upgrades.get() > 2_000);
        var numCurrentWriteOwners = new AtomicInteger();
        var numCurrentReadOwners = new AtomicInteger();
        // READ
        race.addContestants(2, () -> {
            var latch = service.latch(treeNodeId);
            latch.acquireRead();
            assertThat(numCurrentReadOwners.incrementAndGet()).isGreaterThanOrEqualTo(1);
            assertThat(numCurrentWriteOwners.get()).isZero();
            assertThat(numCurrentReadOwners.decrementAndGet()).isGreaterThanOrEqualTo(0);
            latch.releaseRead();
            latch.deref();
            reads.incrementAndGet();
        });
        // UPGRADE
        race.addContestant(() -> {
            var latch = service.latch(treeNodeId);
            latch.acquireRead();
            assertThat(numCurrentReadOwners.incrementAndGet()).isGreaterThanOrEqualTo(1);
            assertThat(numCurrentWriteOwners.get()).isZero();
            if (latch.tryUpgradeToWrite()) {
                assertThat(numCurrentWriteOwners.incrementAndGet()).isOne();
                assertThat(numCurrentReadOwners.decrementAndGet()).isZero();
                assertThat(numCurrentWriteOwners.decrementAndGet()).isZero();
                latch.releaseWrite();
                upgrades.incrementAndGet();
            } else {
                assertThat(numCurrentReadOwners.decrementAndGet()).isGreaterThanOrEqualTo(0);
                latch.releaseRead();
            }
            latch.deref();
        });
        // WRITE
        race.addContestant(() -> {
            var latch = service.latch(treeNodeId);
            latch.acquireWrite();
            assertThat(numCurrentWriteOwners.incrementAndGet()).isOne();
            assertThat(numCurrentWriteOwners.decrementAndGet()).isZero();
            latch.releaseWrite();
            latch.deref();
            writes.incrementAndGet();
        });

        race.goUnchecked();
    }
}
