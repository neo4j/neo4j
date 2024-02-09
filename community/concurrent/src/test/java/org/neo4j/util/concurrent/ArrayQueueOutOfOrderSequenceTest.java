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
package org.neo4j.util.concurrent;

import static java.lang.Integer.max;
import static java.lang.Thread.sleep;
import static java.util.Arrays.copyOfRange;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongFunction;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.Test;

class ArrayQueueOutOfOrderSequenceTest {
    private final long[] EMPTY_META = new long[] {42L};

    @Test
    void shouldExposeGapFreeSequenceSingleThreaded() {
        // GIVEN
        OutOfOrderSequence sequence = new ArrayQueueOutOfOrderSequence(0L, 10, new long[1]);

        // WHEN/THEN
        sequence.offer(1, new long[] {1});
        assertGet(sequence, 1, new long[] {1});

        sequence.offer(2, new long[] {2});
        assertGet(sequence, 2, new long[] {2});

        sequence.offer(4, new long[] {3});
        assertGet(sequence, 2, new long[] {2});

        sequence.offer(3, new long[] {4});
        assertGet(sequence, 4, new long[] {3});

        sequence.offer(5, new long[] {5});
        assertGet(sequence, 5, new long[] {5});

        // AND WHEN/THEN
        sequence.offer(10, new long[] {6});
        sequence.offer(11, new long[] {7});
        sequence.offer(8, new long[] {8});
        sequence.offer(9, new long[] {9});
        sequence.offer(7, new long[] {10});
        assertGet(sequence, 5, new long[] {5});
        sequence.offer(6, new long[] {11});
        assertGet(sequence, 11L, new long[] {7});
    }

    @Test
    void shouldExtendArrayIfNeedBe() {
        // GIVEN
        OutOfOrderSequence sequence = new ArrayQueueOutOfOrderSequence(0L, 5, new long[1]);

        sequence.offer(3L, new long[] {0});
        sequence.offer(2L, new long[] {1});
        sequence.offer(5L, new long[] {2});
        sequence.offer(4L, new long[] {3});

        // WHEN offering a number that should result in extending the array
        sequence.offer(6L, new long[] {4});
        // and WHEN offering the missing number to fill the gap
        sequence.offer(1L, new long[] {5});

        // THEN the high number should be visible
        assertGet(sequence, 6L, new long[] {4});
    }

    @Test
    void closingLastGapAfterArrayExtension() {
        // GIVEN
        OutOfOrderSequence sequence = new ArrayQueueOutOfOrderSequence(0, 5, new long[1]);
        assertTrue(sequence.offer(1, new long[] {0}));
        assertFalse(sequence.offer(3, new long[] {0}));
        assertFalse(sequence.offer(4, new long[] {0}));
        assertTrue(sequence.offer(2, new long[] {0}));
        assertFalse(sequence.offer(6, new long[] {0}));
        assertTrue(sequence.offer(5, new long[] {0}));
        // leave out 7
        assertFalse(sequence.offer(8, new long[] {0}));
        assertFalse(sequence.offer(9, new long[] {0}));
        assertFalse(sequence.offer(10, new long[] {0}));
        assertFalse(sequence.offer(11, new long[] {0}));
        // putting 12 should need extending the backing queue array
        assertFalse(sequence.offer(12, new long[] {0}));
        assertFalse(sequence.offer(13, new long[] {0}));
        assertFalse(sequence.offer(14, new long[] {0}));

        // WHEN finally offering nr 7
        assertTrue(sequence.offer(7, new long[] {0}));

        // THEN the number should jump to 14
        assertGet(sequence, 14, new long[] {0});
    }

    @Test
    void shouldKeepItsCoolWhenMultipleThreadsAreHammeringIt() throws Exception {
        // An interesting note is that during tests the call to sequence#offer made no difference
        // in performance, so there seems to be no visible penalty in using ArrayQueueOutOfOrderSequence.

        // GIVEN a sequence with intentionally low starting queue size
        LongFunction<long[]> metaFunction = number -> new long[] {number + 2, number * 2};
        final AtomicLong numberSource = new AtomicLong();
        final OutOfOrderSequence sequence =
                new ArrayQueueOutOfOrderSequence(numberSource.get(), 5, metaFunction.apply(numberSource.get()));
        int offerThreads = max(2, Runtime.getRuntime().availableProcessors() - 1);
        final ExecutorService race = Executors.newFixedThreadPool(offerThreads + 1);
        for (int i = 0; i < offerThreads; i++) {
            race.submit(() -> {
                long number;
                while ((number = numberSource.incrementAndGet()) < 10_000_000) {
                    sequence.offer(number, metaFunction.apply(number));
                }
            });
        }
        Runnable verifier = () -> {
            long[] highest = sequence.get();
            long[] expectedMeta = metaFunction.apply(highest[0]);
            assertArrayEquals(expectedMeta, copyOfRange(highest, 1, highest.length));
        };
        race.submit(() -> {
            while (numberSource.get() < 10_000_000) {
                verifier.run();
            }
        });
        race.shutdown();
        race.awaitTermination(1, TimeUnit.MINUTES);

        // THEN
        verifier.run();
    }

    @Test
    void highestEverSeenTest() {
        final OutOfOrderSequence sequence = new ArrayQueueOutOfOrderSequence(0, 5, EMPTY_META);
        assertEquals(0L, sequence.highestEverSeen());

        sequence.offer(1L, EMPTY_META);
        assertEquals(1L, sequence.highestEverSeen());

        sequence.offer(42L, EMPTY_META);
        assertEquals(42L, sequence.highestEverSeen());
    }

    @Test
    void closedTransactionSnapshotNotMissingOfferedNumbers() {
        var sequence = new ArrayQueueOutOfOrderSequence(0, 8, EMPTY_META);
        sequence.offer(1, EMPTY_META);
        sequence.offer(2, EMPTY_META);
        sequence.offer(3, EMPTY_META);
        sequence.offer(4, EMPTY_META);
        sequence.offer(5, EMPTY_META);
        sequence.offer(6, EMPTY_META);
        // skipping 7
        sequence.offer(8, EMPTY_META);
        sequence.offer(9, EMPTY_META);

        var reverseSnapshot1 = sequence.reverseSnapshot();
        assertThat(reverseSnapshot1.highestGapFree()).isEqualTo(6);
        assertThat(reverseSnapshot1.highestEverSeen()).isEqualTo(9);
        assertThat(reverseSnapshot1.missingIds()).containsExactly(7);

        sequence.offer(10, EMPTY_META);
        var reverseSnapshot2 = sequence.reverseSnapshot();
        assertThat(reverseSnapshot2.highestGapFree()).isEqualTo(6);
        assertThat(reverseSnapshot2.highestEverSeen()).isEqualTo(10);
        assertThat(reverseSnapshot2.missingIds()).containsExactly(7);
    }

    @Test
    void closedTransactionSnapshotWithSingleMissingRange() {
        var sequence = new ArrayQueueOutOfOrderSequence(0, 8, new long[] {1, 2});
        sequence.offer(1, new long[] {1, 2});
        sequence.offer(2, new long[] {3, 4});
        sequence.offer(3, new long[] {5, 6});
        sequence.offer(6, new long[] {7, 8});

        var reverseSnapshot = sequence.reverseSnapshot();
        assertEquals(3, reverseSnapshot.highestGapFree());
        assertThat(reverseSnapshot.missingIds()).hasSize(2).contains(4, 5);
    }

    @Test
    void closedTransactionSnapshotWithSeveralMissingRanges() {
        var sequence = new ArrayQueueOutOfOrderSequence(0, 8, new long[] {1, 2});
        sequence.offer(1, new long[] {1, 2});
        sequence.offer(2, new long[] {3, 4});
        sequence.offer(3, new long[] {5, 6});
        sequence.offer(6, new long[] {7, 8});
        sequence.offer(8, new long[] {9, 10});
        sequence.offer(11, new long[] {11, 12});

        var reverseSnapshot = sequence.reverseSnapshot();
        assertEquals(3, reverseSnapshot.highestGapFree());
        assertThat(reverseSnapshot.missingIds()).hasSize(5).contains(4, 5, 7, 9, 10);
    }

    @Test
    void closedTransactionSnapshotWithNoneOutOfOrder() {
        var sequence = new ArrayQueueOutOfOrderSequence(0, 8, new long[] {1, 2});
        sequence.offer(1, new long[] {1, 2});
        sequence.offer(2, new long[] {3, 4});
        sequence.offer(3, new long[] {5, 6});

        var reverseSnapshot = sequence.reverseSnapshot();
        assertEquals(3, reverseSnapshot.highestGapFree());
        assertThat(reverseSnapshot.missingIds()).isEmpty();
    }

    @Test
    void longRangeOfMissingTransactions() {
        var sequence = new ArrayQueueOutOfOrderSequence(10, 8, new long[] {1, 2});
        sequence.offer(11, new long[] {1, 2});
        sequence.offer(21, new long[] {3, 4});
        sequence.offer(31, new long[] {5, 6});
        var reverseSnapshot = sequence.reverseSnapshot();
        assertEquals(11, reverseSnapshot.highestGapFree());
        assertThat(reverseSnapshot.missingIds())
                .hasSize(18)
                .contains(12, 13, 14, 15, 16, 17, 18, 19, 20, 22, 23, 24, 25, 26, 27, 28, 29, 30);
    }

    @Test
    void emptyReverseSnapshotAfterClosingLastGapWithFewCompletedTransactionAhead() {
        // GIVEN
        OutOfOrderSequence sequence = new ArrayQueueOutOfOrderSequence(1L, 5, ArrayUtils.EMPTY_LONG_ARRAY);
        sequence.offer(3, ArrayUtils.EMPTY_LONG_ARRAY);
        sequence.offer(2, ArrayUtils.EMPTY_LONG_ARRAY);
        sequence.offer(8, ArrayUtils.EMPTY_LONG_ARRAY);
        sequence.offer(7, ArrayUtils.EMPTY_LONG_ARRAY);
        sequence.offer(6, ArrayUtils.EMPTY_LONG_ARRAY);
        sequence.offer(5, ArrayUtils.EMPTY_LONG_ARRAY);
        sequence.offer(4, ArrayUtils.EMPTY_LONG_ARRAY);
        var reverseSnapshot = sequence.reverseSnapshot();
        assertThat(reverseSnapshot.highestGapFree()).isEqualTo(8);
        assertThat(reverseSnapshot.highestEverSeen()).isEqualTo(8);
        assertThat(reverseSnapshot.missingIds()).isEmpty();
    }

    @Test
    void shouldSnapshotState() {
        // given
        OutOfOrderSequence sequence = new ArrayQueueOutOfOrderSequence(2, 8, new long[] {1, 2});
        sequence.offer(3, new long[] {3, 4});
        sequence.offer(10, new long[] {10, 11});
        sequence.offer(12, new long[] {12, 13});
        sequence.offer(6, new long[] {6, 7});

        // when grabbing a snapshot
        OutOfOrderSequence.Snapshot snapshot = sequence.snapshot();
        // and making some update afterwards
        sequence.offer(8, new long[] {8, 9});
        sequence.offer(4, new long[] {4, 5});

        // then the snapshot should contain data from when it was taken
        assertThat(snapshot.highestGapFree()).isEqualTo(3);
        assertThat(snapshot.idsOutOfOrder()).containsExactly(6, 10, 12);
    }

    @Test
    void shouldSnapshotUnderStress() throws Throwable {
        // given
        OutOfOrderSequence sequence = new ArrayQueueOutOfOrderSequence(0, 8, new long[] {0, 0});
        int threads = max(2, Runtime.getRuntime().availableProcessors() - 1);
        ExecutorService executorService = Executors.newFixedThreadPool(threads + 1);
        AtomicLong nextNumber = new AtomicLong(1);
        AtomicInteger snapshots = new AtomicInteger(10);
        for (int i = 0; i < threads; i++) {
            executorService.submit(() -> {
                while (snapshots.get() > 0) {
                    long number = nextNumber.getAndIncrement();
                    sequence.offer(number, new long[] {number * 2, number * 3});
                }
            });
        }
        executorService.submit(new Callable<Void>() {
            @Override
            public Void call() throws InterruptedException {
                while (snapshots.get() > 0) {
                    verifyInternallyConsistent(sequence.get());
                    sleep(1);
                    snapshots.decrementAndGet();
                }
                return null;
            }

            private void verifyInternallyConsistent(long[] data) {
                long number = data[0];
                for (int i = 1; i < data.length; i++) {
                    assertEquals(number * (1 + i), data[i]);
                }
            }
        });

        // when/then verifications are made inside the race
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.MINUTES);
    }

    @Test
    void shouldThrowOnOfferingLowerOrEqualToHighestGapFree() {
        // given
        OutOfOrderSequence sequence = new ArrayQueueOutOfOrderSequence(4, 10, new long[] {0});

        // when
        assertThatThrownBy(() -> sequence.offer(4, new long[] {3})).isInstanceOf(IllegalStateException.class);
    }

    private static void assertGet(OutOfOrderSequence sequence, long number, long[] meta) {
        long[] data = sequence.get();
        long[] expected = new long[meta.length + 1];
        expected[0] = number;
        System.arraycopy(meta, 0, expected, 1, meta.length);
        assertArrayEquals(expected, data);
    }
}
