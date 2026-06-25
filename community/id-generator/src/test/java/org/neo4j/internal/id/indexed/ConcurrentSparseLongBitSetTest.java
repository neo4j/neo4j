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
package org.neo4j.internal.id.indexed;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.neo4j.test.Race;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;

@RandomSupportExtension
class ConcurrentSparseLongBitSetTest {
    @Inject
    private RandomSupport random;

    @Test
    void shouldSetSomeBits() {
        // given
        ConcurrentSparseLongBitSet set = new ConcurrentSparseLongBitSet(128);
        BitSet key = new BitSet(128);

        // when
        set(set, key, 5, 6, true);
        set(set, key, 62, 4, true);
        set(set, key, 70, 7, true);
        long[] snapshot = new long[2];
        set.snapshotRange(0, snapshot);

        // then
        for (int i = 0; i < 128; i++) {
            int arrayIndex = i / Long.SIZE;
            int offset = i % Long.SIZE;
            assertThat((snapshot[arrayIndex] & (1L << offset)) != 0).isEqualTo(key.get(i));
        }
    }

    @Test
    void shouldSetRemoveSet() {
        // given
        ConcurrentSparseLongBitSet set = new ConcurrentSparseLongBitSet(128);
        assertThat(set.set(0, 8, true)).isTrue();
        assertThat(set.set(0, 8, false)).isTrue();

        // when
        boolean reset = set.set(0, 8, true);

        // then
        assertThat(reset).isTrue();
    }

    @Test
    void shouldSetNonConflictingBitsConcurrently() throws Throwable {
        // given
        ConcurrentSparseLongBitSet set = new ConcurrentSparseLongBitSet(128);
        Race race = new Race().withMaxDuration(10, SECONDS);
        int numberOfThreads = 10;
        int idsPerChunk = 1 << random.nextInt(4);
        for (int i = 0; i < numberOfThreads; i++) {
            race.addContestant(setter(set, idsPerChunk, i, numberOfThreads, random.nextLong()), 1_000_000);
        }

        // when
        race.go();

        // then we're good, all assertions were made while running
    }

    @Test
    void shouldSetConflictingBitsConcurrently() throws Throwable {
        // given
        ConcurrentSparseLongBitSet set = new ConcurrentSparseLongBitSet(128);
        Race race = new Race().withMaxDuration(10, SECONDS);
        AtomicBoolean isSet = new AtomicBoolean();
        race.addContestants(
                10,
                () -> {
                    boolean wasSet = set.set(3, 10, true);
                    if (wasSet) {
                        assertThat(isSet.compareAndSet(false, true)).isTrue();
                    }
                },
                1);

        // when
        race.go();

        // then
        assertThat(isSet.get()).isTrue();
    }

    @Test
    void shouldRemoveEmptyRanges() {
        // given
        ConcurrentSparseLongBitSet set = new ConcurrentSparseLongBitSet(128);
        set.set(5, 2, true);
        set.set(7, 2, true);
        assertThat(set.size()).isEqualTo(1);

        // when
        set.set(5, 4, false);

        // then
        assertThat(set.size()).isEqualTo(0);

        // and when
        set.set(9, 5, true);
        assertThat(set.size()).isEqualTo(1);
    }

    private static Runnable setter(
            ConcurrentSparseLongBitSet set, int idsPerChunk, int i, int numberOfThreads, long seed) {
        return new Runnable() {
            private final BitSet key = new BitSet();
            private final long[] reader = new long[2];
            private final long[] temp = new long[2];
            private final Random random = new Random(seed);

            @Override
            public void run() {
                int chunk = random.nextInt(1024);
                int id = (chunk * numberOfThreads + i) * idsPerChunk;
                boolean isSet = key.get(chunk);

                // read
                set.snapshotRange(id / set.getIdsPerEntry(), reader);
                Arrays.fill(temp, 0);
                BitsUtil.setBits(temp, id % set.getIdsPerEntry(), idsPerChunk, 0);
                assertThat(bitsMatches(reader, temp, isSet)).isTrue();

                // write
                boolean actuallySet = set.set(id, idsPerChunk, !isSet);
                assertThat(actuallySet).isTrue();
                key.set(chunk, !isSet);
            }
        };
    }

    private static void set(ConcurrentSparseLongBitSet set, BitSet key, long id, int slots, boolean value) {
        boolean actuallySet = set.set(id, slots, value);
        assertThat(actuallySet).isTrue();
        for (int i = 0; i < slots; i++) {
            key.set((int) (id + i), value);
        }
    }

    static boolean bitsMatches(long[] bits, long[] mask, boolean value) {
        assert bits.length == mask.length;
        if (value) {
            for (int i = 0; i < bits.length; i++) {
                if ((bits[i] & mask[i]) != mask[i]) {
                    return false;
                }
            }
        } else {
            for (int i = 0; i < bits.length; i++) {
                if ((bits[i] & mask[i]) != 0) {
                    return false;
                }
            }
        }
        return true;
    }
}
