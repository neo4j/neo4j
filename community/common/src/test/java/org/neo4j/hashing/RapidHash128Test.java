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
package org.neo4j.hashing;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The 128-bit buffer hash: that each half is the 64-bit hash under its own seed, and that its two halves are
 * worth 128 bits between them.
 * <p>
 * The id map relies on 128 bits being enough to stand in for a key whose bytes it throws away, which needs each
 * half to carry its own state through the whole key. The halves cannot be shown independent by testing, but
 * truncating them and counting collisions against what a uniform hash of that width would give is sharp
 * enough to catch a construction that is anywhere near degenerate.
 */
@Timeout(300)
class RapidHash128Test {
    private static final long SEED = 0x9E3779B97F4A7C15L;

    @Test
    void lowHalfMatchesTheReferenceVectors() {
        long[] out = new long[2];
        Scanner scanner = new Scanner(requireNonNull(RapidHashTest.class.getResourceAsStream("RapidHashV3.txt")));
        scanner.nextLine(); // Skip header
        while (scanner.hasNextLine()) {
            String[] split = scanner.nextLine().split(",");
            long expectedHash = Long.parseUnsignedLong(split[0]);
            long seed = Long.parseUnsignedLong(split[1]);
            byte[] bytes = split.length == 4 ? split[3].getBytes(StandardCharsets.UTF_8) : new byte[0];
            RapidHash.create(seed).hashBytesTo128(direct(bytes, 0), 0, bytes.length, out);
            assertThat(out[0]).as("length %d", bytes.length).isEqualTo(expectedHash);
        }
    }

    @Test
    void eachHalfIsTheSixtyFourBitHashUnderItsOwnSeed() {
        RapidHash hasher = RapidHash.create(SEED);
        RapidHash highHasher = RapidHash.create(SEED ^ RapidHash.HIGH_SEED);
        var random = new Random(1);
        long[] out = new long[2];

        // Every length across all of the algorithm's shapes: the byte-at-a-time case, the 4-to-16 case, each
        // step of the ladder up to 112, and several passes of the 112-byte loop.
        for (int length = 0; length <= 400; length++) {
            byte[] bytes = new byte[length];
            random.nextBytes(bytes);
            hasher.hashBytesTo128(direct(bytes, 0), 0, length, out);
            assertThat(out[0]).as("low, length %d", length).isEqualTo(hasher.hash(bytes, 0, length));
            assertThat(out[1]).as("high, length %d", length).isEqualTo(highHasher.hash(bytes, 0, length));
        }
    }

    @Test
    void readsFromAnyOffsetInTheBuffer() {
        RapidHash hasher = RapidHash.create(SEED);
        var random = new Random(2);
        long[] expected = new long[2];
        long[] out = new long[2];
        byte[] bytes = new byte[37];
        random.nextBytes(bytes);
        hasher.hashBytesTo128(direct(bytes, 0), 0, bytes.length, expected);

        for (int offset = 0; offset < 40; offset++) {
            ByteBuffer direct =
                    ByteBuffer.allocateDirect(offset + bytes.length + 64).order(ByteOrder.LITTLE_ENDIAN);
            // Noise around the key, so a read that strayed outside it would change the answer.
            byte[] noise = new byte[direct.capacity()];
            random.nextBytes(noise);
            direct.put(0, noise);
            direct.put(offset, bytes);
            hasher.hashBytesTo128(direct, offset, bytes.length, out);
            assertThat(out).as("offset %d", offset).isEqualTo(expected);
        }
    }

    @Test
    void refusesABufferWhoseByteOrderWouldChangeTheAnswer() {
        RapidHash buffer = RapidHash.create(SEED);
        ByteBuffer wrongWayRound = ByteBuffer.allocateDirect(64).order(ByteOrder.BIG_ENDIAN);
        assertThatThrownBy(() -> buffer.hashBytesTo128(wrongWayRound, 0, 16, new long[2]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("little-endian");
    }

    @Test
    void flippingOneInputBitChangesAboutHalfOfTheHundredAndTwentyEightBits() {
        RapidHash hasher = RapidHash.create(SEED);
        long[] before = new long[2];
        long[] after = new long[2];
        int keyBytes = 24;
        ByteBuffer buffer = ByteBuffer.allocateDirect(keyBytes).order(ByteOrder.LITTLE_ENDIAN);
        var random = new Random(3);

        int trials = 0;
        long changedBits = 0;
        int worst = Integer.MAX_VALUE;
        int best = 0;
        byte[] key = new byte[keyBytes];
        for (int trial = 0; trial < 2000; trial++) {
            random.nextBytes(key);
            buffer.put(0, key);
            hasher.hashBytesTo128(buffer, 0, keyBytes, before);
            for (int bit = 0; bit < keyBytes * 8; bit += 7) {
                key[bit >> 3] ^= (byte) (1 << (bit & 7));
                buffer.put(0, key);
                hasher.hashBytesTo128(buffer, 0, keyBytes, after);
                key[bit >> 3] ^= (byte) (1 << (bit & 7));

                int changed = Long.bitCount(before[0] ^ after[0]) + Long.bitCount(before[1] ^ after[1]);
                changedBits += changed;
                worst = Math.min(worst, changed);
                best = Math.max(best, changed);
                trials++;
            }
        }
        double mean = (double) changedBits / trials;
        assertThat(mean)
                .as("mean of 128 output bits changed by a one-bit input change")
                .isBetween(62.0, 66.0);
        assertThat(worst)
                .as("no input bit change ever left most of the hash alone")
                .isGreaterThan(30);
        assertThat(best).as("no input bit change ever flipped nearly all of it").isLessThan(98);
    }

    @Test
    void theTwoHalvesTogetherCollideNoMoreThanAUniformHashOfTheSameWidth() {
        // Sixteen bits from each half, so the pair is 32 bits wide and collisions are plentiful enough to
        // count. Two halves that were near-copies of each other would leave far fewer than 32 usable bits and
        // collide far more than this bound allows.
        int keys = 2_000_000;
        long combined = collisions(keys, out -> ((out[0] & 0xFFFFL) << 16) | (out[1] & 0xFFFFL));
        long expected = expectedCollisions(keys, 32);
        assertThat(combined)
                .as("collisions in 16 bits of each half against %d expected of a uniform 32-bit hash", expected)
                .isLessThan(expected * 11 / 10);

        // And each half has to be a decent hash on its own, or the pair inherits the weakness.
        long lowOnly = collisions(keys, out -> out[0] & 0xFFFFFFFFL);
        long highOnly = collisions(keys, out -> out[1] & 0xFFFFFFFFL);
        long expected32 = expectedCollisions(keys, 32);
        assertThat(lowOnly).as("low half alone").isLessThan(expected32 * 11 / 10);
        assertThat(highOnly).as("high half alone").isLessThan(expected32 * 11 / 10);
    }

    private interface Truncation {
        long of(long[] hash);
    }

    private static long collisions(int keys, Truncation truncation) {
        RapidHash hasher = RapidHash.create(SEED);
        long[] out = new long[2];
        ByteBuffer buffer = ByteBuffer.allocateDirect(32).order(ByteOrder.LITTLE_ENDIAN);
        Set<Long> seen = new HashSet<>(keys * 2);
        long collisions = 0;
        for (int i = 0; i < keys; i++) {
            // Distinct keys that differ in only a few bytes, which is the realistic shape of an id space.
            buffer.putLong(0, i);
            buffer.putLong(8, 0x5DEECE66DL);
            buffer.putLong(16, i * 31L);
            buffer.putLong(24, 0L);
            hasher.hashBytesTo128(buffer, 0, 24, out);
            if (!seen.add(truncation.of(out))) {
                collisions++;
            }
        }
        return collisions;
    }

    /** How many of {@code keys} distinct inputs a uniform hash of {@code bits} would map onto a taken slot. */
    private static long expectedCollisions(int keys, int bits) {
        double slots = Math.pow(2, bits);
        return Math.round(keys - slots * (1 - Math.pow(1 - 1 / slots, keys)));
    }

    private static ByteBuffer direct(byte[] bytes, int offset) {
        ByteBuffer buffer =
                ByteBuffer.allocateDirect(offset + bytes.length + 64).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put(offset, bytes);
        return buffer;
    }
}
