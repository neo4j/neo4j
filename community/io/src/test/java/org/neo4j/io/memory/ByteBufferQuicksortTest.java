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
package org.neo4j.io.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.IntFunction;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.eclipse.collections.api.factory.primitive.LongLists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Some tests have been adapted from the SortingNearlySortedPrimitive.java in the JDK test suite to test the various
 * branches of the ByteBuffer/DualPivotQuicksort algorithm
 */
class ByteBufferQuicksortTest {

    private static final int M1 = 1024 * 1024;

    private static final int BASE = 3;
    private static final int WIDTH = 4;
    private static final int PAD = 300;

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 9})
    void sortMustOnlyTakesLongs(int extras) {
        var buffer = ByteBuffers.allocate(Long.BYTES + extras, ByteOrder.LITTLE_ENDIAN, INSTANCE);
        try {
            assertThatThrownBy(() -> ByteBufferQuicksort.sort(buffer, INSTANCE))
                    .isInstanceOf(IllegalArgumentException.class);
        } finally {
            ByteBuffers.releaseBuffer(buffer, INSTANCE);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 4, 8, 128, 1024, 8192, M1, 2 * M1, 4 * M1, 8 * M1, 16 * M1})
    void shouldSort(int longCount) {
        long seed = System.currentTimeMillis();
        var random = new Random(seed);

        ByteBuffer buffer;
        if (random.nextBoolean()) {
            buffer = ByteBuffers.allocate(
                    longCount * Long.BYTES,
                    random.nextBoolean() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN,
                    INSTANCE);
        } else {
            buffer = ByteBuffers.allocateDirect(
                    longCount * Long.BYTES,
                    random.nextBoolean() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN,
                    INSTANCE);
        }

        try {
            var values = LongLists.mutable.empty();
            for (int i = 0; i < longCount; i++) {
                long value = random.nextLong();
                buffer.putLong(value);
                values.add(value);
            }

            ByteBufferQuicksort.sort(buffer.flip(), INSTANCE);

            var longs = values.sortThis().longIterator();
            while (longs.hasNext() && buffer.hasRemaining()) {
                assertThat(longs.next())
                        .as("should have the correct element using the seed: " + seed)
                        .isEqualTo(buffer.getLong());
            }

            assertThat(buffer.hasRemaining()).isFalse();
            assertThat(longs.hasNext()).isFalse();
        } finally {
            ByteBuffers.releaseBuffer(buffer, INSTANCE);
        }
    }

    @Test
    void testCombinations() {
        LongStream.range(0, (int) Math.pow(BASE, 2 * WIDTH))
                .mapToObj(ByteBufferQuicksortTest::createBuffer)
                .forEach(buffer -> assertOrdering("results should have been ordered", buffer));
    }

    @ParameterizedTest
    @MethodSource("createShapes")
    void testShapes(String testName, IntFunction<ByteBuffer> dataMethod, int size) {
        assertOrdering("results should have been ordered in: " + testName, dataMethod.apply(size));
    }

    private static void assertOrdering(String message, ByteBuffer buffer) {
        ByteBufferQuicksort.sort(buffer, INSTANCE);

        long last = Long.MIN_VALUE;
        while (buffer.hasRemaining()) {
            long value = buffer.getLong();
            assertThat(value).as(message).isGreaterThanOrEqualTo(last);
            last = value;
        }
    }

    private static ByteBuffer createBuffer(long v) {
        var buffer = ByteBuffer.allocate((WIDTH + PAD + WIDTH) * Long.BYTES);

        // Fill head of buffer
        for (int j = 0; j < WIDTH; j++) {
            buffer.putLong(j * Long.BYTES, (v % BASE) - (BASE / 2));
            v /= BASE;
        }
        // Fill tail of buffer
        for (int j = 0; j < WIDTH; j++) {
            buffer.putLong((WIDTH + PAD + j) * Long.BYTES, (v % BASE) - (BASE / 2));
            v /= BASE;
        }
        return buffer;
    }

    private static Object[][] createShapes() {
        Stream<List<Object>> baseCases = Stream.of(
                List.of("hiZeroLowTest", (IntFunction<ByteBuffer>) ByteBufferQuicksortTest::hiZeroLowData),
                List.of("endLessThanTest", (IntFunction<ByteBuffer>) ByteBufferQuicksortTest::endLessThanData),
                List.of("highFlatLowTest", (IntFunction<ByteBuffer>) ByteBufferQuicksortTest::highFlatLowData),
                List.of("identicalTest", (IntFunction<ByteBuffer>) ByteBufferQuicksortTest::identicalData),
                List.of("sortedReversedSortedTest", (IntFunction<ByteBuffer>)
                        ByteBufferQuicksortTest::sortedReversedSortedData),
                List.of("pairFlipTest", (IntFunction<ByteBuffer>) ByteBufferQuicksortTest::pairFlipData),
                List.of("zeroHiTest", (IntFunction<ByteBuffer>) ByteBufferQuicksortTest::zeroHiData));

        var sizes = List.of(100, 1_000, 10_000, 1_000_000);
        return baseCases.flatMap(l -> sizes.stream().map(s -> append(l, s))).toArray(Object[][]::new);
    }

    private static Object[] append(List<Object> l, Object value) {
        var nl = new ArrayList<>(l);
        nl.add(value);
        return nl.toArray();
    }

    private static ByteBuffer zeroHiData(int size) {
        var buffer = ByteBuffer.allocate(size * Long.BYTES);

        int threeQuarters = (int) (size * 0.75);
        for (int i = 0; i < threeQuarters; i++) {
            buffer.putLong(toPos(i), 0);
        }
        int k = 1;
        for (int i = threeQuarters; i < size; i++) {
            buffer.putLong(toPos(i), k++);
        }

        return buffer;
    }

    private static ByteBuffer hiZeroLowData(int size) {
        var buffer = ByteBuffer.allocate(size * Long.BYTES);

        int oneThird = size / 3;
        for (int i = 0; i < oneThird; i++) {
            buffer.putLong(toPos(i), i);
        }
        int twoThirds = oneThird * 2;
        for (int i = oneThird; i < twoThirds; i++) {
            buffer.putLong(toPos(i), 0);
        }
        for (int i = twoThirds; i < size; i++) {
            buffer.putLong(toPos(i), oneThird - i + twoThirds);
        }
        return buffer;
    }

    private static ByteBuffer highFlatLowData(int size) {
        var buffer = ByteBuffer.allocate(size * Long.BYTES);

        int oneThird = size / 3;
        for (int i = 0; i < oneThird; i++) {
            buffer.putLong(toPos(i), i);
        }
        int twoThirds = oneThird * 2;
        int constant = oneThird - 1;
        for (int i = oneThird; i < twoThirds; i++) {
            buffer.putLong(toPos(i), constant);
        }
        for (int i = twoThirds; i < size; i++) {
            buffer.putLong(toPos(i), constant - i + twoThirds);
        }

        return buffer;
    }

    private static ByteBuffer identicalData(int size) {
        var buffer = ByteBuffer.allocate(size * Long.BYTES);
        for (int i = 0; i < size; i++) {
            buffer.putLong(toPos(i), 42);
        }
        return buffer;
    }

    private static ByteBuffer endLessThanData(int size) {
        var buffer = ByteBuffer.allocate(size * Long.BYTES);
        for (int i = 0; i < size - 1; i++) {
            buffer.putLong(toPos(i), 3);
        }
        buffer.putLong(toPos(size - 1), 1);
        return buffer;
    }

    private static ByteBuffer sortedReversedSortedData(int size) {
        var buffer = ByteBuffer.allocate(size * Long.BYTES);

        for (int i = 0; i < size / 2; i++) {
            buffer.putLong(toPos(i), i);
        }
        int num = 0;
        for (int i = size / 2; i < size; i++) {
            buffer.putLong(toPos(i), size - num++);
        }

        return buffer;
    }

    private static ByteBuffer pairFlipData(int size) {
        var buffer = ByteBuffer.allocate(size * Long.BYTES);

        for (int i = 0; i < size; i++) {
            buffer.putLong(toPos(i), i);
        }
        for (int i = 0; i < size; i += 2) {
            long temp = buffer.getLong(toPos(i));
            buffer.putLong(toPos(i), buffer.getLong(toPos(i + 1)));
            buffer.putLong(toPos(i + 1), temp);
        }

        return buffer;
    }

    private static int toPos(int ix) {
        return ix * Long.BYTES;
    }
}
