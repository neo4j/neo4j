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
package org.neo4j.collection;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.collection.PrimitiveArrays.countUnique;
import static org.neo4j.collection.PrimitiveArrays.intersect;
import static org.neo4j.collection.PrimitiveArrays.subtract;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.internal.Longs;
import org.junit.jupiter.api.Test;

class PrimitiveArraysTest {
    private static final int[] ONE_INT = new int[] {1};

    // union() null checks. Actual behaviour is tested in PrimitiveSortedArraySetUnionTest

    @Test
    void shouldDeduplicate() {
        int[] array = new int[] {1, 1, 2, 5, 6, 6};

        assertArrayEquals(new int[] {1, 2, 5, 6}, PrimitiveArrays.deduplicate(array));
    }

    @Test
    void shouldDeduplicateWithRandomArrays() {
        int arrayLength = 5000;
        int iterations = 10;
        for (int i = 0; i < iterations; i++) {
            int[] array = ThreadLocalRandom.current()
                    .ints(arrayLength, 0, arrayLength)
                    .sorted()
                    .toArray();
            int[] dedupedActual = PrimitiveArrays.deduplicate(array);
            Set<Integer> set = new TreeSet<>();
            for (int value : array) {
                set.add(value);
            }
            int[] dedupedExpected = new int[set.size()];
            Iterator<Integer> itr = set.iterator();
            for (int j = 0; j < dedupedExpected.length; j++) {
                assertTrue(itr.hasNext());
                dedupedExpected[j] = itr.next();
            }
            assertArrayEquals(dedupedExpected, dedupedActual);
        }
    }

    @Test
    void union_shouldHandleNullInput() {
        assertThat(PrimitiveArrays.union(null, null)).isNull();
        assertThat(PrimitiveArrays.union(null, EMPTY_INT_ARRAY)).isEmpty();
        assertThat(PrimitiveArrays.union(EMPTY_INT_ARRAY, null)).isEmpty();
        assertThat(PrimitiveArrays.union(null, ONE_INT)).isEqualTo(ONE_INT);
        assertThat(PrimitiveArrays.union(ONE_INT, null)).isEqualTo(ONE_INT);
    }

    // intersect()

    @Test
    void intersect_shouldHandleNullInput() {
        assertThat(intersect(null, null)).isEmpty();
        assertThat(intersect(null, EMPTY_INT_ARRAY)).isEmpty();
        assertThat(intersect(EMPTY_INT_ARRAY, null)).isEmpty();
        assertThat(intersect(null, ONE_INT)).isEmpty();
        assertThat(intersect(ONE_INT, null)).isEmpty();
    }

    @Test
    void intersect_shouldHandleNonIntersectingArrays() {
        assertThat(intersect(new int[] {1, 2, 3}, new int[] {4, 5, 6})).isEmpty();

        assertThat(intersect(new int[] {14, 15, 16}, new int[] {1, 2, 3})).isEmpty();
    }

    @Test
    void intersect_shouldHandleIntersectingArrays() {
        assertThat(intersect(new int[] {1, 2, 3}, new int[] {3, 4, 5})).containsExactly(3);

        assertThat(intersect(new int[] {3, 4, 5}, new int[] {1, 2, 3, 4})).containsExactly(3, 4);
    }

    @Test
    void intersect_shouldHandleComplexIntersectingArraysWithGaps() {
        assertThat(intersect(new int[] {4, 6, 9, 11, 12, 15}, new int[] {2, 3, 4, 7, 8, 9, 12, 16, 19}))
                .containsExactly(4, 9, 12);
        assertThat(intersect(new int[] {2, 3, 4, 7, 8, 9, 12, 16, 19}, new int[] {4, 6, 9, 11, 12, 15}))
                .containsExactly(4, 9, 12);
    }

    // symmetricDifference()

    @Test
    void symDiff_shouldHandleNullInput() {
        assertThat(PrimitiveArrays.symmetricDifference(null, null)).isEqualTo(null);
        assertThat(PrimitiveArrays.symmetricDifference(null, EMPTY_INT_ARRAY)).isEmpty();
        assertThat(PrimitiveArrays.symmetricDifference(EMPTY_INT_ARRAY, null)).isEmpty();
        assertThat(PrimitiveArrays.symmetricDifference(null, ONE_INT)).isEqualTo(ONE_INT);
        assertThat(PrimitiveArrays.symmetricDifference(ONE_INT, null)).isEqualTo(ONE_INT);
    }

    @Test
    void symDiff_shouldHandleNonIntersectingArrays() {
        assertThat(PrimitiveArrays.symmetricDifference(new int[] {1, 2, 3}, new int[] {4, 5, 6}))
                .containsExactly(1, 2, 3, 4, 5, 6);

        assertThat(PrimitiveArrays.symmetricDifference(new int[] {14, 15, 16}, new int[] {1, 2, 3}))
                .containsExactly(1, 2, 3, 14, 15, 16);
    }

    @Test
    void symDiff_shouldHandleIntersectingArrays() {
        assertThat(PrimitiveArrays.symmetricDifference(new int[] {1, 2, 3}, new int[] {3, 4, 5}))
                .containsExactly(1, 2, 4, 5);

        assertThat(PrimitiveArrays.symmetricDifference(new int[] {3, 4, 5}, new int[] {1, 2, 3, 4}))
                .containsExactly(1, 2, 5);
    }

    @Test
    void symDiff_shouldHandleComplexIntersectingArraysWithGaps() {
        assertThat(PrimitiveArrays.symmetricDifference(
                        new int[] {4, 6, 9, 11, 12, 15}, new int[] {2, 3, 4, 7, 8, 9, 12, 16, 19}))
                .containsExactly(2, 3, 6, 7, 8, 11, 15, 16, 19);
        assertThat(PrimitiveArrays.symmetricDifference(
                        new int[] {2, 3, 4, 7, 8, 9, 12, 16, 19}, new int[] {4, 6, 9, 11, 12, 15}))
                .containsExactly(2, 3, 6, 7, 8, 11, 15, 16, 19);
    }

    // count unique

    @Test
    void shouldCountUnique() {
        IntPairAssert.assertThat(countUnique(new int[] {1, 2, 3}, new int[] {4, 5, 6}))
                .hasCounts(3, 3);

        IntPairAssert.assertThat(countUnique(new int[] {1, 2, 3}, new int[] {3, 6}))
                .hasCounts(2, 1);

        IntPairAssert.assertThat(countUnique(new int[] {1, 2, 3}, new int[] {3}))
                .hasCounts(2, 0);

        IntPairAssert.assertThat(countUnique(new int[] {3}, new int[] {1, 2, 3}))
                .hasCounts(0, 2);

        IntPairAssert.assertThat(countUnique(new int[] {3}, new int[] {3})).hasCounts(0, 0);

        IntPairAssert.assertThat(countUnique(new int[] {3, 6, 8}, new int[] {})).hasCounts(3, 0);

        IntPairAssert.assertThat(countUnique(new int[] {}, new int[] {3, 6, 8})).hasCounts(0, 3);

        IntPairAssert.assertThat(countUnique(new int[] {}, new int[] {})).hasCounts(0, 0);

        IntPairAssert.assertThat(countUnique(new int[] {4, 6, 9, 11, 12, 15}, new int[] {2, 3, 4, 7, 8, 9, 12, 16, 19}))
                .hasCounts(3, 6);
    }

    @Test
    void shouldSubtract() {
        assertThat(subtract(new int[] {0, 1, 2, 3, 4}, 1, 3)).isEqualTo(new int[] {0, 2, 4});
        assertThat(subtract(new int[] {0, 1, 2, 3, 4}, 0, 4)).isEqualTo(new int[] {1, 2, 3});
        assertThat(subtract(new int[] {0, 1, 2, 3, 4}, -5, 5)).isEqualTo(new int[] {0, 1, 2, 3, 4});
    }

    private static class IntPairAssert extends AbstractAssert<IntPairAssert, Long> {
        private final Longs longs = Longs.instance();

        static IntPairAssert assertThat(long value) {
            return new IntPairAssert(value);
        }

        IntPairAssert(Long value) {
            super(value, IntPairAssert.class);
        }

        IntPairAssert hasCounts(int left, int right) {
            isNotNull();
            long expectedValue = (long) left << 32 | right;
            longs.assertEqual(info, actual, expectedValue);
            return myself;
        }
    }
}
