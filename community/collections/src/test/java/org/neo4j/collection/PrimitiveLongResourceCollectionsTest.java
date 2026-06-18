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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphdb.Resource;

class PrimitiveLongResourceCollectionsTest {

    // ITERATOR

    @Test
    void simpleIterator() {
        // Given
        CountingResource resource = new CountingResource();
        PrimitiveLongResourceIterator iterator = PrimitiveLongResourceCollections.iterator(resource, 1, 2, 3, 4);

        // Then
        assertContent(iterator, 1, 2, 3, 4);

        // When
        iterator.close();

        // Then
        assertThat(resource.closeCount()).as("exactly one call to close").isOne();
    }

    // FILTER

    // CONCAT

    @Test
    void concatIterators() {
        // Given
        CountingResource resource = new CountingResource();
        PrimitiveLongResourceIterator first = PrimitiveLongResourceCollections.iterator(resource, 1, 2);
        PrimitiveLongResourceIterator second = PrimitiveLongResourceCollections.iterator(resource, 3, 4);

        // When
        PrimitiveLongResourceIterator concat = PrimitiveLongResourceCollections.concat(first, second);

        // Then
        assertContent(concat, 1, 2, 3, 4);

        // When
        concat.close();

        // Then
        assertThat(resource.closeCount())
                .as("all concatenated iterators are closed")
                .isEqualTo(2);
    }

    public static Stream<Arguments> complement() {
        return Stream.of(
                Arguments.of(new long[] {1, 2}, 5, new long[] {0, 3, 4}),
                Arguments.of(new long[] {1, 2, 3}, 5, new long[] {0, 4}),
                Arguments.of(new long[] {0, 2, 3}, 5, new long[] {1, 4}),
                Arguments.of(new long[] {2}, 5, new long[] {0, 1, 3, 4}),
                Arguments.of(new long[] {0, 1, 2}, 3, new long[] {}),
                Arguments.of(new long[] {0, 1}, 3, new long[] {2}),
                Arguments.of(new long[] {1, 2}, 3, new long[] {0}),
                Arguments.of(new long[] {2}, 3, new long[] {0, 1}),
                Arguments.of(new long[] {}, 3, new long[] {0, 1, 2}),
                Arguments.of(new long[] {}, 1, new long[] {0}),
                Arguments.of(new long[] {0}, 1, new long[] {}),
                Arguments.of(new long[] {}, 0, new long[] {}));
    }

    @ParameterizedTest
    @MethodSource
    void complement(long[] original, long max, long[] expected) {
        // Given
        CountingResource resource = new CountingResource();
        var originalIterator = PrimitiveLongResourceCollections.iterator(resource, original);

        // When
        var inverse = PrimitiveLongResourceCollections.complement(originalIterator, max);

        assertContent(inverse, expected);
    }

    private static void assertContent(PrimitiveLongResourceIterator iterator, long... expected) {
        int i = 0;
        while (iterator.hasNext()) {
            if (i >= expected.length) {
                fail("More values than expected: " + iterator.next());
                return;
            }
            assertThat(iterator.next()).as("has expected value").isEqualTo(expected[i++]);
        }
        assertThat(i).as("has all expected values").isEqualTo(expected.length);
    }

    private static class CountingResource implements Resource {
        private final AtomicInteger closed = new AtomicInteger();

        @Override
        public void close() {
            closed.incrementAndGet();
        }

        int closeCount() {
            return closed.get();
        }
    }
}
