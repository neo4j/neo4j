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
package org.neo4j.internal.helpers.collection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class LongRangeTest {
    @Test
    void shouldBeWithinRange() {
        int from = 4;
        int to = 8;
        LongRange range = LongRange.range(from, to);

        assertThat(range.isWithinRange(from - 1)).isFalse();
        assertThat(range.isWithinRange(to + 1)).isFalse();
        for (int i = from; i < to + 1; i++) {
            assertThat(range.isWithinRange(i)).isTrue();
        }
    }

    @ParameterizedTest
    @MethodSource("validRanges")
    void shouldBeWithinRange(RangeProvider rangeProvider) {
        assertThatCode(rangeProvider::get).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @MethodSource("invalidRanges")
    void checkInvalidRanges(RangeProvider rangeProvider) {
        assertThatThrownBy(rangeProvider::get).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void joinRanges() {
        LongRange rangeA = LongRange.range(10, 12);
        LongRange rangeB = LongRange.range(13, 15);
        LongRange joinedRange = LongRange.join(rangeA, rangeB);
        assertThat(joinedRange.from()).isEqualTo(10);
        assertThat(joinedRange.to()).isEqualTo(15);
    }

    @Test
    void emptyRange() {
        assertThat(LongRange.EMPTY_RANGE.isEmpty()).isTrue();
        assertThat(LongRange.range(5, 5).isEmpty()).isFalse();
        assertThat(LongRange.range(6, 6).stream().findAny().orElseThrow()).isEqualTo(6);
    }

    @Test
    void rangeStream() {
        LongRange longRange = LongRange.range(2, 5);
        assertThat(longRange.stream().min().orElseThrow()).isEqualTo(2);
        assertThat(longRange.stream().max().orElseThrow()).isEqualTo(5);
    }

    @Test
    void streamRangeAndRangeStreamAreEqual() {
        LongStream rangeStream = LongRange.range(1, 10).stream();
        LongStream longStream = LongStream.rangeClosed(1, 10);
        assertThat(rangeStream).containsExactlyElementsOf(longStream::iterator);
    }

    @Test
    void failJoinNonAdjacentRanges() {
        LongRange rangeA = LongRange.range(10, 12);
        LongRange rangeB = LongRange.range(14, 15);
        assertThatThrownBy(() -> LongRange.join(rangeA, rangeB)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void adjacentRangesCheck() {
        LongRange rangeA = LongRange.range(10, 12);
        LongRange rangeB = LongRange.range(14, 15);
        LongRange rangeC = LongRange.range(12, 15);
        LongRange rangeD = LongRange.range(10, 11);

        assertThat(rangeA.isAdjacent(rangeB)).isFalse();
        assertThat(rangeA.isAdjacent(rangeC)).isFalse();
        assertThat(rangeD.isAdjacent(rangeC)).isTrue();
        assertThat(rangeD.isAdjacent(rangeB)).isFalse();
    }

    private static Stream<RangeProvider> invalidRanges() {
        return Stream.of(new RangeProvider(-1, 0));
    }

    private static Stream<RangeProvider> validRanges() {
        return Stream.of(
                new RangeProvider(1, 0),
                new RangeProvider(Long.MAX_VALUE, Long.MIN_VALUE),
                new RangeProvider(0, 0),
                new RangeProvider(Long.MAX_VALUE, Long.MAX_VALUE),
                new RangeProvider(0, Long.MAX_VALUE));
    }

    static class RangeProvider {
        private final long from;
        private final long to;

        RangeProvider(long from, long to) {
            this.from = from;
            this.to = to;
        }

        LongRange get() {
            return LongRange.range(from, to);
        }

        @Override
        public String toString() {
            return "RangeProvider{" + "from=" + from + ", to=" + to + '}';
        }
    }
}
