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
package org.neo4j.internal.id.range;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.id.IdGenerator.NO_ID;

import org.junit.jupiter.api.Test;

class PageIdRangeTest {

    @Test
    void markAndResetContinuousRange() {
        var continuousIdRange = new ContinuousIdRange(5, 15, 120);
        continuousIdRange.mark();

        for (int i = 0; i < 5; i++) {
            assertThat(continuousIdRange.nextId()).isEqualTo(5);
            assertThat(continuousIdRange.nextId()).isEqualTo(6);
            assertThat(continuousIdRange.nextId()).isEqualTo(7);
            assertThat(continuousIdRange.nextId()).isEqualTo(8);

            continuousIdRange.resetToMark();
        }

        assertThat(continuousIdRange.nextId()).isEqualTo(5);

        continuousIdRange.mark();

        for (int i = 0; i < 5; i++) {
            assertThat(continuousIdRange.nextId()).isEqualTo(6);
            assertThat(continuousIdRange.nextId()).isEqualTo(7);
            assertThat(continuousIdRange.nextId()).isEqualTo(8);
            assertThat(continuousIdRange.nextId()).isEqualTo(9);

            continuousIdRange.resetToMark();
        }
    }

    @Test
    void markAndResetArrayRange() {
        var arrayIdRange = new ArrayBasedRange(new long[] {5, 6, 7, 8, 9, 10}, 120);
        arrayIdRange.mark();

        for (int i = 0; i < 5; i++) {
            assertThat(arrayIdRange.nextId()).isEqualTo(5);
            assertThat(arrayIdRange.nextId()).isEqualTo(6);
            assertThat(arrayIdRange.nextId()).isEqualTo(7);
            assertThat(arrayIdRange.nextId()).isEqualTo(8);

            arrayIdRange.resetToMark();
        }

        assertThat(arrayIdRange.nextId()).isEqualTo(5);

        arrayIdRange.mark();

        for (int i = 0; i < 5; i++) {
            assertThat(arrayIdRange.nextId()).isEqualTo(6);
            assertThat(arrayIdRange.nextId()).isEqualTo(7);
            assertThat(arrayIdRange.nextId()).isEqualTo(8);
            assertThat(arrayIdRange.nextId()).isEqualTo(9);

            arrayIdRange.resetToMark();
        }
    }

    @Test
    void shouldGiveConsecutiveIdsArrayBased() {
        var arrayIdRange = new ArrayBasedRange(new long[] {1, 2, 4, 5, 10}, 120);
        assertThat(arrayIdRange.consecutiveIds(2)).isEqualTo(1L);
        assertThat(arrayIdRange.consecutiveIds(2)).isEqualTo(4L);
        assertThat(arrayIdRange.consecutiveIds(2)).isEqualTo(NO_ID);
        assertThat(arrayIdRange.nextId()).isEqualTo(10L);
    }

    @Test
    void shouldGiveNoIdWhenAskingForMoreThanAvailableArrayBased() {
        // When not enough consecutive IDs are found
        var arrayIdRange = new ArrayBasedRange(new long[] {1, 2, 3, 4}, 120);
        // Then get NO_ID back
        assertThat(arrayIdRange.consecutiveIds(5)).isEqualTo(NO_ID);
        // When asking for something that IS available
        assertThat(arrayIdRange.consecutiveIds(4)).isEqualTo(1L);
    }

    @Test
    void shouldResetToMarkArrayBased() {
        var arrayIdRange = new ArrayBasedRange(new long[] {1, 2}, 120);
        arrayIdRange.mark();
        // [1,2]
        // ^ mark
        assertThat(arrayIdRange.consecutiveIds(2)).isEqualTo(1L);
        // [1,2]
        //    ^ cursor
        arrayIdRange.resetToMark();
        assertThat(arrayIdRange.consecutiveIds(2)).isEqualTo(1L);
    }

    @Test
    void shouldGiveConsecutiveIdsContinuousIdRange() {
        var continuousIdRange = new ContinuousIdRange(0, 10, 120);
        assertThat(continuousIdRange.consecutiveIds(2)).isEqualTo(0L);
        assertThat(continuousIdRange.consecutiveIds(3)).isEqualTo(2L);
        assertThat(continuousIdRange.nextId()).isEqualTo(5L);
    }

    @Test
    void shouldGiveNoIdWhenAskingForMoreThanAvailableContinuous() {
        // When not enough consecutive IDs are found
        var continuousIdRange = new ContinuousIdRange(1, 4, 120);
        // Then get NO_ID back
        assertThat(continuousIdRange.consecutiveIds(5)).isEqualTo(NO_ID);
        // When asking for something that IS available
        assertThat(continuousIdRange.consecutiveIds(4)).isEqualTo(1L);
    }

    @Test
    void exhaustContinuousRangeByConsecutiveRequests() {
        var continuousIdRange = new ContinuousIdRange(127, 127, 127);
        assertThat(continuousIdRange.consecutiveIds(126)).isEqualTo(127);
        assertThat(continuousIdRange.hasNext()).isTrue();

        assertThat(continuousIdRange.consecutiveIds(1)).isEqualTo(253);
        assertThat(continuousIdRange.hasNext()).isFalse();
    }
}
