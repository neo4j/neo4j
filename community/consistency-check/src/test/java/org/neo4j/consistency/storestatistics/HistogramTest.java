/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency.storestatistics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.consistency.storestatistics.Histogram.newFragMeasureHistogram;

import org.junit.jupiter.api.Test;

public class HistogramTest {
    private static final String h1ExpectedString = "   Buckets | Frequencies\n" + "         0 | 1\n"
            + "         1 | 0\n"
            + "         4 | 0\n"
            + "        16 | 0\n"
            + "        64 | 3\n"
            + "       256 | 2\n"
            + "      1024 | 0\n"
            + "      4096 | 0\n"
            + "     16384 | 0\n"
            + "     65536 | 0\n"
            + "  16777216 | 0\n"
            + "2147483647 | 2";

    private static final String h3ExpectedString = "   Buckets | Frequencies\n" + "         0 | 1\n"
            + "         1 | 0\n"
            + "         4 | 0\n"
            + "        16 | 1\n"
            + "        64 | 5\n"
            + "       256 | 3\n"
            + "      1024 | 0\n"
            + "      4096 | 0\n"
            + "     16384 | 0\n"
            + "     65536 | 0\n"
            + "  16777216 | 0\n"
            + "2147483647 | 2";

    private static Histogram initialiseH1() {
        Histogram h1 = new Histogram();

        // Lower bound edge case
        h1.addValue(0);
        // Add multiple
        h1.addValue(1 << 6);
        h1.addValue(1 << 6);
        h1.addValue(1 << 6);
        // Check that boundary between buckets is correct
        h1.addValue(1 << 6 + 1);
        // Check that multiple values within bucket are counted
        h1.addValue(100);
        // Check arbitrary value is counted
        h1.addValue((1 << 25) + (1 << 16));
        // Upper bound edge case
        h1.addValue(Integer.MAX_VALUE);

        return h1;
    }

    private static Histogram initialiseH2() {
        Histogram h2 = new Histogram();

        // Add 1 to 0
        h2.addValue(1 << 4);
        // Add 2 to 3
        h2.addValue(1 << 6);
        h2.addValue(1 << 6);
        // Add 1 to 2, but with a new value within the bucket
        h2.addValue(1 << 6 + 2);

        return h2;
    }

    @Test
    void addValue_OutOfBounds_Panics() {
        // Given
        Histogram h1 = newFragMeasureHistogram();

        // Then
        assertThatThrownBy(() -> h1.addValue(Integer.MAX_VALUE)).isInstanceOf(RuntimeException.class);
    }

    @Test
    void addValue_Multiple_CountsAll() {
        // Given
        Histogram h1 = initialiseH1();

        // Then
        assertThat(h1.prettyPrint()).isEqualTo(h1ExpectedString);
    }

    @Test
    void addTo_MismatchedBuckets_Panics() {
        // Given
        Histogram h1 = new Histogram();
        Histogram h2 = newFragMeasureHistogram();

        // Then
        assertThatThrownBy(() -> h1.addTo(h2)).isInstanceOf(RuntimeException.class);
    }

    @Test
    void addTo_MatchingHistogram_SumsAll() {
        // Given
        Histogram h1 = initialiseH1();
        Histogram h2 = initialiseH2();
        Histogram h3 = new Histogram();

        // When
        h1.addTo(h3);
        h2.addTo(h3);

        // Then
        assertThat(h3.prettyPrint()).isEqualTo(h3ExpectedString);
    }
}
