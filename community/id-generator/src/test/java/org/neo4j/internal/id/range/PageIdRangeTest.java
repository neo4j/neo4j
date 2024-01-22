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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class PageIdRangeTest {

    @Test
    void markAndResetContinuousRange() {
        var continuousIdRange = new ContinuousIdRange(5, 15, 120);
        continuousIdRange.mark();

        for (int i = 0; i < 5; i++) {
            assertEquals(5, continuousIdRange.nextId());
            assertEquals(6, continuousIdRange.nextId());
            assertEquals(7, continuousIdRange.nextId());
            assertEquals(8, continuousIdRange.nextId());

            continuousIdRange.resetToMark();
        }

        assertEquals(5, continuousIdRange.nextId());

        continuousIdRange.mark();

        for (int i = 0; i < 5; i++) {
            assertEquals(6, continuousIdRange.nextId());
            assertEquals(7, continuousIdRange.nextId());
            assertEquals(8, continuousIdRange.nextId());
            assertEquals(9, continuousIdRange.nextId());

            continuousIdRange.resetToMark();
        }
    }

    @Test
    void markAndResetArrayRange() {
        var arrayIdRange = new ArrayBasedRange(new long[] {5, 6, 7, 8, 9, 10}, 120);
        arrayIdRange.mark();

        for (int i = 0; i < 5; i++) {
            assertEquals(5, arrayIdRange.nextId());
            assertEquals(6, arrayIdRange.nextId());
            assertEquals(7, arrayIdRange.nextId());
            assertEquals(8, arrayIdRange.nextId());

            arrayIdRange.resetToMark();
        }

        assertEquals(5, arrayIdRange.nextId());

        arrayIdRange.mark();

        for (int i = 0; i < 5; i++) {
            assertEquals(6, arrayIdRange.nextId());
            assertEquals(7, arrayIdRange.nextId());
            assertEquals(8, arrayIdRange.nextId());
            assertEquals(9, arrayIdRange.nextId());

            arrayIdRange.resetToMark();
        }
    }
}
