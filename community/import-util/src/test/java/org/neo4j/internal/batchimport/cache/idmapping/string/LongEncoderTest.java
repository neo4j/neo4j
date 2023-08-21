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
package org.neo4j.internal.batchimport.cache.idmapping.string;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class LongEncoderTest {
    @Test
    void shouldProperlyEncodeLength() {
        // GIVEN
        Encoder encoder = new LongEncoder();

        // WHEN
        long a = encoder.encode(1L << 25);
        long b = encoder.encode(1L << 28);

        // THEN
        assertNotEquals(a, b);
    }

    @Test
    void shouldThrowOnEncodingTooLargeID() {
        // GIVEN
        Encoder encoder = new LongEncoder();

        // WHEN
        long invalidValue = 0x01ABC123_4567890FL;

        // THEN
        assertThrows(IllegalArgumentException.class, () -> encoder.encode(invalidValue));
    }

    @Test
    void shouldThrowOnEncodingNegativeID() {
        // GIVEN
        Encoder encoder = new LongEncoder();

        // WHEN
        long invalidValue = -1;

        // THEN
        assertThrows(IllegalArgumentException.class, () -> encoder.encode(invalidValue));
    }
}
