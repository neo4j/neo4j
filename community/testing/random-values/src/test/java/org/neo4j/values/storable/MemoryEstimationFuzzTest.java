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
package org.neo4j.values.storable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.values.virtual.VirtualValues.fromArray;

import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.neo4j.values.virtual.ListValue;

class MemoryEstimationFuzzTest {
    private final RandomValues random = RandomValues.create();
    private static final int ITERATIONS = 1000;

    @Test
    void shouldEstimateIntegerTypes() {
        for (int i = 0; i < ITERATIONS; i++) {
            assertThat(random.nextByteValue().estimatedHeapUsage())
                    .isLessThanOrEqualTo(random.nextShortValue().estimatedHeapUsage());
            assertThat(random.nextShortValue().estimatedHeapUsage())
                    .isLessThanOrEqualTo(random.nextIntValue().estimatedHeapUsage());
            assertThat(random.nextIntValue().estimatedHeapUsage())
                    .isLessThanOrEqualTo(random.nextLongValue().estimatedHeapUsage());
        }
    }

    @Test
    void shouldEstimateFloatingTypes() {
        for (int i = 0; i < ITERATIONS; i++) {
            assertThat(random.nextFloatValue().estimatedHeapUsage())
                    .isLessThanOrEqualTo(random.nextDoubleValue().estimatedHeapUsage());
        }
    }

    @Test
    void shouldEstimateTextValue() {
        for (int i = 0; i < ITERATIONS; i++) {
            assertThat(random.nextTextValue(10, 10).estimatedHeapUsage())
                    .isLessThan(random.nextTextValue(100, 100).estimatedHeapUsage());
        }
    }

    @Test
    void shouldEstimateArrayValues() {
        for (ValueType type : arrayTypes()) {
            for (int i = 0; i < ITERATIONS; i++) {
                ArrayValue a = (ArrayValue) random.nextValueOfType(type);
                ArrayValue b = (ArrayValue) random.nextValueOfType(type);
                if (a.intSize() < b.intSize()) {
                    assertTrue(a.estimatedHeapUsage() <= b.estimatedHeapUsage());
                } else {
                    assertTrue(a.estimatedHeapUsage() >= b.estimatedHeapUsage());
                }
            }
        }
    }

    @Test
    void shouldEstimateListValues() {
        for (ValueType type : arrayTypes()) {
            for (int i = 0; i < ITERATIONS; i++) {
                ListValue a = fromArray((ArrayValue) random.nextValueOfType(type));
                ListValue b = fromArray((ArrayValue) random.nextValueOfType(type));
                if (a.intSize() < b.intSize()) {
                    assertTrue(a.estimatedHeapUsage() <= b.estimatedHeapUsage());
                } else {
                    assertTrue(a.estimatedHeapUsage() >= b.estimatedHeapUsage());
                }
            }
        }
    }

    private static Iterable<ValueType> arrayTypes() {
        // For strings the size of the individual elements will vary
        // and it is not always true that a bigger array uses more memory
        // than a smaller one
        return () -> Arrays.stream(ValueType.arrayTypes())
                .filter(t -> t != ValueType.STRING_ARRAY
                        && t != ValueType.STRING_ALPHANUMERIC_ARRAY
                        && t != ValueType.STRING_ASCII_ARRAY
                        && t != ValueType.STRING_BMP_ARRAY)
                .iterator();
    }
}
