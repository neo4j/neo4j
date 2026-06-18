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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class ByteToEnumTest {

    public enum TestType {
        MIN(Byte.MIN_VALUE),
        MINUS_THREE((byte) -3),
        MINUS_ONE((byte) -1),
        TWO((byte) 2),
        FIVE((byte) 5),
        EIGHT((byte) 8),
        ONE_TWO_NINE((byte) 129),
        TWO_FIVE_TWO((byte) 252),
        TWO_FIVE_NINE((byte) 259),
        MAX(Byte.MAX_VALUE);

        final byte byteValue;

        TestType(byte byteValue) {
            this.byteValue = byteValue;
        }

        public byte getByteValue() {
            return byteValue;
        }
    };

    static ByteToEnum<TestType> MAP = new ByteToEnum<>(TestType.class, TestType::getByteValue);

    @ParameterizedTest
    @EnumSource(TestType.class)
    void validLookup(TestType type) {
        assertThat(MAP.get(type.byteValue)).isEqualTo(type);
    }

    @Test
    void nulls() {
        assertThat(MAP.get((byte) -2)).isNull();
        assertThat(MAP.get((byte) 0)).isNull();
        assertThat(MAP.get((byte) 4)).isNull();
        assertThat(MAP.get((byte) 6)).isNull();
        assertThat(MAP.get((byte) 7)).isNull();
        assertThat(MAP.get((byte) 9)).isNull();
    }

    @Test
    void wrap() {
        assertThat(MAP.get((byte) 128)).isEqualTo(TestType.MIN);
        assertThat(MAP.get((byte) 255)).isEqualTo(TestType.MINUS_ONE);
        assertThat(MAP.get((byte) 383)).isEqualTo(TestType.MAX);
        assertThat(MAP.get((byte) 253)).isEqualTo(TestType.MINUS_THREE);
        assertThat(MAP.get((byte) -4)).isEqualTo(TestType.TWO_FIVE_TWO);
    }
}
