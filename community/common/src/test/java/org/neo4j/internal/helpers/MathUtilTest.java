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
package org.neo4j.internal.helpers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.helpers.MathUtil.clamp;

import org.junit.jupiter.api.Test;

class MathUtilTest {
    @Test
    void shouldClampInt() {
        // given
        int value = 12345;

        // when/then
        assertThat(clamp(value, value - 10, value + 10)).isEqualTo(value);
        assertThat(clamp(value, value + 10, value + 100)).isEqualTo(value + 10);
        assertThat(clamp(value, value - 100, value - 10)).isEqualTo(value - 10);
    }

    @Test
    void shouldClampLong() {
        // given
        long value = 123456789000L;

        // when/then
        assertThat(clamp(value, value - 10, value + 10)).isEqualTo(value);
        assertThat(clamp(value, value + 10, value + 100)).isEqualTo(value + 10);
        assertThat(clamp(value, value - 100, value - 10)).isEqualTo(value - 10);
    }

    @Test
    void shouldClampFloat() {
        // given
        float value = 123.456f;

        // when/then
        assertThat(clamp(value, value - 1.5, value + 1.5)).isEqualTo(value);
        assertThat(clamp(value, value + 1.5, value + 3)).isEqualTo(value + 1.5);
        assertThat(clamp(value, value - 3, value - 1.5)).isEqualTo(value - 1.5);
    }

    @Test
    void shouldClampDouble() {
        // given
        double value = 123.456D;

        // when/then
        assertThat(clamp(value, value - 1.5, value + 1.5)).isEqualTo(value);
        assertThat(clamp(value, value + 1.5, value + 3)).isEqualTo(value + 1.5);
        assertThat(clamp(value, value - 3, value - 1.5)).isEqualTo(value - 1.5);
    }
}
