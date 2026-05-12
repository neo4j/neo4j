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
package org.neo4j.genai.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.junit.jupiter.api.Test;

class UrlPathTest {

    @Test
    void shouldAllowSafeCharacters() {
        assertThat(UrlPath.pathSafe("valid-name_1.0")).isEqualTo("valid-name_1.0");
    }

    @Test
    void shouldRejectUnsafeCharacters() {
        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> UrlPath.pathSafe("invalid/name"));
        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> UrlPath.pathSafe("invalid name"));
        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> UrlPath.pathSafe("invalid?name"));
    }

    @Test
    void shouldAllowAtSymbol() {
        assertThat(UrlPath.pathSafe("multimodalembedding@001")).isEqualTo("multimodalembedding@001");
    }
}
