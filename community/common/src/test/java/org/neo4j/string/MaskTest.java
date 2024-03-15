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
package org.neo4j.string;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class MaskTest {
    @Test
    void testFilter() {
        assertThat(Mask.NO.filter("hello")).isEqualTo("hello");
        assertThat(Mask.YES.filter("hello")).isEqualTo("<MASKED>");
    }

    @Test
    void testBuild() {
        var builder = new StringBuilder();
        Mask.NO.build(builder, b -> b.append("hello"));
        assertThat(builder.toString()).isEqualTo("hello");

        builder = new StringBuilder();
        Mask.YES.build(builder, b -> b.append("hello"));
        assertThat(builder.toString()).isEqualTo("<MASKED>");
    }

    @Test
    void testFilterIterable() {
        final var list = List.of(new MaskableThing("hello"), new MaskableThing("goodbye"));
        assertThat(Mask.NO.filter(list)).isEqualTo("[data:hello, data:goodbye]");
        assertThat(Mask.YES.filter(list)).isEqualTo("[data:<MASKED>, data:<MASKED>]");
    }

    @Test
    void testAppendIterable() {
        final var list = List.of(new MaskableThing("hello"), new MaskableThing("goodbye"));

        var builder = new StringBuilder();
        Mask.NO.append(builder, list);
        assertThat(builder.toString()).isEqualTo("[data:hello, data:goodbye]");

        builder = new StringBuilder();
        Mask.YES.append(builder, list);
        assertThat(builder.toString()).isEqualTo("[data:<MASKED>, data:<MASKED>]");
    }

    private record MaskableThing(String secret) implements Mask.Maskable {
        @Override
        public String toString(Mask mask) {
            return "data:" + mask.filter(secret);
        }
    }
}
