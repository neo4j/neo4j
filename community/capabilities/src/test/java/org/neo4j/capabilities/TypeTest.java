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
package org.neo4j.capabilities;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.capabilities.Type.BOOLEAN;
import static org.neo4j.capabilities.Type.DOUBLE;
import static org.neo4j.capabilities.Type.FLOAT;
import static org.neo4j.capabilities.Type.INTEGER;
import static org.neo4j.capabilities.Type.LONG;
import static org.neo4j.capabilities.Type.STRING;
import static org.neo4j.capabilities.Type.listOf;

import java.util.Collection;
import org.junit.jupiter.api.Test;

class TypeTest {

    @Test
    void testString() {
        assertThat(STRING.name()).isEqualTo("string");
        assertThat(STRING.description()).isEqualTo("a string value");
        assertThat(STRING.type()).isEqualTo(String.class);
    }

    @Test
    void testBoolean() {
        assertThat(BOOLEAN.name()).isEqualTo("boolean");
        assertThat(BOOLEAN.description()).isEqualTo("a boolean value");
        assertThat(BOOLEAN.type()).isEqualTo(Boolean.class);
    }

    @Test
    void testInteger() {
        assertThat(INTEGER.name()).isEqualTo("integer");
        assertThat(INTEGER.description()).isEqualTo("an integer value");
        assertThat(INTEGER.type()).isEqualTo(Integer.class);
    }

    @Test
    void testLong() {
        assertThat(LONG.name()).isEqualTo("long");
        assertThat(LONG.description()).isEqualTo("a long value");
        assertThat(LONG.type()).isEqualTo(Long.class);
    }

    @Test
    void testFloat() {
        assertThat(FLOAT.name()).isEqualTo("float");
        assertThat(FLOAT.description()).isEqualTo("a float value");
        assertThat(FLOAT.type()).isEqualTo(Float.class);
    }

    @Test
    void testDouble() {
        assertThat(DOUBLE.name()).isEqualTo("double");
        assertThat(DOUBLE.description()).isEqualTo("a double value");
        assertThat(DOUBLE.type()).isEqualTo(Double.class);
    }

    @Test
    void testListOf() {
        var list = listOf(STRING);
        assertThat(list.name()).isEqualTo("list of string");
        assertThat(list.description()).isEqualTo("a list of string values");
        assertThat(list.type()).isEqualTo(Collection.class);

        assertThatThrownBy(() -> listOf(listOf(STRING)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("nested list types is not supported.");
    }
}
