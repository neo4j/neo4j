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
package org.neo4j.values.virtual;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.values.storable.Values.booleanValue;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValueTestUtil.map;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_LIST;
import static org.neo4j.values.virtual.VirtualValues.concat;
import static org.neo4j.values.virtual.VirtualValues.list;

import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.neo4j.values.AnyValue;

class ConcatListTest {
    @Test
    void shouldHandleZeroListConcatenation() {
        // Given
        ListValue inner = EMPTY_LIST;

        // When
        ListValue concat = concat(inner);

        // Then
        assertTrue(concat.isEmpty());
    }

    @Test
    void shouldKeepRepresentationWithEmptyListConcatenation() {
        // Given
        ListValue list = list(stringValue("foo"));
        ListValue emptyList = EMPTY_LIST;

        // When
        ListValue concat = concat(list, emptyList);
        ListValue concatReverse = concat(emptyList, list);

        // Then
        assertEquals(concat.itemValueRepresentation(), concatReverse.itemValueRepresentation());
        assertEquals(concat.itemValueRepresentation(), list.itemValueRepresentation());
    }

    @Test
    void shouldHandleSingleListConcatenation() {
        // Given
        ListValue inner = list(stringValue("foo"), longValue(42), booleanValue(true));

        // When
        ListValue concat = concat(inner);

        // Then
        assertEquals(inner, concat);
        assertEquals(inner.hashCode(), concat.hashCode());
        assertArrayEquals(inner.asArray(), concat.asArray());
    }

    @Test
    void shouldHandleMultipleListConcatenation() {
        // Given
        ListValue inner1 = list(stringValue("foo"), longValue(42), booleanValue(true));
        ListValue inner2 = list(list(stringValue("bar"), intValue(42)));
        ListValue inner3 = list(map("foo", 1337L, "bar", 42), stringValue("baz"));

        // When
        ListValue concat = concat(inner1, inner2, inner3);

        // Then
        ListValue expected = list(
                stringValue("foo"),
                longValue(42),
                booleanValue(true),
                list(stringValue("bar"), intValue(42)),
                map("foo", 1337L, "bar", 42),
                stringValue("baz"));
        assertEquals(expected, concat);
        assertEquals(expected.hashCode(), concat.hashCode());
        assertArrayEquals(expected.asArray(), concat.asArray());
    }

    @Test
    void heapUsageShouldNotOverflow() {
        // Given
        var array = new AnyValue[8192];
        Arrays.fill(array, stringValue("foo"));
        ListValue list = list(array);
        for (int i = 0; i < 4000; i++) {
            assertThat(list.estimatedHeapUsage()).isGreaterThan(0);
            list = concat(list, list(array));
        }
    }
}
