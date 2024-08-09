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
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.stringValue;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.impl.factory.Multimaps;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

class ValueUtilsTest {
    @Test
    void shouldHandleCollection() {
        // Given
        Collection<Integer> collection = Arrays.asList(1, 2, 3);

        // When
        AnyValue of = ValueUtils.of(collection);

        // Then
        assertThat(of).isInstanceOf(ListValue.class);
        ListValue listValue = (ListValue) of;
        assertThat(listValue.value(0)).isEqualTo(intValue(1));
        assertThat(listValue.value(1)).isEqualTo(intValue(2));
        assertThat(listValue.value(2)).isEqualTo(intValue(3));
        assertThat(listValue.actualSize()).isEqualTo(3);
    }

    @Test
    void shouldHandleIterator() {
        // Given
        Iterator<Integer> iterator = Arrays.asList(1, 2, 3).iterator();

        // When
        AnyValue of = ValueUtils.of(iterator);

        // Then
        assertThat(of).isInstanceOf(ListValue.class);
        ListValue listValue = (ListValue) of;
        assertThat(listValue.value(0)).isEqualTo(intValue(1));
        assertThat(listValue.value(1)).isEqualTo(intValue(2));
        assertThat(listValue.value(2)).isEqualTo(intValue(3));
        assertThat(listValue.actualSize()).isEqualTo(3);
    }

    @Test
    void shouldHandleMaps() {
        // Given
        Map<String, Object> map = MapUtil.map("a", Arrays.asList("foo", 42));

        // When
        AnyValue anyValue = ValueUtils.of(map);

        // Then
        assertThat(anyValue).isInstanceOf(MapValue.class);
        MapValue mapValue = (MapValue) anyValue;
        assertThat(mapValue.get("a")).isEqualTo(VirtualValues.list(stringValue("foo"), intValue(42)));
        assertThat(mapValue.size()).isEqualTo(1);
    }

    @Test
    void shouldHandleIterable() {
        // Given
        Iterable<Integer> collection = Arrays.asList(1, 2, 3);

        // When
        AnyValue of = ValueUtils.of(collection);

        // Then
        assertThat(of).isInstanceOf(ListValue.class);
        ListValue listValue = (ListValue) of;
        assertThat(listValue.value(0)).isEqualTo(intValue(1));
        assertThat(listValue.value(1)).isEqualTo(intValue(2));
        assertThat(listValue.value(2)).isEqualTo(intValue(3));
        assertThat(listValue.actualSize()).isEqualTo(3);
    }

    @Test
    void shouldHandleIterableMutableMaps() {
        // Given
        Map<String, Object> map = Maps.mutable.with("a", Arrays.asList("foo", 42));

        // When
        AnyValue anyValue = ValueUtils.of(map);

        // Then
        assertThat(anyValue).isInstanceOf(MapValue.class);
        MapValue mapValue = (MapValue) anyValue;
        assertThat(mapValue.get("a")).isEqualTo(VirtualValues.list(stringValue("foo"), intValue(42)));
        assertThat(mapValue.size()).isEqualTo(1);
    }

    @Test
    void shouldHandleMultimaps() {
        // Given
        final var multimap = Multimaps.mutable.list.<String, Object>empty();
        multimap.put("a", "foo");
        multimap.put("a", 42);
        multimap.put("b", "bar");
        multimap.put("b", 69);

        // When
        AnyValue anyValue = ValueUtils.of(multimap);

        // Then
        assertThat(anyValue).isInstanceOf(MapValue.class);
        MapValue mapValue = (MapValue) anyValue;
        assertThat(mapValue.get("a")).isEqualTo(VirtualValues.list(stringValue("foo"), intValue(42)));
        assertThat(mapValue.get("b")).isEqualTo(VirtualValues.list(stringValue("bar"), intValue(69)));
        assertThat(mapValue.size()).isEqualTo(2);
    }
}
