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

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.numberValue;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;
import static org.neo4j.values.virtual.VirtualValues.map;

import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.values.AnyValue;

class MapValueTest {
    @Test
    void shouldFilterOnKeys() {
        // Given
        MapValue base = mapValue("k1", stringValue("v1"), "k2", stringValue("v2"), "k3", stringValue("v3"));

        // When
        MapValue filtered = base.filter((k, ignore) -> k.equals("k2"));

        // Then
        assertMapValueEquals(filtered, mapValue("k2", stringValue("v2")));
    }

    @Test
    void shouldFilterOnValues() {
        // Given
        MapValue base = mapValue("k1", stringValue("v1"), "k2", stringValue("v2"), "k3", stringValue("v3"));

        // When
        MapValue filtered = base.filter((ignore, v) -> v.equals(stringValue("v2")));

        // Then
        assertMapValueEquals(filtered, mapValue("k2", stringValue("v2")));
    }

    @Test
    void shouldFilterOnKeysAndValues() {
        // Given
        MapValue base = mapValue("k1", stringValue("v1"), "k2", stringValue("v2"), "k3", stringValue("v3"));

        // When
        MapValue filtered = base.filter((k, v) -> k.equals("k1") && v.equals(stringValue("v2")));

        // Then
        assertMapValueEquals(filtered, EMPTY_MAP);
    }

    @Test
    void shouldUpdateWithIdenticalValues() {
        // Given
        MapValue base = mapValue("k1", stringValue("v1"), "k2", stringValue("v2"), "k3", stringValue("v3"));

        // When
        MapValue updated = base.updatedWith("k3", stringValue("v3"));

        // Then
        assertMapValueEquals(updated, base);
    }

    @Test
    void shouldUpdateWithExistingKey() {
        // Given
        MapValue base = mapValue("k1", stringValue("v1"), "k2", stringValue("v2"), "k3", stringValue("v3"));

        // When
        MapValue updated = base.updatedWith("k3", stringValue("version3"));

        // Then
        assertMapValueEquals(
                updated, mapValue("k1", stringValue("v1"), "k2", stringValue("v2"), "k3", stringValue("version3")));
    }

    @Test
    void shouldUpdateWithExistingKeyAndNoValue() {
        // Given
        MapValue base = mapValue("k1", stringValue("v1"), "k2", stringValue("v2"), "k3", NO_VALUE);

        // When
        MapValue updated = base.updatedWith("k3", stringValue("version3"));

        // Then
        assertMapValueEquals(
                updated, mapValue("k1", stringValue("v1"), "k2", stringValue("v2"), "k3", stringValue("version3")));
    }

    @Test
    void shouldUpdateWithNonExistingKeyAndNoValue() {
        // Given
        MapValue base = mapValue("k1", stringValue("v1"), "k2", stringValue("v2"));

        // When
        MapValue updated = base.updatedWith("k3", NO_VALUE);

        // Then
        assertMapValueEquals(updated, mapValue("k1", stringValue("v1"), "k2", stringValue("v2"), "k3", NO_VALUE));
    }

    @Test
    void shouldUpdateWithNewKey() {
        // Given
        MapValue base = mapValue("k1", stringValue("v1"), "k2", stringValue("v2"), "k3", stringValue("v3"));

        // When
        MapValue updated = base.updatedWith("k4", stringValue("v4"));

        // Then
        assertMapValueEquals(
                updated,
                mapValue(
                        "k1",
                        stringValue("v1"),
                        "k2",
                        stringValue("v2"),
                        "k3",
                        stringValue("v3"),
                        "k4",
                        stringValue("v4")));
    }

    @Test
    void shouldUpdateWithOtherMapValue() {
        // Given
        MapValue a = mapValue("k1", stringValue("v1"), "k2", stringValue("v2"), "k3", stringValue("v3"));
        MapValue b =
                mapValue("k1", stringValue("version1"), "k2", stringValue("version2"), "k4", stringValue("version4"));

        // When
        MapValue updated = a.updatedWith(b);

        // Then
        assertMapValueEquals(
                updated,
                mapValue(
                        "k1", stringValue("version1"),
                        "k2", stringValue("version2"),
                        "k3", stringValue("v3"),
                        "k4", stringValue("version4")));
    }

    @Test
    void shouldUpdateWithOtherMapValueWithSeveralNewKeys() {
        // Given
        MapValue a = mapValue("k1", stringValue("v1"), "k2", stringValue("v2"), "k3", stringValue("v3"));
        MapValue b =
                mapValue("k1", stringValue("version1"), "k4", stringValue("version4"), "k5", stringValue("version5"));

        // When
        MapValue updated = a.updatedWith(b);

        // Then
        assertMapValueEquals(
                updated,
                mapValue(
                        "k1", stringValue("version1"),
                        "k2", stringValue("v2"),
                        "k3", stringValue("v3"),
                        "k4", stringValue("version4"),
                        "k5", stringValue("version5")));
    }

    @Test
    void shouldUpdateMultipleTimesMapValue() {
        // Given
        MapValue a = mapValue("k1", stringValue("v1"), "k2", stringValue("v2"));
        MapValue b = mapValue("k1", stringValue("version1"), "k4", stringValue("version4"));
        MapValue c = mapValue("k3", stringValue("v3"));

        // When
        MapValue updated = a.updatedWith(b).updatedWith(c);

        // Then
        assertMapValueEquals(
                updated,
                mapValue(
                        "k1", stringValue("version1"),
                        "k2", stringValue("v2"),
                        "k3", stringValue("v3"),
                        "k4", stringValue("version4")));
    }

    @Test
    void shouldCompareTwoCombinedMapValues() {
        // Given
        MapValue a = mapValue("note", stringValue("test"), "Id", numberValue(8));
        MapValue b = mapValue("note", stringValue("test"), "Id", numberValue(14));

        // When
        MapValue x = mapValue().updatedWith(a);
        MapValue y = mapValue().updatedWith(b);

        assertThat(a).as("Two simple maps should be different").isNotEqualTo(b);
        assertThat(x).as("Two combined maps should be different").isNotEqualTo(y);
    }

    @Test
    void shouldReportIsEmpty() {
        // Given
        MapValue empty = MapValue.EMPTY;
        MapValue nonEmpty = mapValue("key", stringValue("value"));
        MapValue otherNonEmpty = mapValue("key", stringValue("hello"));
        MapValue combined = otherNonEmpty.updatedWith(otherNonEmpty);
        MapValue filtered = nonEmpty.filter((k, v) -> false);
        MapValue combinedFiltered = combined.filter((k, v) -> false);

        // Then
        assertThat(empty.isEmpty()).isTrue();
        assertThat(nonEmpty.isEmpty()).isFalse();
        assertThat(otherNonEmpty.isEmpty()).isFalse();
        assertThat(combined.isEmpty()).isFalse();
        assertThat(filtered.isEmpty()).isTrue();
        assertThat(combinedFiltered.isEmpty()).isTrue();
    }

    private static void assertMapValueEquals(MapValue a, MapValue b) {
        assertThat(a).isEqualTo(b);
        assertThat(a.size()).isEqualTo(b.size());
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
        assertThat(a.keySet()).contains(Iterables.asArray(String.class, b.keySet()));
        assertThat(Arrays.asList(a.keys().asArray())).contains(b.keys().asArray());
        a.foreach((k, v) -> assertThat(b.get(k)).isEqualTo(v));
        b.foreach((k, v) -> assertThat(a.get(k)).isEqualTo(v));
        a.foreach((ak, av) -> assertThat(b.entryExists((bk, bv) -> ak.equals(bk) && av.equals(bv)))
                .isTrue());
        b.foreach((bk, bv) -> assertThat(a.entryExists((ak, av) -> ak.equals(bk) && av.equals(bv)))
                .isTrue());
    }

    private static MapValue mapValue(Object... kv) {
        assert kv.length % 2 == 0;
        String[] keys = new String[kv.length / 2];
        AnyValue[] values = new AnyValue[kv.length / 2];
        for (int i = 0; i < kv.length; i += 2) {
            keys[i / 2] = (String) kv[i];
            values[i / 2] = (AnyValue) kv[i + 1];
        }
        return map(keys, values);
    }
}
