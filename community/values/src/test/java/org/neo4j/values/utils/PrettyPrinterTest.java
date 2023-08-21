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
package org.neo4j.values.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.values.AnyValueWriter.EntityMode.FULL;
import static org.neo4j.values.AnyValueWriter.EntityMode.REFERENCE;
import static org.neo4j.values.storable.DateTimeValue.datetime;
import static org.neo4j.values.storable.DateValue.date;
import static org.neo4j.values.storable.DurationValue.duration;
import static org.neo4j.values.storable.LocalDateTimeValue.localDateTime;
import static org.neo4j.values.storable.LocalTimeValue.localTime;
import static org.neo4j.values.storable.TimeValue.time;
import static org.neo4j.values.storable.Values.byteValue;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValueTestUtil.node;
import static org.neo4j.values.virtual.VirtualValueTestUtil.rel;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;
import static org.neo4j.values.virtual.VirtualValues.list;

import java.time.ZoneOffset;
import java.util.Map;
import java.util.Map.Entry;
import org.junit.jupiter.api.Test;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeIdReference;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.RelationshipReference;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

class PrettyPrinterTest {
    @Test
    void shouldHandleNodeReference() {
        // Given
        NodeIdReference node = VirtualValues.node(42L);
        PrettyPrinter printer = new PrettyPrinter();

        // When
        node.writeTo(printer);

        // Then
        assertThat(printer.value()).isEqualTo("(id=42)");
    }

    @Test
    void shouldHandleNodeValue() {
        // Given
        NodeValue node = node(
                42L,
                Values.stringArray("L1", "L2", "L3"),
                props("foo", intValue(42), "bar", list(intValue(1337), stringValue("baz"))));
        PrettyPrinter printer = new PrettyPrinter();

        // When
        node.writeTo(printer);

        // Then
        assertThat(printer.value()).isEqualTo("(elementId=42 :L1:L2:L3 {bar: [1337, \"baz\"], foo: 42})");
    }

    @Test
    void shouldHandleNodeValueAsReference() {
        // Given
        NodeValue node = node(
                42L,
                Values.stringArray("L1", "L2", "L3"),
                props("foo", intValue(42), "bar", list(intValue(1337), stringValue("baz"))));
        PrettyPrinter printer = new PrettyPrinter(REFERENCE);

        // When
        node.writeTo(printer);

        // Then
        assertThat(printer.value()).isEqualTo("(id=42)");
    }

    @Test
    void shouldHandleNodeValueWithoutLabels() {
        // Given
        NodeValue node = node(
                42L, Values.stringArray(), props("foo", intValue(42), "bar", list(intValue(1337), stringValue("baz"))));
        PrettyPrinter printer = new PrettyPrinter();

        // When
        node.writeTo(printer);

        // Then
        assertThat(printer.value()).isEqualTo("(elementId=42 {bar: [1337, \"baz\"], foo: 42})");
    }

    @Test
    void shouldHandleNodeValueWithoutProperties() {
        // Given
        NodeValue node = node(42L, Values.stringArray("L1", "L2", "L3"), EMPTY_MAP);
        PrettyPrinter printer = new PrettyPrinter();

        // When
        node.writeTo(printer);

        // Then
        assertThat(printer.value()).isEqualTo("(elementId=42 :L1:L2:L3)");
    }

    @Test
    void shouldHandleNodeValueWithoutLabelsNorProperties() {
        // Given
        NodeValue node = node(42L, Values.stringArray(), EMPTY_MAP);
        PrettyPrinter printer = new PrettyPrinter();

        // When
        node.writeTo(printer);

        // Then
        assertThat(printer.value()).isEqualTo("(elementId=42)");
    }

    @Test
    void shouldHandleRelationshipReference() {
        // Given
        RelationshipReference rel = VirtualValues.relationship(42L);
        PrettyPrinter printer = new PrettyPrinter();

        // When
        rel.writeTo(printer);

        // Then
        assertThat(printer.value()).isEqualTo("-[id=42]-");
    }

    @Test
    void shouldHandleRelationshipValue() {
        // Given
        NodeValue startNode = node(1L, Values.stringArray("L"), EMPTY_MAP);
        NodeValue endNode = node(2L, Values.stringArray("L"), EMPTY_MAP);
        RelationshipValue rel = rel(
                42L,
                startNode,
                endNode,
                stringValue("R"),
                props("foo", intValue(42), "bar", list(intValue(1337), stringValue("baz"))));
        PrettyPrinter printer = new PrettyPrinter();

        // When
        rel.writeTo(printer);

        // Then
        assertThat(printer.value()).isEqualTo("-[elementId=42 :R {bar: [1337, \"baz\"], foo: 42}]-");
    }

    @Test
    void shouldHandleRelationshipValueAsReference() {
        // Given
        NodeValue startNode = node(1L, Values.stringArray("L"), EMPTY_MAP);
        NodeValue endNode = node(2L, Values.stringArray("L"), EMPTY_MAP);
        RelationshipValue rel = rel(
                42L,
                startNode,
                endNode,
                stringValue("R"),
                props("foo", intValue(42), "bar", list(intValue(1337), stringValue("baz"))));
        PrettyPrinter printer = new PrettyPrinter(REFERENCE);

        // When
        rel.writeTo(printer);

        // Then
        assertThat(printer.value()).isEqualTo("-[id=42]-");
    }

    @Test
    void shouldHandleRelationshipValueWithoutProperties() {
        NodeValue startNode = node(1L, Values.stringArray("L"), EMPTY_MAP);
        NodeValue endNode = node(2L, Values.stringArray("L"), EMPTY_MAP);
        RelationshipValue rel = rel(42L, startNode, endNode, stringValue("R"), EMPTY_MAP);
        PrettyPrinter printer = new PrettyPrinter();

        // When
        rel.writeTo(printer);

        // Then
        assertThat(printer.value()).isEqualTo("-[elementId=42 :R]-");
    }

    @Test
    void shouldHandleRelationshipValueWithoutLabelsNorProperties() {
        // Given
        NodeValue node = node(42L, Values.stringArray(), EMPTY_MAP);
        PrettyPrinter printer = new PrettyPrinter();

        // When
        node.writeTo(printer);

        // Then
        assertThat(printer.value()).isEqualTo("(elementId=42)");
    }

    @Test
    void shouldHandlePaths() {
        // Given
        NodeValue startNode = node(1L, Values.stringArray("L"), EMPTY_MAP);
        NodeValue endNode = node(2L, Values.stringArray("L"), EMPTY_MAP);
        RelationshipValue rel = rel(42L, startNode, endNode, stringValue("R"), EMPTY_MAP);
        PathValue path = VirtualValues.path(new NodeValue[] {startNode, endNode}, new RelationshipValue[] {rel});
        PrettyPrinter printer = new PrettyPrinter();

        // When
        path.writeTo(printer);

        // Then
        assertThat(printer.value()).isEqualTo("(elementId=1 :L)-[elementId=42 :R]->(elementId=2 :L)");
    }

    @Test
    void shouldHandleMaps() {
        // Given
        PrettyPrinter printer = new PrettyPrinter();
        MapValue mapValue = props("k1", intValue(42));

        // When
        mapValue.writeTo(printer);

        // Then
        assertThat(printer.value()).isEqualTo("{k1: 42}");
    }

    @Test
    void shouldHandleNestedMaps() {
        // Given
        PrettyPrinter printer = new PrettyPrinter();
        MapValue mapValue = props("k1", intValue(42), "k2", props("k3", intValue(1337)));

        // When
        mapValue.writeTo(printer);

        // Then
        assertThat(printer.value()).isEqualTo("{k1: 42, k2: {k3: 1337}}");
    }

    @Test
    void shouldHandleLists() {
        // Given
        PrettyPrinter printer = new PrettyPrinter();
        ListValue list = list(stringValue("foo"), byteValue((byte) 42));

        // When
        list.writeTo(printer);

        // Then
        assertThat(printer.value()).isEqualTo("[\"foo\", 42]");
    }

    @Test
    void shouldHandleNestedLists() {
        // Given
        PrettyPrinter printer = new PrettyPrinter();
        ListValue list = list(intValue(1), list(intValue(2), intValue(3)), intValue(4));

        // When
        list.writeTo(printer);

        // Then
        assertThat(printer.value()).isEqualTo("[1, [2, 3], 4]");
    }

    @Test
    void shouldHandleListsWithListsAndMaps() {
        // Given
        PrettyPrinter printer = new PrettyPrinter();
        ListValue list = list(intValue(1), list(intValue(2), props("k", intValue(3))));

        // When
        list.writeTo(printer);

        // Then
        assertThat(printer.value()).isEqualTo("[1, [2, {k: 3}]]");
    }

    @Test
    void shouldHandleListsWithListsAndMaps1() {
        // Given
        PrettyPrinter printer = new PrettyPrinter();
        ListValue list = list(intValue(1), props("k", intValue(3)));

        // When
        list.writeTo(printer);

        // Then
        assertThat(printer.value()).isEqualTo("[1, {k: 3}]");
    }

    @Test
    void shouldHandleArrays() {
        // Given
        PrettyPrinter printer = new PrettyPrinter();
        TextArray array = Values.stringArray("a", "b", "c");

        // When
        array.writeTo(printer);

        // Then
        assertThat(printer.value()).isEqualTo("[\"a\", \"b\", \"c\"]");
    }

    @Test
    void shouldHandleArraysInMaps() {
        // Given
        PrettyPrinter printer = new PrettyPrinter();
        TextArray array = Values.stringArray("a", "b", "c");
        MapValue map = props("k1", array, "k2", array);

        // When
        map.writeTo(printer);

        // Then
        assertThat(printer.value()).isEqualTo("{k1: [\"a\", \"b\", \"c\"], k2: [\"a\", \"b\", \"c\"]}");
    }

    @Test
    void shouldHandleBooleans() {
        // Given
        Value array = Values.booleanArray(new boolean[] {true, false, true});
        PrettyPrinter printer = new PrettyPrinter();

        // When
        array.writeTo(printer);

        // Then
        assertThat(printer.value()).isEqualTo("[true, false, true]");
    }

    @Test
    void shouldHandleByteArrays() {
        // Given
        Value array = Values.byteArray(new byte[] {2, 3, 42});
        PrettyPrinter printer = new PrettyPrinter();

        // When
        array.writeTo(printer);

        // Then
        assertThat(printer.value()).isEqualTo("[2, 3, 42]");
    }

    @Test
    void shouldHandleNull() {
        // Given
        PrettyPrinter printer = new PrettyPrinter();

        // When
        Values.NO_VALUE.writeTo(printer);

        // Then
        assertThat(printer.value()).isEqualTo("<null>");
    }

    @Test
    void shouldHandlePoints() {
        // Given
        PointValue pointValue = Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 11d, 12d);
        PrettyPrinter printer = new PrettyPrinter();

        // When
        pointValue.writeTo(printer);

        // Then
        assertThat(printer.value())
                .isEqualTo("{geometry: {type: \"Point\", coordinates: [11.0, 12.0], " + "crs: {type: link, properties: "
                        + "{href: \"https://spatialreference.org/ref/sr-org/7203/\", code: " + "7203}}}}");
    }

    @Test
    void shouldBeAbleToUseAnyQuoteMark() {
        // Given
        TextValue hello = stringValue("(ツ)");
        PrettyPrinter printer = new PrettyPrinter("__", FULL);

        // When
        hello.writeTo(printer);

        // Then
        assertThat(printer.value()).isEqualTo("__(ツ)__");
    }

    @Test
    void shouldHandleDuration() {
        DurationValue duration = duration(12, 45, 90, 9911);
        PrettyPrinter printer = new PrettyPrinter();

        duration.writeTo(printer);

        assertEquals("{duration: {months: 12, days: 45, seconds: 90, nanos: 9911}}", printer.value());
    }

    @Test
    void shouldHandleDate() {
        DateValue date = date(1991, 9, 24);
        PrettyPrinter printer = new PrettyPrinter();

        date.writeTo(printer);

        assertEquals("{date: \"1991-09-24\"}", printer.value());
    }

    @Test
    void shouldHandleLocalTime() {
        LocalTimeValue localTime = localTime(18, 39, 24, 111222777);
        PrettyPrinter printer = new PrettyPrinter();

        localTime.writeTo(printer);

        assertEquals("{localTime: \"18:39:24.111222777\"}", printer.value());
    }

    @Test
    void shouldHandleTime() {
        TimeValue time = time(11, 19, 11, 123456789, ZoneOffset.ofHoursMinutes(-9, -30));
        PrettyPrinter printer = new PrettyPrinter();

        time.writeTo(printer);

        assertEquals("{time: \"11:19:11.123456789-09:30\"}", printer.value());
    }

    @Test
    void shouldHandleLocalDateTime() {
        LocalDateTimeValue localDateTime = localDateTime(2015, 8, 8, 8, 40, 29, 999888111);
        PrettyPrinter printer = new PrettyPrinter();

        localDateTime.writeTo(printer);

        assertEquals("{localDateTime: \"2015-08-08T08:40:29.999888111\"}", printer.value());
    }

    @Test
    void shouldHandleDateTimeWithTimeZoneId() {
        DateTimeValue datetime = datetime(2045, 2, 7, 12, 0, 40, 999888999, "Europe/London");
        PrettyPrinter printer = new PrettyPrinter();

        datetime.writeTo(printer);

        assertEquals("{datetime: \"2045-02-07T12:00:40.999888999Z[Europe/London]\"}", printer.value());
    }

    @Test
    void shouldHandleDateTimeWithTimeZoneOffset() {
        DateTimeValue datetime = datetime(1988, 4, 19, 10, 12, 59, 112233445, ZoneOffset.ofHoursMinutes(3, 15));
        PrettyPrinter printer = new PrettyPrinter();

        datetime.writeTo(printer);

        assertEquals("{datetime: \"1988-04-19T10:12:59.112233445+03:15\"}", printer.value());
    }

    @Test
    void shouldLimitTheNumberOfCharacters() {
        // Given
        Map<AnyValue, String> toTest = Map.of(
                stringValue("This is a long string"), "\"This",
                props("k", intValue(1337)), "{k: 133",
                list(intValue(1), intValue(2), stringValue("3"), intValue(4)), "[1, 2, \"3\",",
                list(
                                intValue(1),
                                list(intValue(1), props("k", intValue(42))),
                                props("foo", props("bar", intValue(32)))),
                        "[1, [1, {k: 42}], {foo: {bar: 3");

        for (Entry<AnyValue, String> entry : toTest.entrySet()) {
            var given = entry.getKey();
            var expected = entry.getValue();
            PrettyPrinter printer = new PrettyPrinter("\"", FULL, expected.length());
            given.writeTo(printer);
            assertThat(printer.value()).isEqualTo(expected + "...");
        }
    }

    private static MapValue props(Object... keyValue) {
        String[] keys = new String[keyValue.length / 2];
        AnyValue[] values = new AnyValue[keyValue.length / 2];
        for (int i = 0; i < keyValue.length; i++) {
            if (i % 2 == 0) {
                keys[i / 2] = (String) keyValue[i];
            } else {
                values[i / 2] = (AnyValue) keyValue[i];
            }
        }
        return VirtualValues.map(keys, values);
    }
}
