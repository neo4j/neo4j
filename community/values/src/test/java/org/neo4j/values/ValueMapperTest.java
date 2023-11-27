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
package org.neo4j.values;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.values.storable.CoordinateReferenceSystem.CARTESIAN;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS_84;
import static org.neo4j.values.storable.DateTimeValue.datetime;
import static org.neo4j.values.storable.DateValue.date;
import static org.neo4j.values.storable.DurationValue.duration;
import static org.neo4j.values.storable.LocalDateTimeValue.localDateTime;
import static org.neo4j.values.storable.LocalTimeValue.localTime;
import static org.neo4j.values.storable.TimeValue.time;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.booleanArray;
import static org.neo4j.values.storable.Values.booleanValue;
import static org.neo4j.values.storable.Values.byteArray;
import static org.neo4j.values.storable.Values.byteValue;
import static org.neo4j.values.storable.Values.charArray;
import static org.neo4j.values.storable.Values.charValue;
import static org.neo4j.values.storable.Values.doubleArray;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.floatArray;
import static org.neo4j.values.storable.Values.floatValue;
import static org.neo4j.values.storable.Values.intArray;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longArray;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.numberValue;
import static org.neo4j.values.storable.Values.pointArray;
import static org.neo4j.values.storable.Values.pointValue;
import static org.neo4j.values.storable.Values.shortArray;
import static org.neo4j.values.storable.Values.shortValue;
import static org.neo4j.values.storable.Values.stringArray;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValueTestUtil.node;
import static org.neo4j.values.virtual.VirtualValueTestUtil.rel;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;
import static org.neo4j.values.virtual.VirtualValues.list;
import static org.neo4j.values.virtual.VirtualValues.map;
import static org.neo4j.values.virtual.VirtualValues.path;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualPathValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

class ValueMapperTest {
    private static Stream<AnyValue> parameters() {
        NodeValue node1 = node(1, stringArray(), EMPTY_MAP);
        NodeValue node2 = node(2, stringArray(), EMPTY_MAP);
        NodeValue node3 = node(3, stringArray(), EMPTY_MAP);
        RelationshipValue relationship1 = rel(100, node1, node2, stringValue("ONE"), EMPTY_MAP);
        RelationshipValue relationship2 = rel(200, node2, node2, stringValue("TWO"), EMPTY_MAP);
        return Stream.of(
                node1,
                relationship1,
                path(new NodeValue[] {node1, node2, node3}, new RelationshipValue[] {relationship1, relationship2}),
                map(new String[] {"alpha", "beta"}, new AnyValue[] {stringValue("one"), numberValue(2)}),
                NO_VALUE,
                list(numberValue(1), stringValue("fine"), node2),
                stringValue("hello world"),
                stringArray("hello", "brave", "new", "world"),
                booleanValue(false),
                booleanArray(new boolean[] {true, false, true}),
                charValue('\n'),
                charArray(new char[] {'h', 'e', 'l', 'l', 'o'}),
                byteValue((byte) 3),
                byteArray(new byte[] {0x00, (byte) 0x99, (byte) 0xcc}),
                shortValue((short) 42),
                shortArray(new short[] {1337, (short) 0xcafe, (short) 0xbabe}),
                intValue(987654321),
                intArray(new int[] {42, 11}),
                longValue(9876543210L),
                longArray(new long[] {0xcafebabe, 0x1ee7}),
                floatValue(Float.MAX_VALUE),
                floatArray(new float[] {Float.NEGATIVE_INFINITY, Float.MIN_VALUE}),
                doubleValue(Double.MIN_NORMAL),
                doubleArray(new double[] {Double.POSITIVE_INFINITY, Double.MAX_VALUE}),
                datetime(2018, 1, 16, 10, 36, 43, 123456788, ZoneId.of("Europe/Stockholm")),
                localDateTime(2018, 1, 16, 10, 36, 43, 123456788),
                date(2018, 1, 16),
                time(10, 36, 43, 123456788, ZoneOffset.ofHours(1)),
                localTime(10, 36, 43, 123456788),
                duration(399, 4, 48424, 133701337),
                pointValue(CARTESIAN, 11, 32),
                pointArray(new Point[] {pointValue(CARTESIAN, 11, 32), pointValue(WGS_84, 13, 56)}));
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void shouldMapToJavaObject(AnyValue value) {
        // given
        ValueMapper<Object> mapper = new Mapper();

        // when
        Object mapped = value.map(mapper);

        // then
        assertEquals(value, valueOf(mapped));

        if (mapped instanceof List<?> mappedList) {
            // Some users depends on mutable lists (GDS for example)
            mappedList.clear();
            assertEquals(0, mappedList.size());
        }
    }

    private static AnyValue valueOf(Object obj) {
        if (obj instanceof MappedGraphType) {
            return ((MappedGraphType) obj).value;
        }
        Value value = Values.unsafeOf(obj, true);
        if (value != null) {
            return value;
        }
        if (obj instanceof List<?>) {
            return ((List<?>) obj).stream().map(ValueMapperTest::valueOf).collect(ListValueBuilder.collector());
        }
        if (obj instanceof Map<?, ?>) {
            @SuppressWarnings("unchecked")
            Map<String, ?> map = (Map<String, ?>) obj;
            int size = map.size();
            if (size == 0) {
                return EMPTY_MAP;
            }
            MapValueBuilder builder = new MapValueBuilder(size);
            for (Map.Entry<String, ?> entry : map.entrySet()) {
                builder.add(entry.getKey(), valueOf(entry.getValue()));
            }

            return builder.build();
        }
        throw new AssertionError(
                "cannot convert: " + obj + " (a " + obj.getClass().getName() + ")");
    }

    private static class Mapper extends ValueMapper.JavaMapper {
        @Override
        public Object mapPath(VirtualPathValue value) {
            return new MappedGraphType(value);
        }

        @Override
        public Object mapNode(VirtualNodeValue value) {
            return new MappedGraphType(value);
        }

        @Override
        public Object mapRelationship(VirtualRelationshipValue value) {
            return new MappedGraphType(value);
        }
    }

    private static class MappedGraphType {
        private final VirtualValue value;

        MappedGraphType(VirtualValue value) {

            this.value = value;
        }
    }
}
