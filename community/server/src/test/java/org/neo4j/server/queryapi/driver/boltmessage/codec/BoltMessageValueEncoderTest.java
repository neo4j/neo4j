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
package org.neo4j.server.queryapi.driver.boltmessage.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.bolt.connection.values.Value;
import org.neo4j.driver.internal.InternalFloat32Vector;
import org.neo4j.driver.internal.InternalFloat64Vector;
import org.neo4j.driver.internal.InternalInt16Vector;
import org.neo4j.driver.internal.InternalInt32Vector;
import org.neo4j.driver.internal.InternalInt64Vector;
import org.neo4j.driver.internal.InternalInt8Vector;
import org.neo4j.driver.internal.InternalIsoDuration;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.internal.InternalPoint2D;
import org.neo4j.driver.internal.InternalPoint3D;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.internal.value.BooleanValue;
import org.neo4j.driver.internal.value.BytesValue;
import org.neo4j.driver.internal.value.DateTimeValue;
import org.neo4j.driver.internal.value.DateValue;
import org.neo4j.driver.internal.value.DurationValue;
import org.neo4j.driver.internal.value.FloatValue;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.LocalDateTimeValue;
import org.neo4j.driver.internal.value.LocalTimeValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.internal.value.NodeValue;
import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.driver.internal.value.PathValue;
import org.neo4j.driver.internal.value.PointValue;
import org.neo4j.driver.internal.value.RelationshipValue;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.internal.value.TimeValue;
import org.neo4j.driver.internal.value.UUIDValue;
import org.neo4j.driver.internal.value.VectorValue;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.NoValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.VirtualValues;

public class BoltMessageValueEncoderTest {

    public static Stream<Arguments> testValues() {
        return Stream.of(
                Arguments.of(NullValue.NULL, NoValue.NO_VALUE),
                Arguments.of(NullValue.NULL, NoValue.NO_VALUE),
                Arguments.of(BooleanValue.TRUE, org.neo4j.values.storable.BooleanValue.TRUE),
                Arguments.of(BooleanValue.FALSE, org.neo4j.values.storable.BooleanValue.FALSE),
                Arguments.of(new BytesValue(new byte[] {1, 2, 3}), Values.byteArray(new byte[] {1, 2, 3})),
                Arguments.of(new StringValue("hello"), Values.stringValue("hello")),
                Arguments.of(new IntegerValue(35), Values.longValue(35)),
                Arguments.of(new FloatValue(3.14f), Values.doubleValue(3.14f)),
                Arguments.of(
                        new DateValue(LocalDate.parse("2015-06-24")),
                        Values.temporalValue(LocalDate.parse("2015-06-24"))),
                Arguments.of(
                        new TimeValue(OffsetTime.parse("12:50:35.556+01:00")),
                        Values.temporalValue(OffsetTime.parse("12:50:35.556+01:00"))),
                Arguments.of(
                        new LocalTimeValue(LocalTime.parse("12:50:35.556")),
                        Values.temporalValue(LocalTime.parse("12:50:35.556"))),
                Arguments.of(
                        new LocalDateTimeValue(LocalDateTime.parse("2015-06-24T12:50:35.556")),
                        Values.temporalValue(LocalDateTime.parse("2015-06-24T12:50:35.556"))),
                Arguments.of(
                        new DateTimeValue(ZonedDateTime.parse("2015-06-24T12:50:35.556+01:00")),
                        Values.temporalValue(ZonedDateTime.parse("2015-06-24T12:50:35.556+01:00"))),
                Arguments.of(
                        new DurationValue(new InternalIsoDuration(0, 0, 1, 0)),
                        Values.durationValue(Duration.ofSeconds(1))),
                Arguments.of(
                        new DurationValue(new InternalIsoDuration(14, 3, 0, 0)),
                        Values.durationValue(Period.of(1, 2, 3))),
                Arguments.of(
                        new PointValue(new InternalPoint2D(7203, 0, 0)),
                        Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 0, 0)),
                Arguments.of(
                        new PointValue(new InternalPoint3D(9157, 0, 0, 0)),
                        Values.pointValue(CoordinateReferenceSystem.CARTESIAN_3D, 0, 0, 0)),
                Arguments.of(
                        new VectorValue(new InternalInt8Vector(new byte[] {1, 2, 3})),
                        Values.int8Vector((byte) 1, (byte) 2, (byte) 3)),
                Arguments.of(
                        new VectorValue(new InternalInt16Vector(new short[] {1, 2, 3})),
                        Values.int16Vector((short) 1, (short) 2, (short) 3)),
                Arguments.of(
                        new VectorValue(new InternalInt32Vector(new int[] {1, 2, 3})), Values.int32Vector(1, 2, 3)),
                Arguments.of(
                        new VectorValue(new InternalInt64Vector(new long[] {1, 2, 3})), Values.int64Vector(1, 2, 3)),
                Arguments.of(
                        new VectorValue(new InternalFloat32Vector(new float[] {1, 2, 3})),
                        Values.float32Vector(1, 2, 3)),
                Arguments.of(
                        new VectorValue(new InternalFloat64Vector(new double[] {1, 2, 3})),
                        Values.float64Vector(1, 2, 3)),
                Arguments.of(
                        new ListValue(List.of(new IntegerValue(1))),
                        ListValueBuilder.newListBuilder()
                                .add(Values.longValue(1))
                                .build()),
                Arguments.of(
                        new MapValue(Map.of("mappy", new IntegerValue(1))),
                        VirtualValues.map(new String[] {"mappy"}, new AnyValue[] {Values.longValue(1)})),
                Arguments.of(
                        new UUIDValue(UUID.fromString("1a9cc6fb-1a27-46ce-aba8-cd692a2baf09")),
                        Values.uuidValue("1a9cc6fb-1a27-46ce-aba8-cd692a2baf09")));
    }

    public static Stream<Arguments> unsupportedValues() {
        return Stream.of(
                Arguments.of(new NodeValue(new InternalNode(1))),
                Arguments.of(new RelationshipValue(new InternalRelationship(1, 2, 3, "KNOWS"))),
                Arguments.of(new PathValue(new InternalPath(
                        new InternalNode(2), new InternalRelationship(1, 2, 3, "KNOWS"), new InternalNode(3)))));
    }

    @ParameterizedTest
    @MethodSource("unsupportedValues")
    void shouldThrowForUnsupported(Value value) {
        var ex = assertThrows(UnsupportedOperationException.class, () -> BoltMessageValueEncoder.encode(value));
        assertThat(ex.getMessage()).contains("Unsupported value type");
    }

    @ParameterizedTest
    @MethodSource("testValues")
    void shouldEncode(Value value, AnyValue expectedValue) {
        assertThat(BoltMessageValueEncoder.encode(value))
                .usingRecursiveComparison()
                .isEqualTo(expectedValue);
    }
}
