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
package org.neo4j.cypher.operations;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.utils.ValueTypeNames;

/// Check that 2 different implementations of "Cypher names of Value types" are consistent
/// The `ValueTypeNames` is in core `org.neo4j.values.utils` and is used to generate Cypher-friendly
/// type names for error messages (e.g. a Value is of an unexpected type) when accessing
/// `CypherTypeValueMapper` would break encapsulation.
class CypherTypeValueMapperValueTypeNamesTest {

    private static final ZonedDateTime ZD1 = ZonedDateTime.of(2018, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC"));
    private static final ZonedDateTime ZD2 = ZonedDateTime.of(1972, 12, 25, 0, 0, 0, 0, ZoneId.of("CET"));

    @ParameterizedTest
    @MethodSource("provideRawValuesAndTypes")
    void typeNamesOfSomeValues(Object value, String expected) throws Exception {
        assertThat(cypherTypeName(Values.of(value))).isEqualTo(expected);
        assertThat(ValueTypeNames.nameOfType(Values.of(value))).isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("provideStoredValuesAndTypes")
    void typeNamesOfSomeValues(Value value, String expected) throws Exception {
        assertThat(cypherTypeName(value)).isEqualTo(expected);
        assertThat(ValueTypeNames.nameOfType(value)).isEqualTo(expected);
    }

    private static Stream<Arguments> provideRawValuesAndTypes() {
        return Stream.of(
                Arguments.of((short) 3, "INTEGER"),
                Arguments.of(Long.MAX_VALUE, "INTEGER"),
                Arguments.of(Byte.MIN_VALUE, "INTEGER"),
                Arguments.of('c', "STRING"),
                Arguments.of("", "STRING"),
                Arguments.of("Hello world!", "STRING"),
                Arguments.of(ZonedDateTime.now(), "ZONED DATETIME"),
                Arguments.of(LocalDateTime.now(), "LOCAL DATETIME"),
                Arguments.of(OffsetTime.now(), "ZONED TIME"),
                Arguments.of(LocalTime.now(), "LOCAL TIME"),
                Arguments.of(LocalDate.now(), "DATE"),
                Arguments.of(Duration.ofDays(40), "DURATION"),
                Arguments.of(null, "NULL"),
                Arguments.of(false, "BOOLEAN"),
                Arguments.of(true, "BOOLEAN"),
                Arguments.of(new int[] {1, 2, 3}, "LIST<INTEGER>"),
                Arguments.of(new double[] {1, 2, 3}, "LIST<FLOAT>"));
    }

    private static Stream<Arguments> provideStoredValuesAndTypes() {
        return Stream.of(
                Arguments.of(Values.longValue(42L), "INTEGER"),
                Arguments.of(Values.floatValue(1.1f), "FLOAT"),
                Arguments.of(Values.doubleValue(1.1d), "FLOAT"),
                Arguments.of(Values.pointValue(CoordinateReferenceSystem.WGS_84, 1, 2), "POINT"),

                /// NOTE VECTOR<INTEGER32> vs VECTOR<INTEGER> - consistent with `CypherTypeValueMapper`
                Arguments.of(Values.int8Vector((byte) 1, (byte) 2), "VECTOR<INTEGER8>(2)"),
                Arguments.of(Values.int16Vector((short) 1, (short) 2), "VECTOR<INTEGER16>(2)"),
                Arguments.of(Values.int32Vector(1, 2), "VECTOR<INTEGER32>(2)"),
                Arguments.of(Values.int64Vector((byte) 1, (byte) 2, (byte) 3), "VECTOR<INTEGER>(3)"),

                /// NOTE VECTOR<FLOAT32> vs VECTOR<FLOAT> - consistent with `CypherTypeValueMapper`
                Arguments.of(Values.float32Vector(1, 2, 3), "VECTOR<FLOAT32>(3)"),
                Arguments.of(Values.float64Vector(1, 2, 3, 4), "VECTOR<FLOAT>(4)"),

                /// NOTE LIST<FLOAT> - consistent with `CypherTypeValueMapper`
                Arguments.of(Values.floatArray(new float[] {1, 2, 3}), "LIST<FLOAT>"),
                Arguments.of(Values.doubleArray(new double[] {1, 2, 3}), "LIST<FLOAT>"),
                Arguments.of(Values.charArray(new char[] {'a', 'b', 'c'}), "LIST<STRING>"),
                Arguments.of(Values.booleanArray(new boolean[] {true, false}), "LIST<BOOLEAN>"),
                Arguments.of(
                        Values.pointArray(new PointValue[] {Values.pointValue(CoordinateReferenceSystem.WGS_84, 1, 2)}),
                        "LIST<POINT>"),
                Arguments.of(Values.dateArray(new LocalDate[] {LocalDate.now(), ZD1.toLocalDate()}), "LIST<DATE>"),
                Arguments.of(
                        Values.dateTimeArray(new ZonedDateTime[] {ZonedDateTime.now(), ZD1, ZD2}),
                        "LIST<ZONED DATETIME>"),
                Arguments.of(
                        Values.localDateTimeArray(
                                new LocalDateTime[] {LocalDateTime.now(), ZD1.toLocalDateTime(), ZD2.toLocalDateTime()
                                }),
                        "LIST<LOCAL DATETIME>"),
                Arguments.of(
                        Values.timeArray(new OffsetTime[] {
                            OffsetTime.now(),
                            ZD1.toOffsetDateTime().toOffsetTime(),
                            ZD2.toOffsetDateTime().toOffsetTime()
                        }),
                        "LIST<ZONED TIME>"),
                Arguments.of(
                        Values.localTimeArray(new LocalTime[] {LocalTime.now(), ZD1.toLocalTime(), ZD2.toLocalTime()}),
                        "LIST<LOCAL TIME>"),
                Arguments.of(Values.durationArray(new DurationValue[] {DurationValue.ZERO}), "LIST<DURATION>"));
    }

    private static final String NOT_NULL = " NOT NULL";

    /// Remove the "NOT NULL" suffix of the Cypher type name
    /// We don't expect to use that
    private String cypherTypeName(Value value) {
        String s = CypherTypeValueMapper.valueType(value);
        return removeNotNullable(s);
    }

    private String removeNotNullable(String s) {
        if (s.endsWith(NOT_NULL)) {
            return removeNotNullable(s.substring(0, s.lastIndexOf(NOT_NULL)));
        }
        int left = s.indexOf("<");
        int right = s.lastIndexOf(">");
        if (left >= 0 && right > left) {
            String subtype = removeNotNullable(s.substring(left + 1, right));
            return s.substring(0, left) + "<" + subtype + ">" + s.substring(right + 1);
        }
        return s;
    }
}
