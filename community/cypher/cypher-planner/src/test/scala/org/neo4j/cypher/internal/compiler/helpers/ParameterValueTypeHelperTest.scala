/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.helpers

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.CoordinateReferenceSystem
import org.neo4j.values.storable.DateTimeValue
import org.neo4j.values.storable.DateValue
import org.neo4j.values.storable.DurationValue
import org.neo4j.values.storable.LocalDateTimeValue
import org.neo4j.values.storable.LocalTimeValue
import org.neo4j.values.storable.TimeValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues
import org.neo4j.cypher.internal.compiler.helpers.ParameterValueTypeHelper.deriveCypherType
import org.neo4j.cypher.internal.compiler.helpers.ParameterValueTypeHelper.asCypherTypeMap
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTPoint
import org.neo4j.cypher.internal.util.symbols.CTDateTime
import org.neo4j.cypher.internal.util.symbols.CTLocalDateTime
import org.neo4j.cypher.internal.util.symbols.CTTime
import org.neo4j.cypher.internal.util.symbols.CTLocalTime
import org.neo4j.cypher.internal.util.symbols.CTDate
import org.neo4j.cypher.internal.util.symbols.CTDuration
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.values.virtual.MapValueBuilder

class ParameterValueTypeHelperTest extends CypherFunSuite {

  test("translating map of parameters into map of cypher types should work") {
    // GIVEN
    val mapValueBuilder = new MapValueBuilder()
    mapValueBuilder.add("param1", Values.booleanValue(true))
    mapValueBuilder.add("param2", Values.floatValue(1.1f))
    mapValueBuilder.add("param3", Values.EMPTY_STRING)
    mapValueBuilder.add("param4", DateTimeValue.MAX_VALUE)
    mapValueBuilder.add("param5", Values.floatValue(1f))

    // WHEN
    val resultMap = asCypherTypeMap(mapValueBuilder.build())

    // THEN
    resultMap.size should be(5)
    resultMap("param1") should be(CTBoolean)
    resultMap("param2") should be(CTAny)
    resultMap("param3") should be(CTString)
    resultMap("param4") should be(CTDateTime)
    resultMap("param5") should be(CTAny)
  }

  test("deriveCypherType should be correct") {
    deriveCypherType(Values.booleanValue(true)) should be(CTBoolean)
    deriveCypherType(Values.EMPTY_BOOLEAN_ARRAY) should be(CTAny)

    deriveCypherType(Values.ZERO_INT) should be(CTInteger)
    deriveCypherType(Values.EMPTY_INT_ARRAY) should be(CTAny)

    deriveCypherType(Values.doubleValue(1)) should be(CTAny)
    deriveCypherType(Values.EMPTY_DOUBLE_ARRAY) should be(CTAny)

    deriveCypherType(Values.floatValue(1)) should be(CTAny)
    deriveCypherType(Values.EMPTY_FLOAT_ARRAY) should be(CTAny)

    deriveCypherType(Values.shortValue(1)) should be(CTInteger)
    deriveCypherType(Values.EMPTY_SHORT_ARRAY) should be(CTAny)

    deriveCypherType(Values.byteValue(1)) should be(CTInteger)
    deriveCypherType(Values.EMPTY_BYTE_ARRAY) should be(CTAny)

    deriveCypherType(Values.charValue('a')) should be(CTString)
    deriveCypherType(Values.EMPTY_CHAR_ARRAY) should be(CTAny)

    deriveCypherType(Values.EMPTY_STRING) should be(CTString)
    deriveCypherType(Values.EMPTY_TEXT_ARRAY) should be(CTAny)

    deriveCypherType(Values.pointValue( CoordinateReferenceSystem.WGS84, 13.2, 56.7 )) should be(CTPoint)
    deriveCypherType(Values.pointArray(Array(Values.pointValue(CoordinateReferenceSystem.WGS84, 13.2, 56.7 )))) should be(CTAny)

    deriveCypherType(DateTimeValue.MAX_VALUE) should be(CTDateTime)
    deriveCypherType(Values.dateTimeArray( Array(
      ZonedDateTime.of( 999, 9, 8, 7, 6, 5, 4, ZoneId.of( "UTC" ) )
    ))) should be(CTAny)

    deriveCypherType(LocalDateTimeValue.MAX_VALUE) should be(CTLocalDateTime)
    deriveCypherType(Values.localDateTimeArray( Array(
      LocalDateTime.of( 2018, 10, 9, 8, 7, 6, 5 )))) should be(CTAny)

    deriveCypherType(TimeValue.MAX_VALUE) should be(CTTime)
    deriveCypherType(Values.timeArray(Array(
      OffsetTime.of(20, 8, 7, 6, ZoneOffset.UTC)))) should be(CTAny)

    deriveCypherType(LocalTimeValue.MAX_VALUE) should be(CTLocalTime)
    deriveCypherType(Values.localTimeArray(Array(
      LocalTime.of(9, 28)))) should be(CTAny)

    deriveCypherType(DateValue.MAX_VALUE) should be(CTDate)
    deriveCypherType(Values.dateArray(Array(
      LocalDate.of(1, 12, 28)))) should be(CTAny)

    deriveCypherType(DurationValue.MAX_VALUE) should be(CTDuration)
    deriveCypherType(Values.durationArray(Array(
      DurationValue.duration(12, 10, 10, 10)))) should be(CTAny)

    deriveCypherType(VirtualValues.EMPTY_MAP) should be(CTMap)
    deriveCypherType(VirtualValues.EMPTY_LIST) should be(CTList(CTAny))
    deriveCypherType(VirtualValues.list(Values.booleanValue(true))) should be(CTList(CTAny))
    deriveCypherType(VirtualValues.list(Values.ZERO_INT)) should be(CTList(CTAny))
    deriveCypherType(VirtualValues.list(Values.doubleValue(1))) should be(CTList(CTAny))
    deriveCypherType(VirtualValues.list(Values.floatValue(1))) should be(CTList(CTAny))
    deriveCypherType(VirtualValues.list(Values.shortValue(1))) should be(CTList(CTAny))
    deriveCypherType(VirtualValues.list(Values.byteValue(1))) should be(CTList(CTAny))
    deriveCypherType(VirtualValues.list(Values.charValue('a'))) should be(CTList(CTAny))
    deriveCypherType(VirtualValues.list(Values.stringValue("a"))) should be(CTList(CTAny))
    deriveCypherType(VirtualValues.list(Values.pointValue( CoordinateReferenceSystem.WGS84, 13.2, 56.7 ))) should be(CTList(CTAny))
    deriveCypherType(VirtualValues.list(DateTimeValue.MAX_VALUE)) should be(CTList(CTAny))
    deriveCypherType(VirtualValues.list(LocalDateTimeValue.MAX_VALUE)) should be(CTList(CTAny))
    deriveCypherType(VirtualValues.list(TimeValue.MAX_VALUE)) should be(CTList(CTAny))
    deriveCypherType(VirtualValues.list(LocalTimeValue.MAX_VALUE)) should be(CTList(CTAny))
    deriveCypherType(VirtualValues.list(DateValue.MAX_VALUE)) should be(CTList(CTAny))
    deriveCypherType(VirtualValues.list(DurationValue.MAX_VALUE)) should be(CTList(CTAny))
    deriveCypherType(VirtualValues.list(VirtualValues.EMPTY_MAP)) should be(CTList(CTAny))
    deriveCypherType(VirtualValues.list(VirtualValues.EMPTY_LIST)) should be(CTList(CTAny))
  }
}
