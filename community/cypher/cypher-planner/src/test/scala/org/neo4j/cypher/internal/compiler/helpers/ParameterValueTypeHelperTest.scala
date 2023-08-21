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
package org.neo4j.cypher.internal.compiler.helpers

import org.neo4j.cypher.internal.compiler.helpers.ParameterValueTypeHelper.asCypherTypeMap
import org.neo4j.cypher.internal.compiler.helpers.ParameterValueTypeHelper.deriveCypherType
import org.neo4j.cypher.internal.util.UnknownSize
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo.ANY
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo.BOOL
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo.DATE
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo.DATE_TIME
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo.DURATION
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo.INT
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo.LOCAL_DATE_TIME
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo.LOCAL_TIME
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo.MAP
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo.POINT
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo.STRING
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo.TIME
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.CoordinateReferenceSystem
import org.neo4j.values.storable.DateTimeValue
import org.neo4j.values.storable.DateValue
import org.neo4j.values.storable.DurationValue
import org.neo4j.values.storable.LocalDateTimeValue
import org.neo4j.values.storable.LocalTimeValue
import org.neo4j.values.storable.TimeValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValueBuilder
import org.neo4j.values.virtual.VirtualValues

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime

class ParameterValueTypeHelperTest extends CypherFunSuite {

  test("translating map of parameters into map of cypher types should work (no sizeHint)") {
    // GIVEN
    val mapValueBuilder = new MapValueBuilder()
    mapValueBuilder.add("param1", Values.booleanValue(true))
    mapValueBuilder.add("param2", Values.floatValue(1.1f))
    mapValueBuilder.add("param3", Values.EMPTY_STRING)
    mapValueBuilder.add("param4", DateTimeValue.MAX_VALUE)
    mapValueBuilder.add("param5", Values.floatValue(1f))

    // WHEN
    val resultMap = asCypherTypeMap(mapValueBuilder.build(), useSizeHint = false)

    // THEN
    resultMap.size should be(5)
    resultMap("param1") should be(BOOL)
    resultMap("param2") should be(ANY)
    resultMap("param3") should be(STRING)
    resultMap("param4") should be(DATE_TIME)
    resultMap("param5") should be(ANY)
  }

  test("translating map of parameters into map of cypher types should work (with sizeHint)") {
    // GIVEN
    val mapValueBuilder = new MapValueBuilder()
    mapValueBuilder.add("param1", Values.booleanValue(true))
    mapValueBuilder.add("param2", Values.floatValue(1.1f))
    mapValueBuilder.add("param3", Values.EMPTY_STRING)
    mapValueBuilder.add("param4", DateTimeValue.MAX_VALUE)
    mapValueBuilder.add("param5", Values.floatValue(1f))

    // WHEN
    val resultMap = asCypherTypeMap(mapValueBuilder.build(), useSizeHint = true)

    // THEN
    resultMap.size should be(5)
    resultMap("param1") should be(BOOL)
    resultMap("param2") should be(ANY)
    resultMap("param3") should be(ParameterTypeInfo.info(CTString, 0))
    resultMap("param4") should be(DATE_TIME)
    resultMap("param5") should be(ANY)
  }

  test("deriveCypherType should be correct") {
    deriveCypherType(Values.booleanValue(true), useSizeHint = false) should be(BOOL)
    deriveCypherType(Values.EMPTY_BOOLEAN_ARRAY, useSizeHint = false) should be(ANY)

    deriveCypherType(Values.ZERO_INT, useSizeHint = false) should be(INT)
    deriveCypherType(Values.EMPTY_INT_ARRAY, useSizeHint = false) should be(ANY)

    deriveCypherType(Values.doubleValue(1), useSizeHint = false) should be(ANY)
    deriveCypherType(Values.EMPTY_DOUBLE_ARRAY, useSizeHint = false) should be(ANY)

    deriveCypherType(Values.floatValue(1), useSizeHint = false) should be(ANY)
    deriveCypherType(Values.EMPTY_FLOAT_ARRAY, useSizeHint = false) should be(ANY)

    deriveCypherType(Values.shortValue(1), useSizeHint = false) should be(INT)
    deriveCypherType(Values.EMPTY_SHORT_ARRAY, useSizeHint = false) should be(ANY)

    deriveCypherType(Values.byteValue(1), useSizeHint = false) should be(INT)
    deriveCypherType(Values.EMPTY_BYTE_ARRAY, useSizeHint = false) should be(ANY)

    deriveCypherType(Values.charValue('a'), useSizeHint = true) should be(ParameterTypeInfo.info(CTString, 1))
    deriveCypherType(Values.charValue('a'), useSizeHint = false) should be(STRING)
    deriveCypherType(Values.EMPTY_CHAR_ARRAY, useSizeHint = false) should be(ANY)

    deriveCypherType(Values.EMPTY_STRING, useSizeHint = true) should be(ParameterTypeInfo.info(CTString, 0))
    deriveCypherType(Values.EMPTY_STRING, useSizeHint = false) should be(STRING)
    deriveCypherType(Values.EMPTY_TEXT_ARRAY, useSizeHint = false) should be(ANY)

    deriveCypherType(Values.pointValue(CoordinateReferenceSystem.WGS_84, 13.2, 56.7), useSizeHint = false) should be(
      POINT
    )
    deriveCypherType(
      Values.pointArray(Array(Values.pointValue(CoordinateReferenceSystem.WGS_84, 13.2, 56.7))),
      useSizeHint = false
    ) should be(ANY)

    deriveCypherType(DateTimeValue.MAX_VALUE, useSizeHint = false) should be(DATE_TIME)
    deriveCypherType(
      Values.dateTimeArray(Array(
        ZonedDateTime.of(999, 9, 8, 7, 6, 5, 4, ZoneId.of("UTC"))
      )),
      useSizeHint = false
    ) should be(ANY)

    deriveCypherType(LocalDateTimeValue.MAX_VALUE, useSizeHint = false) should be(LOCAL_DATE_TIME)
    deriveCypherType(
      Values.localDateTimeArray(Array(
        LocalDateTime.of(2018, 10, 9, 8, 7, 6, 5)
      )),
      useSizeHint = false
    ) should be(ANY)

    deriveCypherType(TimeValue.MAX_VALUE, useSizeHint = false) should be(TIME)
    deriveCypherType(
      Values.timeArray(Array(
        OffsetTime.of(20, 8, 7, 6, ZoneOffset.UTC)
      )),
      useSizeHint = false
    ) should be(ANY)

    deriveCypherType(LocalTimeValue.MAX_VALUE, useSizeHint = false) should be(LOCAL_TIME)
    deriveCypherType(
      Values.localTimeArray(Array(
        LocalTime.of(9, 28)
      )),
      useSizeHint = false
    ) should be(ANY)

    deriveCypherType(DateValue.MAX_VALUE, useSizeHint = false) should be(DATE)
    deriveCypherType(
      Values.dateArray(Array(
        LocalDate.of(1, 12, 28)
      )),
      useSizeHint = false
    ) should be(ANY)

    deriveCypherType(DurationValue.MAX_VALUE, useSizeHint = false) should be(DURATION)
    deriveCypherType(
      Values.durationArray(Array(
        DurationValue.duration(12, 10, 10, 10)
      )),
      useSizeHint = false
    ) should be(ANY)
    deriveCypherType(VirtualValues.EMPTY_MAP, useSizeHint = false) should be(MAP)

    // with size hint
    val sizeOneList = ParameterTypeInfo.info(CTList(CTAny), 1)
    val sizeOneStringList = ParameterTypeInfo.info(
      CTList(CTString),
      1
    )
    deriveCypherType(VirtualValues.EMPTY_LIST, useSizeHint = true) should be(ParameterTypeInfo.info(CTList(CTAny), 0))
    deriveCypherType(VirtualValues.list(Values.booleanValue(true)), useSizeHint = true) should be(
      sizeOneList
    )
    deriveCypherType(VirtualValues.list(Values.ZERO_INT), useSizeHint = true) should be(sizeOneList)
    deriveCypherType(VirtualValues.list(Values.doubleValue(1)), useSizeHint = true) should be(sizeOneList)
    deriveCypherType(VirtualValues.list(Values.floatValue(1)), useSizeHint = true) should be(sizeOneList)
    deriveCypherType(VirtualValues.list(Values.shortValue(1)), useSizeHint = true) should be(sizeOneList)
    deriveCypherType(VirtualValues.list(Values.byteValue(1)), useSizeHint = true) should be(sizeOneList)

    deriveCypherType(VirtualValues.list(Values.charValue('a')), useSizeHint = true) should be(sizeOneStringList)
    deriveCypherType(VirtualValues.list(Values.stringValue("a")), useSizeHint = true) should be(sizeOneStringList)
    deriveCypherType(
      VirtualValues.list(Values.pointValue(CoordinateReferenceSystem.WGS_84, 13.2, 56.7)),
      useSizeHint = true
    ) should be(
      sizeOneList
    )
    deriveCypherType(VirtualValues.list(DateTimeValue.MAX_VALUE), useSizeHint = true) should be(sizeOneList)
    deriveCypherType(VirtualValues.list(LocalDateTimeValue.MAX_VALUE), useSizeHint = true) should be(
      sizeOneList
    )
    deriveCypherType(VirtualValues.list(TimeValue.MAX_VALUE), useSizeHint = true) should be(sizeOneList)
    deriveCypherType(VirtualValues.list(LocalTimeValue.MAX_VALUE), useSizeHint = true) should be(sizeOneList)
    deriveCypherType(VirtualValues.list(DateValue.MAX_VALUE), useSizeHint = true) should be(sizeOneList)
    deriveCypherType(VirtualValues.list(DurationValue.MAX_VALUE), useSizeHint = true) should be(sizeOneList)
    deriveCypherType(VirtualValues.list(VirtualValues.EMPTY_MAP), useSizeHint = true) should be(sizeOneList)
    deriveCypherType(VirtualValues.list(VirtualValues.EMPTY_LIST), useSizeHint = true) should be(sizeOneList)

    // no size hint
    val unknownSizeList = ParameterTypeInfo(CTList(CTAny), UnknownSize)
    val unknownSizeStringList = ParameterTypeInfo(CTList(CTString), UnknownSize)
    deriveCypherType(VirtualValues.EMPTY_LIST, useSizeHint = false) should be(unknownSizeList)
    deriveCypherType(VirtualValues.list(Values.booleanValue(true)), useSizeHint = false) should be(
      unknownSizeList
    )
    deriveCypherType(VirtualValues.list(Values.ZERO_INT), useSizeHint = false) should be(unknownSizeList)
    deriveCypherType(VirtualValues.list(Values.doubleValue(1)), useSizeHint = false) should be(unknownSizeList)
    deriveCypherType(VirtualValues.list(Values.floatValue(1)), useSizeHint = false) should be(unknownSizeList)
    deriveCypherType(VirtualValues.list(Values.shortValue(1)), useSizeHint = false) should be(unknownSizeList)
    deriveCypherType(VirtualValues.list(Values.byteValue(1)), useSizeHint = false) should be(unknownSizeList)
    deriveCypherType(VirtualValues.list(Values.charValue('a')), useSizeHint = false) should be(unknownSizeStringList)
    deriveCypherType(VirtualValues.list(Values.stringValue("a")), useSizeHint = false) should be(unknownSizeStringList)
    deriveCypherType(
      VirtualValues.list(Values.pointValue(CoordinateReferenceSystem.WGS_84, 13.2, 56.7)),
      useSizeHint = false
    ) should be(
      unknownSizeList
    )
    deriveCypherType(VirtualValues.list(DateTimeValue.MAX_VALUE), useSizeHint = false) should be(unknownSizeList)
    deriveCypherType(VirtualValues.list(LocalDateTimeValue.MAX_VALUE), useSizeHint = false) should be(
      unknownSizeList
    )
    deriveCypherType(VirtualValues.list(TimeValue.MAX_VALUE), useSizeHint = false) should be(unknownSizeList)
    deriveCypherType(VirtualValues.list(LocalTimeValue.MAX_VALUE), useSizeHint = false) should be(unknownSizeList)
    deriveCypherType(VirtualValues.list(DateValue.MAX_VALUE), useSizeHint = false) should be(unknownSizeList)
    deriveCypherType(VirtualValues.list(DurationValue.MAX_VALUE), useSizeHint = false) should be(unknownSizeList)
    deriveCypherType(VirtualValues.list(VirtualValues.EMPTY_MAP), useSizeHint = false) should be(unknownSizeList)
    deriveCypherType(VirtualValues.list(VirtualValues.EMPTY_LIST), useSizeHint = false) should be(unknownSizeList)
  }
}
