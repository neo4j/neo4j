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
package org.neo4j.cypher.internal.procs

import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.BooleanType
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.DateType
import org.neo4j.cypher.internal.util.symbols.DurationType
import org.neo4j.cypher.internal.util.symbols.Float32Type
import org.neo4j.cypher.internal.util.symbols.FloatType
import org.neo4j.cypher.internal.util.symbols.Integer16Type
import org.neo4j.cypher.internal.util.symbols.Integer32Type
import org.neo4j.cypher.internal.util.symbols.Integer8Type
import org.neo4j.cypher.internal.util.symbols.IntegerType
import org.neo4j.cypher.internal.util.symbols.ListType
import org.neo4j.cypher.internal.util.symbols.LocalDateTimeType
import org.neo4j.cypher.internal.util.symbols.LocalTimeType
import org.neo4j.cypher.internal.util.symbols.PointType
import org.neo4j.cypher.internal.util.symbols.StringType
import org.neo4j.cypher.internal.util.symbols.UUIDType
import org.neo4j.cypher.internal.util.symbols.VectorType
import org.neo4j.cypher.internal.util.symbols.ZonedDateTimeType
import org.neo4j.cypher.internal.util.symbols.ZonedTimeType
import org.neo4j.exceptions.InternalException
import org.neo4j.graphdb.Vector.CoordinateType
import org.neo4j.internal.schema.constraints.ConstrainableType
import org.neo4j.internal.schema.constraints.PropertyTypeSet
import org.neo4j.internal.schema.constraints.SchemaValueType.BOOLEAN
import org.neo4j.internal.schema.constraints.SchemaValueType.DATE
import org.neo4j.internal.schema.constraints.SchemaValueType.DURATION
import org.neo4j.internal.schema.constraints.SchemaValueType.FLOAT
import org.neo4j.internal.schema.constraints.SchemaValueType.INTEGER
import org.neo4j.internal.schema.constraints.SchemaValueType.LIST_BOOLEAN
import org.neo4j.internal.schema.constraints.SchemaValueType.LIST_DATE
import org.neo4j.internal.schema.constraints.SchemaValueType.LIST_DURATION
import org.neo4j.internal.schema.constraints.SchemaValueType.LIST_FLOAT
import org.neo4j.internal.schema.constraints.SchemaValueType.LIST_INTEGER
import org.neo4j.internal.schema.constraints.SchemaValueType.LIST_LOCAL_DATETIME
import org.neo4j.internal.schema.constraints.SchemaValueType.LIST_LOCAL_TIME
import org.neo4j.internal.schema.constraints.SchemaValueType.LIST_POINT
import org.neo4j.internal.schema.constraints.SchemaValueType.LIST_STRING
import org.neo4j.internal.schema.constraints.SchemaValueType.LIST_UUID
import org.neo4j.internal.schema.constraints.SchemaValueType.LIST_ZONED_DATETIME
import org.neo4j.internal.schema.constraints.SchemaValueType.LIST_ZONED_TIME
import org.neo4j.internal.schema.constraints.SchemaValueType.LOCAL_DATETIME
import org.neo4j.internal.schema.constraints.SchemaValueType.LOCAL_TIME
import org.neo4j.internal.schema.constraints.SchemaValueType.POINT
import org.neo4j.internal.schema.constraints.SchemaValueType.STRING
import org.neo4j.internal.schema.constraints.SchemaValueType.UUID
import org.neo4j.internal.schema.constraints.SchemaValueType.ZONED_DATETIME
import org.neo4j.internal.schema.constraints.SchemaValueType.ZONED_TIME

object PropertyTypeMapper {

  def asPropertyTypeSet(propertyType: CypherType): PropertyTypeSet = {
    val constrainableTypes = propertyType match {
      case c: ClosedDynamicUnionType =>
        // It's normalized so there isn't any inner unions to consider
        c.sortedInnerTypes.map(asSingleConstrainableType)
      case _ =>
        List(asSingleConstrainableType(propertyType))
    }
    PropertyTypeSet.of(constrainableTypes: _*)
  }

  private def asSingleConstrainableType(propertyType: CypherType): ConstrainableType = propertyType match {
    case _: BooleanType       => BOOLEAN
    case _: StringType        => STRING
    case _: UUIDType          => UUID
    case _: IntegerType       => INTEGER
    case _: FloatType         => FLOAT
    case _: DateType          => DATE
    case _: LocalTimeType     => LOCAL_TIME
    case _: ZonedTimeType     => ZONED_TIME
    case _: LocalDateTimeType => LOCAL_DATETIME
    case _: ZonedDateTimeType => ZONED_DATETIME
    case _: DurationType      => DURATION
    case _: PointType         => POINT
    case VectorType(Some(Integer8Type(false)), Some(dim), _) =>
      org.neo4j.internal.schema.constraints.VectorType.int8Vector(dim.toInt)
    case VectorType(Some(Integer16Type(false)), Some(dim), _) =>
      org.neo4j.internal.schema.constraints.VectorType.int16Vector(dim.toInt)
    case VectorType(Some(Integer32Type(false)), Some(dim), _) =>
      org.neo4j.internal.schema.constraints.VectorType.int32Vector(dim.toInt)
    case VectorType(Some(IntegerType(false)), Some(dim), _) =>
      org.neo4j.internal.schema.constraints.VectorType.int64Vector(dim.toInt)
    case VectorType(Some(Float32Type(false)), Some(dim), _) =>
      org.neo4j.internal.schema.constraints.VectorType.float32Vector(dim.toInt)
    case VectorType(Some(FloatType(false)), Some(dim), _) =>
      org.neo4j.internal.schema.constraints.VectorType.float64Vector(dim.toInt)
    case ListType(_: BooleanType, _)       => LIST_BOOLEAN
    case ListType(_: StringType, _)        => LIST_STRING
    case ListType(_: UUIDType, _)          => LIST_UUID
    case ListType(_: IntegerType, _)       => LIST_INTEGER
    case ListType(_: FloatType, _)         => LIST_FLOAT
    case ListType(_: DateType, _)          => LIST_DATE
    case ListType(_: LocalTimeType, _)     => LIST_LOCAL_TIME
    case ListType(_: ZonedTimeType, _)     => LIST_ZONED_TIME
    case ListType(_: LocalDateTimeType, _) => LIST_LOCAL_DATETIME
    case ListType(_: ZonedDateTimeType, _) => LIST_ZONED_DATETIME
    case ListType(_: DurationType, _)      => LIST_DURATION
    case ListType(_: PointType, _)         => LIST_POINT
    case pt =>
      throw InternalException.internalError(
        this.getClass.getSimpleName,
        s"Invalid property type: ${pt.description}.",
        s"Invalid property type: ${pt.description}"
      )
  }

  // Will have InputPosition.NONE
  def asCypherType(propertyTypeSet: PropertyTypeSet): CypherType = {
    val cypherTypeList = propertyTypeSet.values().toList.map {
      case BOOLEAN        => BooleanType(isNullable = true)(InputPosition.NONE)
      case STRING         => StringType(isNullable = true)(InputPosition.NONE)
      case UUID           => UUIDType(isNullable = true)(InputPosition.NONE)
      case INTEGER        => IntegerType(isNullable = true)(InputPosition.NONE)
      case FLOAT          => FloatType(isNullable = true)(InputPosition.NONE)
      case DATE           => DateType(isNullable = true)(InputPosition.NONE)
      case LOCAL_TIME     => LocalTimeType(isNullable = true)(InputPosition.NONE)
      case ZONED_TIME     => ZonedTimeType(isNullable = true)(InputPosition.NONE)
      case LOCAL_DATETIME => LocalDateTimeType(isNullable = true)(InputPosition.NONE)
      case ZONED_DATETIME => ZonedDateTimeType(isNullable = true)(InputPosition.NONE)
      case DURATION       => DurationType(isNullable = true)(InputPosition.NONE)
      case POINT          => PointType(isNullable = true)(InputPosition.NONE)
      case vectorType: org.neo4j.internal.schema.constraints.VectorType =>
        val innerType = vectorType.coordinateType() match {
          case CoordinateType.INTEGER8  => Integer8Type(isNullable = false)(InputPosition.NONE)
          case CoordinateType.INTEGER16 => Integer16Type(isNullable = false)(InputPosition.NONE)
          case CoordinateType.INTEGER32 => Integer32Type(isNullable = false)(InputPosition.NONE)
          case CoordinateType.INTEGER64 => IntegerType(isNullable = false)(InputPosition.NONE)
          case CoordinateType.FLOAT32   => Float32Type(isNullable = false)(InputPosition.NONE)
          case CoordinateType.FLOAT64   => FloatType(isNullable = false)(InputPosition.NONE)
        }
        VectorType(Some(innerType), Some(vectorType.dimensions().toLong), isNullable = true)(InputPosition.NONE)
      case LIST_BOOLEAN =>
        ListType(BooleanType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE)
      case LIST_STRING =>
        ListType(StringType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE)
      case LIST_UUID =>
        ListType(UUIDType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE)
      case LIST_INTEGER =>
        ListType(IntegerType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE)
      case LIST_FLOAT =>
        ListType(FloatType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE)
      case LIST_DATE =>
        ListType(DateType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE)
      case LIST_LOCAL_TIME =>
        ListType(LocalTimeType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE)
      case LIST_ZONED_TIME =>
        ListType(ZonedTimeType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE)
      case LIST_LOCAL_DATETIME =>
        ListType(LocalDateTimeType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE)
      case LIST_ZONED_DATETIME =>
        ListType(ZonedDateTimeType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE)
      case LIST_DURATION =>
        ListType(DurationType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE)
      case LIST_POINT =>
        ListType(PointType(isNullable = false)(InputPosition.NONE), isNullable = true)(InputPosition.NONE)
      case pt =>
        throw InternalException.internalError(
          this.getClass.getSimpleName,
          s"Invalid property type: ${pt.userDescription()}.",
          s"Invalid property type: ${pt.userDescription()}"
        )
    }

    if (cypherTypeList.size == 1) {
      // Single CypherType
      cypherTypeList.head
    } else {
      // Union Cypher Type
      ClosedDynamicUnionType(cypherTypeList.toSet)(InputPosition.NONE)
    }
  }
}
