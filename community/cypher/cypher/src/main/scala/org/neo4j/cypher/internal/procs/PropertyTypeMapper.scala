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

import org.neo4j.cypher.internal.util.symbols.BooleanType
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.DateType
import org.neo4j.cypher.internal.util.symbols.DurationType
import org.neo4j.cypher.internal.util.symbols.FloatType
import org.neo4j.cypher.internal.util.symbols.IntegerType
import org.neo4j.cypher.internal.util.symbols.ListType
import org.neo4j.cypher.internal.util.symbols.LocalDateTimeType
import org.neo4j.cypher.internal.util.symbols.LocalTimeType
import org.neo4j.cypher.internal.util.symbols.PointType
import org.neo4j.cypher.internal.util.symbols.StringType
import org.neo4j.cypher.internal.util.symbols.ZonedDateTimeType
import org.neo4j.cypher.internal.util.symbols.ZonedTimeType
import org.neo4j.internal.schema.constraints.PropertyTypeSet
import org.neo4j.internal.schema.constraints.SchemaValueType
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
import org.neo4j.internal.schema.constraints.SchemaValueType.LIST_ZONED_DATETIME
import org.neo4j.internal.schema.constraints.SchemaValueType.LIST_ZONED_TIME
import org.neo4j.internal.schema.constraints.SchemaValueType.LOCAL_DATETIME
import org.neo4j.internal.schema.constraints.SchemaValueType.LOCAL_TIME
import org.neo4j.internal.schema.constraints.SchemaValueType.POINT
import org.neo4j.internal.schema.constraints.SchemaValueType.STRING
import org.neo4j.internal.schema.constraints.SchemaValueType.ZONED_DATETIME
import org.neo4j.internal.schema.constraints.SchemaValueType.ZONED_TIME

object PropertyTypeMapper {

  def asPropertyTypeSet(propertyType: CypherType): PropertyTypeSet = {
    val schemaValueTypes = propertyType match {
      case c: ClosedDynamicUnionType =>
        // It's normalized so there isn't any inner unions to consider
        c.sortedInnerTypes.map(asSingleSchemaValueType)
      case _ =>
        List(asSingleSchemaValueType(propertyType))
    }
    PropertyTypeSet.of(schemaValueTypes: _*)
  }

  private def asSingleSchemaValueType(propertyType: CypherType): SchemaValueType = propertyType match {
    case _: BooleanType                    => BOOLEAN
    case _: StringType                     => STRING
    case _: IntegerType                    => INTEGER
    case _: FloatType                      => FLOAT
    case _: DateType                       => DATE
    case _: LocalTimeType                  => LOCAL_TIME
    case _: ZonedTimeType                  => ZONED_TIME
    case _: LocalDateTimeType              => LOCAL_DATETIME
    case _: ZonedDateTimeType              => ZONED_DATETIME
    case _: DurationType                   => DURATION
    case _: PointType                      => POINT
    case ListType(_: BooleanType, _)       => LIST_BOOLEAN
    case ListType(_: StringType, _)        => LIST_STRING
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
      throw new IllegalStateException(s"Invalid property type: ${pt.description}")
  }
}
