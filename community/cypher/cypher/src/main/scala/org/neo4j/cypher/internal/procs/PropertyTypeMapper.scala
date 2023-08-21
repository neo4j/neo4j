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

import org.neo4j.cypher.internal.ast.BooleanTypeName
import org.neo4j.cypher.internal.ast.ClosedDynamicUnionTypeName
import org.neo4j.cypher.internal.ast.CypherTypeName
import org.neo4j.cypher.internal.ast.DateTypeName
import org.neo4j.cypher.internal.ast.DurationTypeName
import org.neo4j.cypher.internal.ast.FloatTypeName
import org.neo4j.cypher.internal.ast.IntegerTypeName
import org.neo4j.cypher.internal.ast.ListTypeName
import org.neo4j.cypher.internal.ast.LocalDateTimeTypeName
import org.neo4j.cypher.internal.ast.LocalTimeTypeName
import org.neo4j.cypher.internal.ast.PointTypeName
import org.neo4j.cypher.internal.ast.StringTypeName
import org.neo4j.cypher.internal.ast.ZonedDateTimeTypeName
import org.neo4j.cypher.internal.ast.ZonedTimeTypeName
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

  def asPropertyTypeSet(propertyType: CypherTypeName): PropertyTypeSet = {
    val schemaValueTypes = propertyType match {
      case c: ClosedDynamicUnionTypeName =>
        // It's normalized so there isn't any inner unions to consider
        c.sortedInnerTypes.map(asSingleSchemaValueType)
      case _ =>
        List(asSingleSchemaValueType(propertyType))
    }
    PropertyTypeSet.of(schemaValueTypes: _*)
  }

  private def asSingleSchemaValueType(propertyType: CypherTypeName): SchemaValueType = propertyType match {
    case _: BooleanTypeName                        => BOOLEAN
    case _: StringTypeName                         => STRING
    case _: IntegerTypeName                        => INTEGER
    case _: FloatTypeName                          => FLOAT
    case _: DateTypeName                           => DATE
    case _: LocalTimeTypeName                      => LOCAL_TIME
    case _: ZonedTimeTypeName                      => ZONED_TIME
    case _: LocalDateTimeTypeName                  => LOCAL_DATETIME
    case _: ZonedDateTimeTypeName                  => ZONED_DATETIME
    case _: DurationTypeName                       => DURATION
    case _: PointTypeName                          => POINT
    case ListTypeName(_: BooleanTypeName, _)       => LIST_BOOLEAN
    case ListTypeName(_: StringTypeName, _)        => LIST_STRING
    case ListTypeName(_: IntegerTypeName, _)       => LIST_INTEGER
    case ListTypeName(_: FloatTypeName, _)         => LIST_FLOAT
    case ListTypeName(_: DateTypeName, _)          => LIST_DATE
    case ListTypeName(_: LocalTimeTypeName, _)     => LIST_LOCAL_TIME
    case ListTypeName(_: ZonedTimeTypeName, _)     => LIST_ZONED_TIME
    case ListTypeName(_: LocalDateTimeTypeName, _) => LIST_LOCAL_DATETIME
    case ListTypeName(_: ZonedDateTimeTypeName, _) => LIST_ZONED_DATETIME
    case ListTypeName(_: DurationTypeName, _)      => LIST_DURATION
    case ListTypeName(_: PointTypeName, _)         => LIST_POINT
    case pt =>
      throw new IllegalStateException(s"Invalid property type: ${pt.description}")
  }
}
