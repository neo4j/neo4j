/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.procs

import org.neo4j.cypher.internal.expressions.BooleanTypeName
import org.neo4j.cypher.internal.expressions.CypherTypeName
import org.neo4j.cypher.internal.expressions.DateTypeName
import org.neo4j.cypher.internal.expressions.DurationTypeName
import org.neo4j.cypher.internal.expressions.FloatTypeName
import org.neo4j.cypher.internal.expressions.IntegerTypeName
import org.neo4j.cypher.internal.expressions.ListTypeName
import org.neo4j.cypher.internal.expressions.LocalDateTimeTypeName
import org.neo4j.cypher.internal.expressions.LocalTimeTypeName
import org.neo4j.cypher.internal.expressions.PointTypeName
import org.neo4j.cypher.internal.expressions.StringTypeName
import org.neo4j.cypher.internal.expressions.ZonedDateTimeTypeName
import org.neo4j.cypher.internal.expressions.ZonedTimeTypeName
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
    val schemaType = propertyType match {
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
    PropertyTypeSet.of(schemaType)
  }
}
