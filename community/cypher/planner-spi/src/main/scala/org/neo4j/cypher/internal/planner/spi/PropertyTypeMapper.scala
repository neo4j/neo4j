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
package org.neo4j.cypher.internal.planner.spi

import org.neo4j.cypher.internal.ast.BooleanTypeName
import org.neo4j.cypher.internal.ast.CypherTypeName
import org.neo4j.cypher.internal.ast.DateTypeName
import org.neo4j.cypher.internal.ast.DurationTypeName
import org.neo4j.cypher.internal.ast.FloatTypeName
import org.neo4j.cypher.internal.ast.IntegerTypeName
import org.neo4j.cypher.internal.ast.LocalDateTimeTypeName
import org.neo4j.cypher.internal.ast.LocalTimeTypeName
import org.neo4j.cypher.internal.ast.PointTypeName
import org.neo4j.cypher.internal.ast.StringTypeName
import org.neo4j.cypher.internal.ast.ZonedDateTimeTypeName
import org.neo4j.cypher.internal.ast.ZonedTimeTypeName
import org.neo4j.internal.schema.constraints.SchemaValueType
import org.neo4j.internal.schema.constraints.SchemaValueType.BOOLEAN
import org.neo4j.internal.schema.constraints.SchemaValueType.DATE
import org.neo4j.internal.schema.constraints.SchemaValueType.DURATION
import org.neo4j.internal.schema.constraints.SchemaValueType.FLOAT
import org.neo4j.internal.schema.constraints.SchemaValueType.INTEGER
import org.neo4j.internal.schema.constraints.SchemaValueType.LOCAL_DATETIME
import org.neo4j.internal.schema.constraints.SchemaValueType.LOCAL_TIME
import org.neo4j.internal.schema.constraints.SchemaValueType.POINT
import org.neo4j.internal.schema.constraints.SchemaValueType.STRING
import org.neo4j.internal.schema.constraints.SchemaValueType.ZONED_DATETIME
import org.neo4j.internal.schema.constraints.SchemaValueType.ZONED_TIME

object PropertyTypeMapper {

  def asSchemaValueType(propertyType: CypherTypeName): Option[SchemaValueType] = {
    propertyType match {
      case _: BooleanTypeName       => Some(BOOLEAN)
      case _: StringTypeName        => Some(STRING)
      case _: IntegerTypeName       => Some(INTEGER)
      case _: FloatTypeName         => Some(FLOAT)
      case _: DateTypeName          => Some(DATE)
      case _: LocalTimeTypeName     => Some(LOCAL_TIME)
      case _: ZonedTimeTypeName     => Some(ZONED_TIME)
      case _: LocalDateTimeTypeName => Some(LOCAL_DATETIME)
      case _: ZonedDateTimeTypeName => Some(ZONED_DATETIME)
      case _: DurationTypeName      => Some(DURATION)
      case _: PointTypeName         => Some(POINT)
      case _                        => None
    }
  }
}
