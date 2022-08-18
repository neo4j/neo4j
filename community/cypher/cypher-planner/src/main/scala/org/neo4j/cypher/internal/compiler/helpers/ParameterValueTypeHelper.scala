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
package org.neo4j.cypher.internal.compiler.helpers

import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTDate
import org.neo4j.cypher.internal.util.symbols.CTDateTime
import org.neo4j.cypher.internal.util.symbols.CTDuration
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTGeometry
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTLocalDateTime
import org.neo4j.cypher.internal.util.symbols.CTLocalTime
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNumber
import org.neo4j.cypher.internal.util.symbols.CTPoint
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CTTime
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.DateTimeValue
import org.neo4j.values.storable.DateValue
import org.neo4j.values.storable.DurationValue
import org.neo4j.values.storable.FloatingPointValue
import org.neo4j.values.storable.IntegralValue
import org.neo4j.values.storable.LocalDateTimeValue
import org.neo4j.values.storable.LocalTimeValue
import org.neo4j.values.storable.PointValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.TimeValue
import org.neo4j.values.storable.ValueGroup
import org.neo4j.values.storable.ValueRepresentation
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue

object ParameterValueTypeHelper {

  def asCypherTypeMap(map: MapValue): Map[String, CypherType] = {
    val builder = collection.immutable.Map.newBuilder[String, CypherType]
    map.keySet.forEach((key: String) => builder += (key -> deriveCypherType(map.get(key))))
    builder.result()
  }

  def deriveCypherType(obj: AnyValue): CypherType = { // for scala reasons, we need the cast
    obj match {
      case _: TextValue          => CTString
      case _: BooleanValue       => CTBoolean
      case _: IntegralValue      => CTInteger
      case _: FloatingPointValue =>
        // because Javascript sees everything as floats, even integer values, we need to do this until properly
        // fixing semantic checking
        CTAny
      case _: PointValue         => CTPoint
      case _: DateTimeValue      => CTDateTime
      case _: LocalDateTimeValue => CTLocalDateTime
      case _: TimeValue          => CTTime
      case _: LocalTimeValue     => CTLocalTime
      case _: DateValue          => CTDate
      case _: DurationValue      => CTDuration
      case _: MapValue           => CTMap
      case l: ListValue          => CTList(deriveInnerType(l))
      case _                     => CTAny
    }
  }

  def deriveInnerType(l: ListValue): CypherType = l.itemValueRepresentation() match {
    case ValueRepresentation.UNKNOWN               => CTAny
    case ValueRepresentation.ANYTHING              => CTAny
    case ValueRepresentation.GEOMETRY_ARRAY        => CTList(CTGeometry)
    case ValueRepresentation.ZONED_DATE_TIME_ARRAY => CTList(CTDateTime)
    case ValueRepresentation.LOCAL_DATE_TIME_ARRAY => CTList(CTLocalDateTime)
    case ValueRepresentation.DATE_ARRAY            => CTList(CTDate)
    case ValueRepresentation.ZONED_TIME_ARRAY      => CTList(CTTime)
    case ValueRepresentation.LOCAL_TIME_ARRAY      => CTList(CTLocalTime)
    case ValueRepresentation.DURATION_ARRAY        => CTList(CTDuration)
    case ValueRepresentation.TEXT_ARRAY            => CTList(CTString)
    case ValueRepresentation.BOOLEAN_ARRAY         => CTList(CTBoolean)
    case ValueRepresentation.INT64_ARRAY           => CTList(CTInteger)
    case ValueRepresentation.INT32_ARRAY           => CTList(CTInteger)
    case ValueRepresentation.INT16_ARRAY           => CTList(CTInteger)
    case ValueRepresentation.INT8_ARRAY            => CTList(CTInteger)
    case ValueRepresentation.FLOAT64_ARRAY         => CTList(CTFloat)
    case ValueRepresentation.FLOAT32_ARRAY         => CTList(CTFloat)
    case ValueRepresentation.GEOMETRY              => CTGeometry
    case ValueRepresentation.ZONED_DATE_TIME       => CTDateTime
    case ValueRepresentation.LOCAL_DATE_TIME       => CTLocalDateTime
    case ValueRepresentation.DATE                  => CTDate
    case ValueRepresentation.ZONED_TIME            => CTTime
    case ValueRepresentation.LOCAL_TIME            => CTLocalTime
    case ValueRepresentation.DURATION              => CTDuration
    case ValueRepresentation.UTF16_TEXT            => CTString
    case ValueRepresentation.UTF8_TEXT             => CTString
    case ValueRepresentation.BOOLEAN               => CTBoolean
    case ValueRepresentation.INT64                 => CTInteger
    case ValueRepresentation.INT32                 => CTInteger
    case ValueRepresentation.INT16                 => CTInteger
    case ValueRepresentation.INT8                  => CTInteger
    case ValueRepresentation.FLOAT64               => CTFloat
    case ValueRepresentation.FLOAT32               => CTFloat
    case ValueRepresentation.NO_VALUE              => CTAny
  }

}
