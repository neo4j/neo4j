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

import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTDate
import org.neo4j.cypher.internal.util.symbols.CTDateTime
import org.neo4j.cypher.internal.util.symbols.CTDuration
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTLocalDateTime
import org.neo4j.cypher.internal.util.symbols.CTLocalTime
import org.neo4j.cypher.internal.util.symbols.CTMap
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
      case _: TextValue => CTString
      case _: BooleanValue => CTBoolean
      case _: IntegralValue => CTInteger
      case _: FloatingPointValue =>
        // because Javascript sees everything as floats, even integer values, we need to do this until properly
        // fixing semantic checking
        CTAny
      case _: PointValue => CTPoint
      case _: DateTimeValue => CTDateTime
      case _: LocalDateTimeValue => CTLocalDateTime
      case _: TimeValue => CTTime
      case _: LocalTimeValue => CTLocalTime
      case _: DateValue => CTDate
      case _: DurationValue => CTDuration
      case _: MapValue => CTMap
      case _: ListValue => CTList(CTAny) // we don't care about lists in iBob
      // all else
      case _ => CTAny
    }
  }

}
