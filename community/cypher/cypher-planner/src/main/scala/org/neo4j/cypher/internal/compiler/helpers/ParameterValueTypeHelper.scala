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
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo.FLOAT
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo.INT
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo.LOCAL_DATE_TIME
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo.LOCAL_TIME
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo.MAP
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo.POINT
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo.STRING
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo.TIME
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo.info
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
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue

object ParameterValueTypeHelper {

  def asCypherTypeMap(map: MapValue, useSizeHint: Boolean): Map[String, ParameterTypeInfo] = {
    val builder = collection.immutable.Map.newBuilder[String, ParameterTypeInfo]
    map.keySet.forEach((key: String) => builder += (key -> deriveCypherType(map.get(key), useSizeHint)))
    builder.result()
  }

  def deriveCypherType(obj: AnyValue, useSizeHint: Boolean): ParameterTypeInfo = {
    obj match {
      case v: TextValue if useSizeHint => info(CTString, v.length())
      case _: TextValue                => STRING
      case _: BooleanValue             => BOOL
      case _: IntegralValue            => INT
      case _: FloatingPointValue       => FLOAT
      case _: PointValue               => POINT
      case _: DateTimeValue            => DATE_TIME
      case _: LocalDateTimeValue       => LOCAL_DATE_TIME
      case _: TimeValue                => TIME
      case _: LocalTimeValue           => LOCAL_TIME
      case _: DateValue                => DATE
      case _: DurationValue            => DURATION
      case _: MapValue                 => MAP
      case l: ListValue                =>
        // NOTE: we need to preserve inner type for Strings since that allows us to use the text index, for other types
        //       we would end up with the same plan anyway so there is no need to keep the inner type.
        val typ = if (l.itemValueRepresentation().valueGroup() == ValueGroup.TEXT) CTList(CTString) else CTList(CTAny)
        if (useSizeHint) info(typ, l.intSize())
        else ParameterTypeInfo(typ, UnknownSize)
      case _ => ANY
    }
  }
}
