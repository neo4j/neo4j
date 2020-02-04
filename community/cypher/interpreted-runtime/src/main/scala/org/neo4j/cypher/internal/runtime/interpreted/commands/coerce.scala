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
package org.neo4j.cypher.internal.runtime.interpreted.commands

import org.neo4j.cypher.internal.runtime.IsList
import org.neo4j.cypher.internal.runtime.interpreted.IsMap
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTDate
import org.neo4j.cypher.internal.util.symbols.CTDateTime
import org.neo4j.cypher.internal.util.symbols.CTDuration
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTGeometry
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTLocalDateTime
import org.neo4j.cypher.internal.util.symbols.CTLocalTime
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTNumber
import org.neo4j.cypher.internal.util.symbols.CTPath
import org.neo4j.cypher.internal.util.symbols.CTPoint
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CTTime
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.ListType
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.DateTimeValue
import org.neo4j.values.storable.DateValue
import org.neo4j.values.storable.DurationValue
import org.neo4j.values.storable.LocalDateTimeValue
import org.neo4j.values.storable.LocalTimeValue
import org.neo4j.values.storable.NumberValue
import org.neo4j.values.storable.PointValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.TimeValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.NodeValue
import org.neo4j.values.virtual.PathValue
import org.neo4j.values.virtual.RelationshipValue
import org.neo4j.values.virtual.VirtualValues
import scala.collection.JavaConverters.asScalaIteratorConverter

object coerce {

  def apply(value: AnyValue, state: QueryState, typ: CypherType): AnyValue = {
    val result = if (value eq Values.NO_VALUE) Values.NO_VALUE else try {
      typ match {
        case CTAny => value
        case CTString => value.asInstanceOf[TextValue]
        case CTNode => value.asInstanceOf[NodeValue]
        case CTRelationship => value.asInstanceOf[RelationshipValue]
        case CTPath => value.asInstanceOf[PathValue]
        case CTInteger => Values.longValue(value.asInstanceOf[NumberValue].longValue())
        case CTFloat => Values.doubleValue(value.asInstanceOf[NumberValue].doubleValue())
        case CTMap => value match {
          case IsMap(m) => m(state)
          case _ => throw cantCoerce(value, typ)
        }
        case t: ListType => value match {
          case _: PathValue if t.innerType == CTNode => throw cantCoerce(value, typ)
          case _: PathValue if t.innerType == CTRelationship => throw cantCoerce(value, typ)
          case p: PathValue => p.asList
          case IsList(coll) if t.innerType == CTAny => coll
          case IsList(coll) => VirtualValues.list(coll.iterator().asScala.map(coerce(_, state, t.innerType)).toArray:_*)
          case _ => throw cantCoerce(value, typ)
        }
        case CTBoolean => value.asInstanceOf[BooleanValue]
        case CTNumber => value.asInstanceOf[NumberValue]
        case CTPoint => value.asInstanceOf[PointValue]
        case CTGeometry => value.asInstanceOf[PointValue]
        case CTDate => value.asInstanceOf[DateValue]
        case CTLocalTime => value.asInstanceOf[LocalTimeValue]
        case CTTime => value.asInstanceOf[TimeValue]
        case CTLocalDateTime => value.asInstanceOf[LocalDateTimeValue]
        case CTDateTime => value.asInstanceOf[DateTimeValue]
        case CTDuration => value.asInstanceOf[DurationValue]
        case _ => throw cantCoerce(value, typ)
      }
    }
    catch {
      case e: ClassCastException => throw cantCoerce(value, typ, Some(e))
    }
    result
  }

  private def cantCoerce(value: Any, typ: CypherType, cause: Option[Throwable] = None) =
    new CypherTypeException(s"Wrong argument type: Can't coerce `$value` to $typ", cause.orNull)
}
