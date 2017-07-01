/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.helpers.IsMap
import org.neo4j.cypher.internal.frontend.v3_3.CypherTypeException
import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.spi.v3_3.QueryContext
import org.neo4j.values._
import org.neo4j.values.storable._
import org.neo4j.values.virtual._

object coerce {

  def apply(value: AnyValue, typ: CypherType)(implicit context: QueryContext): AnyValue = {
    val result = if (value == Values.NO_VALUE) Values.NO_VALUE else try {
      typ match {
        case CTAny => value
        case CTString => value.asInstanceOf[TextValue]
        case CTNode => value.asInstanceOf[NodeValue]
        case CTRelationship => value.asInstanceOf[EdgeValue]
        case CTPath => value.asInstanceOf[PathValue]
        case CTInteger => value.asInstanceOf[IntegralValue]
        case CTFloat => value.asInstanceOf[FloatValue]
        case CTMap => value match {
          case IsMap(m) => m
          case _ => throw cantCoerce(value, typ)
        }
        case t: ListType => value match {
          case p: PathValue if t.innerType == CTNode => throw cantCoerce(value, typ)
          case p: PathValue if t.innerType == CTRelationship => throw cantCoerce(value, typ)
          case p: PathValue => p.asList
          case l: ListValue if t.innerType == CTAny => l
          case _ => throw cantCoerce(value, typ)
        }
        case CTBoolean => value.asInstanceOf[BooleanValue]
        case CTNumber => value.asInstanceOf[NumberValue]
        case CTPoint => value.asInstanceOf[PointValue]
        case CTGeometry => value.asInstanceOf[PointValue]
        case _ => throw cantCoerce(value, typ)
      }
    }
    catch {
      case e: ClassCastException => throw cantCoerce(value, typ, Some(e))
    }
    result
  }

  private def cantCoerce(value: Any, typ: CypherType, cause: Option[Throwable] = None) =
    new CypherTypeException(s"Can't coerce `$value` to $typ", cause.orNull)
}
