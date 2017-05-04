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
package org.neo4j.cypher.internal.compiler.v3_3.commands

import org.neo4j.cypher.internal.compiler.v3_3.helpers.{IsList, IsMap}
import org.neo4j.cypher.internal.compiler.v3_3.spi.QueryContext
import org.neo4j.cypher.internal.compiler.v3_3.{Geometry, Point}
import org.neo4j.cypher.internal.frontend.v3_3.CypherTypeException
import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.graphdb.{Node, Path, Relationship}

object coerce {

  def apply(value: Any, typ: CypherType)(implicit context: QueryContext): Any = {
    val result = if (value == null) null else try {
      typ match {
        case CTAny => value
        case CTString => value.asInstanceOf[String]
        case CTNode => value.asInstanceOf[Node]
        case CTRelationship => value.asInstanceOf[Relationship]
        case CTPath => value.asInstanceOf[Path]
        case CTInteger => value.asInstanceOf[Number].longValue()
        case CTFloat => value.asInstanceOf[Number].doubleValue()
        case CTMap => value match {
          case IsMap(m) => m(context)
          case _ => throw cantCoerce(value, typ)
        }
        case t: ListType => value match {
          case p: Path if t.innerType == CTNode => throw cantCoerce(value, typ)
          case p: Path if t.innerType == CTRelationship => throw cantCoerce(value, typ)
          case IsList(coll) if t.innerType == CTAny => coll
          case IsList(coll) => coll.map(coerce(_, t.innerType))
          case _ => throw cantCoerce(value, typ)
        }
        case CTBoolean => value.asInstanceOf[Boolean]
        case CTNumber => value.asInstanceOf[Number]
        case CTPoint => value.asInstanceOf[Point]
        case CTGeometry => value.asInstanceOf[Geometry]
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
