/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.helpers

import java.util.{Map => JavaMap}

import org.neo4j.cypher.internal.compiler.v2_3.spi.{Operations, QueryContext}
import org.neo4j.cypher.internal.frontend.v2_3.EntityNotFoundException
import org.neo4j.graphdb.{Node, PropertyContainer, Relationship}
import org.neo4j.helpers.ThisShouldNotHappenError

import scala.collection.JavaConverters._
import scala.collection.{Iterator, Map, immutable}

object IsMap extends MapSupport {
  def unapply(x: Any): Option[(QueryContext) => Map[String, Any]] = if (isMap(x)) {
    Some(castToMap(x))
  } else {
    None
  }
}

trait MapSupport {
  def isMap(x: Any) = castToMap.isDefinedAt(x)

  def castToMap: PartialFunction[Any, (QueryContext) => Map[String, Any]] = {
    case x: Any if x.isInstanceOf[Map[_, _]] =>
      (_: QueryContext) => x.asInstanceOf[Map[String, Any]]
    case x: Any if x.isInstanceOf[JavaMap[_, _]] =>
      (_: QueryContext) => x.asInstanceOf[JavaMap[String, Any]].asScala
    case x: Node  =>
      (ctx: QueryContext) => new PropertyContainerMap(x, ctx, ctx.nodeOps)
    case x: Relationship =>
      (ctx: QueryContext) => new PropertyContainerMap(x, ctx, ctx.relationshipOps)
  }

  class PropertyContainerMap[T <: PropertyContainer](n: T, ctx: QueryContext, ops: Operations[T]) extends Map[String, Any] {
    def +[B1 >: Any](kv: (String, B1)) = throw new ThisShouldNotHappenError("Andres", "This map is not a real map")

    def -(key: String) = throw new ThisShouldNotHappenError("Andres", "This map is not a real map")

    def get(key: String) = ctx.getOptPropertyKeyId(key).flatMap( (pkId: Int) => Option(ops.getProperty(id(n), pkId)) )

    def iterator: Iterator[(String, Any)] =
      ops.propertyKeyIds(id(n)).
        map(propertyId => ctx.getPropertyKeyName(propertyId) -> ops.getProperty(id(n), propertyId)).
        toIterator

    override def contains(key: String) = ctx.getOptPropertyKeyId(key).exists(ops.hasProperty(id(n), _))

    override def apply(key: String) =
      get(key).getOrElse(throw new EntityNotFoundException("The property '%s' does not exist on %s".format(key, n)))

    private def id(x: PropertyContainer) = n match {
      case n: Node         => n.getId
      case r: Relationship => r.getId
    }
  }
}

object MapSupport {
  implicit class PowerMap[A, B](m: immutable.Map[A, B]) {
    def fuse(other: immutable.Map[A, B])(f: (B, B) => B): immutable.Map[A, B] = {
      other.foldLeft(m) {
        case (acc, (k, v)) if acc.contains(k) => acc + (k -> f(acc(k), v))
        case (acc, entry)                     => acc + entry
      }
    }
  }
}
