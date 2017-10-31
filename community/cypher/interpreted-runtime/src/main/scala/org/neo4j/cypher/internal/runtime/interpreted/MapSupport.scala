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
package org.neo4j.cypher.internal.runtime.interpreted

import java.util
import java.util.Map

import org.neo4j.cypher.internal.runtime.{Operations, QueryContext}
import org.neo4j.graphdb.PropertyContainer
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.{EdgeValue, MapValue, NodeValue, VirtualValues}

import scala.collection.immutable

object IsMap extends MapSupport {

  def unapply(x: AnyValue): Option[QueryContext => MapValue] = if (isMap(x)) {
    Some(castToMap(x))
  } else {
    None
  }
}

trait MapSupport {

  def isMap(x: AnyValue): Boolean = castToMap.isDefinedAt(x)

  def castToMap: PartialFunction[AnyValue, QueryContext => MapValue] = {
    case x: MapValue => _ => x
    case x: NodeValue => ctx => VirtualValues.map(new LazyMap(ctx, ctx.nodeOps, x.id()))
    case x: EdgeValue => ctx => VirtualValues.map(new LazyMap(ctx, ctx.relationshipOps, x.id()))
  }
}

class LazyMap[T <: PropertyContainer](ctx: QueryContext, ops: Operations[T], id: Long)
  extends java.util.Map[String, AnyValue] {

  import scala.collection.JavaConverters._

  private lazy val allProps: util.Map[String, AnyValue] = ops.propertyKeyIds(id)
    .map(propertyId => {
      val value: AnyValue = ops.getProperty(id, propertyId)
      ctx.getPropertyKeyName(propertyId) -> value
    }
    ).toMap.asJava

  override def values(): util.Collection[AnyValue] = allProps.values()

  override def containsValue(value: scala.Any): Boolean = allProps.containsValue(value)

  override def remove(key: scala.Any): AnyValue = throw new UnsupportedOperationException()

  override def put(key: String,
                   value: AnyValue): AnyValue = throw new UnsupportedOperationException()

  override def putAll(m: util.Map[_ <: String, _ <: AnyValue]): Unit = throw new UnsupportedOperationException()

  override def get(key: scala.Any): AnyValue = ops.getProperty(id, ctx.getPropertyKeyId(key.asInstanceOf[String]))

  override def keySet(): util.Set[String] = allProps.keySet()

  override def entrySet(): util.Set[Map.Entry[String, AnyValue]] = allProps.entrySet()

  override def containsKey(key: Any): Boolean = ctx.getOptPropertyKeyId(key.asInstanceOf[String])
    .exists(ops.hasProperty(id, _))

  override def clear(): Unit = throw new UnsupportedOperationException

  override lazy val isEmpty: Boolean = ops.propertyKeyIds(id).isEmpty

  override lazy val size: Int = ops.propertyKeyIds(id).size

  override def hashCode(): Int = allProps.hashCode()

  override def equals(obj: scala.Any): Boolean = allProps.equals(obj)
}

object MapSupport {

  implicit class PowerMap[A, B](m: immutable.Map[A, B]) {

    def fuse(other: immutable.Map[A, B])(f: (B, B) => B): immutable.Map[A, B] = {
      other.foldLeft(m) {
        case (acc, (k, v)) if acc.contains(k) => acc + (k -> f(acc(k), v))
        case (acc, entry) => acc + entry
      }
    }
  }

}
