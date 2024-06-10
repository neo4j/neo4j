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
package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.ReadOperations
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.exceptions.InternalException
import org.neo4j.function.ThrowingBiConsumer
import org.neo4j.internal.kernel.api.PropertyCursor
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.NodeValue.DirectNodeValue
import org.neo4j.values.virtual.RelationshipValue.DirectRelationshipValue
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue

import java.util

import scala.jdk.CollectionConverters.MapHasAsJava

object IsMap extends MapSupport {

  def unapply(x: AnyValue): Option[QueryState => MapValue] =
    if (isMap(x)) {
      Some(castToMap(x))
    } else {
      None
    }
}

trait MapSupport {

  def isMap(x: AnyValue): Boolean = castToMap.isDefinedAt(x)

  def castToMap: PartialFunction[AnyValue, QueryState => MapValue] = {
    case x: MapValue        => _ => x
    case x: DirectNodeValue => _ => x.properties()
    case x: VirtualNodeValue => state =>
        new LazyMap(
          state.query,
          state.query.nodeReadOps,
          state.cursors.nodeCursor,
          state.cursors.propertyCursor,
          x.id()
        )
    case x: DirectRelationshipValue => _ => x.properties()
    case x: VirtualRelationshipValue => state =>
        new LazyMap(
          state.query,
          state.query.relationshipReadOps,
          state.cursors.relationshipScanCursor,
          state.cursors.propertyCursor,
          x.id()
        )
  }
}

class LazyMap[T, CURSOR](
  ctx: QueryContext,
  ops: ReadOperations[T, CURSOR],
  cursor: CURSOR,
  propertyCursor: PropertyCursor,
  id: Long
) extends MapValue {

  private lazy val allProps: util.Map[String, AnyValue] = ops.propertyKeyIds(id, cursor, propertyCursor)
    .map(propertyId => {
      val value: AnyValue = ops.getProperty(id, propertyId, cursor, propertyCursor, throwOnDeleted = true)
      ctx.getPropertyKeyName(propertyId) -> value
    }).toMap.asJava

  override def keySet(): util.Set[String] = allProps.keySet()

  override def foreach[E <: Exception](f: ThrowingBiConsumer[String, AnyValue, E]): Unit = {
    val it = allProps.entrySet().iterator()
    while (it.hasNext) {
      val entry = it.next()
      f.accept(entry.getKey, entry.getValue)
    }
  }

  override def entryExists(p: java.util.function.BiFunction[String, AnyValue, java.lang.Boolean]): Boolean = {
    val it = allProps.entrySet().iterator()
    while (it.hasNext) {
      val entry = it.next()
      if (p.apply(entry.getKey, entry.getValue)) {
        return true
      }
    }
    false
  }

  override def containsKey(key: String): Boolean =
    ctx.getOptPropertyKeyId(key).exists(propertyKeyId => ops.hasProperty(id, propertyKeyId, cursor, propertyCursor))

  override def get(key: String): AnyValue =
    ctx.getOptPropertyKeyId(key) match {
      case Some(keyId) =>
        ops.getProperty(id, keyId, cursor, propertyCursor, throwOnDeleted = true)
      case None =>
        Values.NO_VALUE
    }

  override def size(): Int = allProps.size()

  override def isEmpty: Boolean = allProps.isEmpty

  // we need a way forcefully load lazy values
  def load(): MapValue =
    if (allProps != null) this
    else throw new InternalException("properties must be loadable at this instant")

  override def estimatedHeapUsage(): Long = 0 // Turns out programmers are lazy too
}
