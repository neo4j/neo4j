/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.slotted

import org.eclipse.collections.api.set.primitive.LongSet
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.{InCheckContainer, SingleThreadedLRUCache}
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, MutableMaps}
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

import scala.collection.mutable

class SlottedQueryState(query: QueryContext,
                        resources: ExternalCSVResource,
                        params: MapValue,
                        decorator: PipeDecorator = NullPipeDecorator,
                        initialContext: Option[ExecutionContext] = None,
                        triadicState: mutable.Map[String, LongSet] = mutable.Map.empty,
                        repeatableReads: mutable.Map[Pipe, Seq[ExecutionContext]] = mutable.Map.empty,
                        cachedIn: SingleThreadedLRUCache[Any, InCheckContainer] = new SingleThreadedLRUCache(maxSize = 16),
                        lenientCreateRelationship: Boolean = false)
  extends QueryState(query, resources, params, decorator, initialContext, triadicState,
    repeatableReads, cachedIn, lenientCreateRelationship) {

  override def withDecorator(decorator: PipeDecorator) =
    new SlottedQueryState(query, resources, params, decorator, initialContext, triadicState, repeatableReads, cachedIn, lenientCreateRelationship)

  override def withInitialContext(initialContext: ExecutionContext) =
    new SlottedQueryState(query, resources, params, decorator, Some(initialContext), triadicState, repeatableReads, cachedIn, lenientCreateRelationship)

  override def withQueryContext(query: QueryContext) =
    new SlottedQueryState(query, resources, params, decorator, initialContext, triadicState, repeatableReads, cachedIn, lenientCreateRelationship)
}

case class SlottedExecutionContextFactory(slots: SlotConfiguration) extends ExecutionContextFactory {
  override def newExecutionContext(m: mutable.Map[String, AnyValue] = MutableMaps.empty): ExecutionContext =
    throw new UnsupportedOperationException("Please implement")

  override def newExecutionContext(): ExecutionContext =
    SlottedExecutionContext(slots)

  override def copyWith(row: ExecutionContext): ExecutionContext = {
    val newCtx = SlottedExecutionContext(slots)
    row.copyTo(newCtx)
    newCtx
  }

  override def copyWith(row: ExecutionContext, newEntries: Seq[(String, AnyValue)]): ExecutionContext = {
    val newCopy = SlottedExecutionContext(slots)
    row.copyTo(newCopy)
    for ((key,value) <- newEntries) {
      newCopy.set(key, value)
    }
    newCopy
  }

  override def copyWith(row: ExecutionContext, key: String, value: AnyValue): ExecutionContext = {
    val newCtx = SlottedExecutionContext(slots)
    row.copyTo(newCtx)
    newCtx.set(key, value)
    newCtx
  }

  override def copyWith(row: ExecutionContext,
                        key1: String, value1: AnyValue,
                        key2: String, value2: AnyValue): ExecutionContext = {
    val newCopy = SlottedExecutionContext(slots)
    row.copyTo(newCopy)
    newCopy.set(key1, value1)
    newCopy.set(key2, value2)
    newCopy
  }

  override def copyWith(row: ExecutionContext,
                        key1: String, value1: AnyValue,
                        key2: String, value2: AnyValue,
                        key3: String, value3: AnyValue): ExecutionContext = {
    val newCopy = SlottedExecutionContext(slots)
    row.copyTo(newCopy)
    newCopy.set(key1, value1)
    newCopy.set(key2, value2)
    newCopy.set(key3, value3)
    newCopy
  }
}
