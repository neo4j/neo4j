/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.slotted

import org.neo4j.collection.primitive.PrimitiveLongSet
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.SlotConfiguration
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
                        timeReader: TimeReader = new TimeReader,
                        initialContext: Option[ExecutionContext] = None,
                        triadicState: mutable.Map[String, PrimitiveLongSet] = mutable.Map.empty,
                        repeatableReads: mutable.Map[Pipe, Seq[ExecutionContext]] = mutable.Map.empty,
                        cachedIn: SingleThreadedLRUCache[Any, InCheckContainer] = new SingleThreadedLRUCache(maxSize = 16),
                        lenientCreateRelationship: Boolean = false)
  extends QueryState(query, resources, params, decorator, timeReader, initialContext, triadicState,
    repeatableReads, cachedIn, lenientCreateRelationship) {

  override def createOrGetInitialContext(factory: ExecutionContextFactory): ExecutionContext =
    initialContext.getOrElse(factory.newExecutionContext())

  override def withDecorator(decorator: PipeDecorator) =
    new SlottedQueryState(query, resources, params, decorator, timeReader, initialContext, triadicState, repeatableReads, cachedIn, lenientCreateRelationship)

  override def withInitialContext(initialContext: ExecutionContext) =
    new SlottedQueryState(query, resources, params, decorator, timeReader, Some(initialContext), triadicState, repeatableReads, cachedIn, lenientCreateRelationship)

  override def withQueryContext(query: QueryContext) =
    new SlottedQueryState(query, resources, params, decorator, timeReader, initialContext, triadicState, repeatableReads, cachedIn, lenientCreateRelationship)
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
