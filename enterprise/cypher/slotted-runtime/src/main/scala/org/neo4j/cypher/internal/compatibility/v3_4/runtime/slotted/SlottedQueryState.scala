/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.slotted

import java.util.UUID

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
                        queryId: AnyRef = UUID.randomUUID().toString,
                        triadicState: mutable.Map[String, PrimitiveLongSet] = mutable.Map.empty,
                        repeatableReads: mutable.Map[Pipe, Seq[ExecutionContext]] = mutable.Map.empty,
                        cachedIn: SingleThreadedLRUCache[Any, InCheckContainer] =
                        new SingleThreadedLRUCache(maxSize = 16))
  extends QueryState(query, resources, params, decorator, timeReader, initialContext, queryId, triadicState,
    repeatableReads, cachedIn) {

  override def createOrGetInitialContext(factory: ExecutionContextFactory): ExecutionContext =
    initialContext.getOrElse(factory.newExecutionContext())

  override def withDecorator(decorator: PipeDecorator) =
    new SlottedQueryState(query, resources, params, decorator, timeReader, initialContext, queryId, triadicState, repeatableReads, cachedIn)

  override def withInitialContext(initialContext: ExecutionContext) =
    new SlottedQueryState(query, resources, params, decorator, timeReader, Some(initialContext), queryId, triadicState, repeatableReads, cachedIn)

  override def withQueryContext(query: QueryContext) =
    new SlottedQueryState(query, resources, params, decorator, timeReader, initialContext, queryId, triadicState, repeatableReads, cachedIn)
}

case class SlottedExecutionContextFactory(slots: SlotConfiguration) extends ExecutionContextFactory {
  override def newExecutionContext(m: mutable.Map[String, AnyValue] = MutableMaps.empty): ExecutionContext =
    throw new UnsupportedOperationException("Please implement")

  override def newExecutionContext(): ExecutionContext =
    PrimitiveExecutionContext(slots)
}