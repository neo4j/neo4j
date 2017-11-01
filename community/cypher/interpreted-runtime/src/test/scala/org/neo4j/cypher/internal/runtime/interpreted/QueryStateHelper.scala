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

import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.{ArgumentMatchers, Mockito}
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{ExternalCSVResource, NullPipeDecorator, PipeDecorator, QueryState}
import org.neo4j.graphdb.spatial.Point
import org.neo4j.graphdb.{Node, Relationship}
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.impl.coreapi.{InternalTransaction, PropertyContainerLocker}
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo
import org.neo4j.kernel.impl.util.BaseToObjectValueWriter
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP

import scala.collection.mutable

object QueryStateHelper {
  def empty: QueryState = emptyWith()

  def emptyWith(db: GraphDatabaseQueryService = null,
                query: QueryContext = null,
                resources: ExternalCSVResource = null,
                params: MapValue = EMPTY_MAP,
                decorator: PipeDecorator = NullPipeDecorator,
                initialContext: Option[ExecutionContext] = None
               ):QueryState =
    new QueryState(query, resources, params, decorator,
      triadicState = mutable.Map.empty, repeatableReads = mutable.Map.empty, initialContext = initialContext)

  private val locker: PropertyContainerLocker = new PropertyContainerLocker

  def queryStateFrom(db: GraphDatabaseQueryService,
                     tx: InternalTransaction,
                     params: MapValue = EMPTY_MAP
                    ): QueryState = {
    val searchMonitor = new KernelMonitors().newMonitor(classOf[IndexSearchMonitor])
    val contextFactory = Neo4jTransactionalContextFactory.create(db, locker)
    val transactionalContext = TransactionalContextWrapper(contextFactory.newContext(ClientConnectionInfo.EMBEDDED_CONNECTION, tx, "X", EMPTY_MAP))
    val queryContext = new TransactionBoundQueryContext(transactionalContext)(searchMonitor)
    emptyWith(db = db, query = queryContext, params = params)
  }

  def withQueryState[T](db: GraphDatabaseQueryService, tx: InternalTransaction, params: MapValue = EMPTY_MAP, f: (QueryState) => T)  = {
    val queryState = queryStateFrom(db, tx, params)
    try {
      f(queryState)
    } finally {
      queryState.query.transactionalContext.close(true)
    }

  }

  def countStats(q: QueryState) = q.withQueryContext(query = new UpdateCountingQueryContext(q.query))

  def emptyWithValueSerialization: QueryState = emptyWith(query = context)

  private val context = Mockito.mock(classOf[QueryContext])
  Mockito.when(context.asObject(ArgumentMatchers.any())).thenAnswer(new Answer[Any] {
    override def answer(invocationOnMock: InvocationOnMock): AnyRef = toObject(invocationOnMock.getArgument(0))
  })

  private def toObject(any: AnyValue) = {
    val writer = new BaseToObjectValueWriter[RuntimeException] {
      override protected def newNodeProxyById(id: Long): Node = ???
      override protected def newRelationshipProxyById(id: Long): Relationship = ???
      override protected def newGeographicPoint(longitude: Double, latitude: Double, name: String,
                                                code: Int,
                                                href: String): Point = ???
      override protected def newCartesianPoint(x: Double, y: Double, name: String, code: Int,
                                               href: String): Point = ???
    }
    any.writeTo(writer)
    writer.value()
  }
}
