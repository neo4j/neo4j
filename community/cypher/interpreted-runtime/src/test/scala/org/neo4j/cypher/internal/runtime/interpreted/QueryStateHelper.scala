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
package org.neo4j.cypher.internal.runtime.interpreted

import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.cypher.internal.runtime.NoOpQueryMemoryTracker
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ExternalCSVResource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NullPipeDecorator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeDecorator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.spatial.Point
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.kernel.impl.util.BaseToObjectValueWriter
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.monitoring.Monitors
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.CoordinateReferenceSystem
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP
import org.scalatest.mockito.MockitoSugar

object QueryStateHelper extends MockitoSugar {
  def empty: QueryState = emptyWith()

  def emptyWith(db: GraphDatabaseQueryService = null,
                query: QueryContext = null,
                resources: ExternalCSVResource = null,
                params: Array[AnyValue] = Array.empty,
                expressionCursors: ExpressionCursors = new ExpressionCursors(mock[CursorFactory], PageCursorTracer.NULL, EmptyMemoryTracker.INSTANCE),
                queryIndexes: Array[IndexReadSession] = Array(mock[IndexReadSession]),
                expressionVariables: Array[AnyValue] = Array.empty,
                subscriber: QuerySubscriber = QuerySubscriber.DO_NOTHING_SUBSCRIBER,
                decorator: PipeDecorator = NullPipeDecorator,
                initialContext: Option[CypherRow] = None
               ):QueryState =
    new QueryState(query, resources, params, expressionCursors, queryIndexes, expressionVariables, subscriber, NoOpQueryMemoryTracker,
      decorator, initialContext = initialContext)

  def queryStateFrom(db: GraphDatabaseQueryService,
                     tx: InternalTransaction,
                     params: Array[AnyValue] = Array.empty,
                     subscriber: QuerySubscriber = QuerySubscriber.DO_NOTHING_SUBSCRIBER
                    ): QueryState = {
    val searchMonitor = new Monitors().newMonitor(classOf[IndexSearchMonitor])
    val contextFactory = Neo4jTransactionalContextFactory.create(db)
    val transactionalContext = TransactionalContextWrapper(contextFactory.newContext(tx, "X", EMPTY_MAP))
    val queryContext = new TransactionBoundQueryContext(transactionalContext, new ResourceManager)(searchMonitor)
    emptyWith(db = db,
      query = queryContext,
      params = params,
      expressionCursors = new ExpressionCursors(transactionalContext.cursors, transactionalContext.transaction.pageCursorTracer(), transactionalContext.transaction.memoryTracker()),
      subscriber = subscriber)
  }

  def withQueryState[T](db: GraphDatabaseQueryService, tx: InternalTransaction, params: Array[AnyValue] = Array.empty,
                        f: QueryState => T, subscriber: QuerySubscriber = QuerySubscriber.DO_NOTHING_SUBSCRIBER): T = {
    val queryState = queryStateFrom(db, tx, params, subscriber)
    try {
      f(queryState)
    } finally {
      queryState.close()
      queryState.query.transactionalContext.close()
    }

  }

  def countStats(q: QueryState): QueryState = q.withQueryContext(query = new UpdateCountingQueryContext(q.query))

  def emptyWithValueSerialization: QueryState = emptyWith(query = context)

  private val context = mock[QueryContext]
  Mockito.when(context.asObject(ArgumentMatchers.any())).thenAnswer((invocationOnMock: InvocationOnMock) => toObject(invocationOnMock.getArgument(0)))

  private def toObject(any: AnyValue) = {
    val writer = new BaseToObjectValueWriter[RuntimeException] {
      override protected def newNodeEntityById(id: Long): Node = ???
      override protected def newRelationshipEntityById(id: Long): Relationship = ???
      override protected def newPoint(crs: CoordinateReferenceSystem, coordinate: Array[Double]): Point = ???
    }
    any.writeTo(writer)
    writer.value()
  }
}
