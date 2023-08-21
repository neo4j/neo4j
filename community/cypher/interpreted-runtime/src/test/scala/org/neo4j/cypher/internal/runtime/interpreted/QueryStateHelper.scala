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

import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.NoInput
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.ResourceMonitor
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ExternalCSVResource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NullPipeDecorator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeDecorator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.memory.NoOpMemoryTrackerForOperatorProvider
import org.neo4j.cypher.internal.runtime.memory.NoOpQueryMemoryTracker
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.spatial.Point
import org.neo4j.internal.kernel.api.AutoCloseablePlus
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.PropertyCursor
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.internal.kernel.api.TokenReadSession
import org.neo4j.io.pagecache.context.CursorContext
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory
import org.neo4j.kernel.impl.query.QueryExecutionConfiguration
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.kernel.impl.util.BaseToObjectValueWriter
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.monitoring.Monitors
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.CoordinateReferenceSystem
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP
import org.scalatestplus.mockito.MockitoSugar

import scala.collection.mutable.ArrayBuffer

object QueryStateHelper extends MockitoSugar {
  def empty: QueryState = emptyWith()

  def emptyWith(
    db: GraphDatabaseQueryService = null,
    query: QueryContext = null,
    resources: ExternalCSVResource = null,
    params: Array[AnyValue] = Array.empty,
    expressionCursors: ExpressionCursors =
      new ExpressionCursors(mockCursorFactory, CursorContext.NULL_CONTEXT, EmptyMemoryTracker.INSTANCE),
    queryIndexes: Array[IndexReadSession] = Array(mock[IndexReadSession]),
    nodeTokenIndex: Option[TokenReadSession] = Some(mock[TokenReadSession]),
    relTokenIndex: Option[TokenReadSession] = Some(mock[TokenReadSession]),
    expressionVariables: Array[AnyValue] = Array.empty,
    subscriber: QuerySubscriber = QuerySubscriber.DO_NOTHING_SUBSCRIBER,
    decorator: PipeDecorator = NullPipeDecorator,
    initialContext: Option[CypherRow] = None,
    input: InputDataStream = NoInput
  ): QueryState =
    new QueryState(
      query,
      resources,
      params,
      expressionCursors,
      queryIndexes,
      nodeTokenIndex,
      relTokenIndex,
      expressionVariables,
      subscriber,
      NoOpQueryMemoryTracker,
      NoOpMemoryTrackerForOperatorProvider,
      decorator = decorator,
      initialContext = initialContext,
      input = input
    )

  def queryStateFrom(
    db: GraphDatabaseQueryService,
    tx: InternalTransaction,
    params: Array[AnyValue] = Array.empty,
    subscriber: QuerySubscriber = QuerySubscriber.DO_NOTHING_SUBSCRIBER,
    expressionVariables: Array[AnyValue] = Array.empty
  ): QueryState = {
    val searchMonitor = new Monitors().newMonitor(classOf[IndexSearchMonitor])
    val contextFactory = Neo4jTransactionalContextFactory.create(db)
    val transactionalContext = TransactionalContextWrapper(contextFactory.newContext(
      tx,
      "X",
      EMPTY_MAP,
      QueryExecutionConfiguration.DEFAULT_CONFIG
    ))
    val queryContext = new TransactionBoundQueryContext(transactionalContext, new ResourceManager)(searchMonitor)
    val resources = mock[CSVResources]
    emptyWith(
      db = db,
      query = queryContext,
      params = params,
      expressionCursors = new ExpressionCursors(
        transactionalContext.cursors,
        transactionalContext.cursorContext,
        transactionalContext.memoryTracker
      ),
      subscriber = subscriber,
      resources = resources,
      queryIndexes = Array.empty,
      nodeTokenIndex = None,
      relTokenIndex = None,
      expressionVariables = expressionVariables
    )
  }

  def withQueryState[T](
    db: GraphDatabaseQueryService,
    tx: InternalTransaction,
    params: Array[AnyValue] = Array.empty,
    f: QueryState => T,
    subscriber: QuerySubscriber = QuerySubscriber.DO_NOTHING_SUBSCRIBER,
    expressionVariables: Array[AnyValue] = Array.empty
  ): T = {
    val queryState = queryStateFrom(db, tx, params, subscriber, expressionVariables)
    try {
      f(queryState)
    } finally {
      queryState.close()
      queryState.query.transactionalContext.close()
    }

  }

  def countStats(q: QueryState): QueryState = q.withQueryContext(query = new UpdateCountingQueryContext(q.query))

  def emptyWithValueSerialization: QueryState = {
    val context = mock[QueryContext](Mockito.RETURNS_DEEP_STUBS)
    Mockito.when(context.asObject(ArgumentMatchers.any())).thenAnswer((invocationOnMock: InvocationOnMock) =>
      toObject(invocationOnMock.getArgument(0))
    )
    emptyWith(query = context)
  }

  def emptyWithResourceManager(resourceManager: ResourceManager): QueryState = {
    val context = mock[QueryContext](Mockito.RETURNS_DEEP_STUBS)
    Mockito.when(context.resources).thenReturn(resourceManager)
    emptyWith(query = context)
  }

  class TrackClosedMonitor extends ResourceMonitor {
    private val _closedResources = new ArrayBuffer[AutoCloseablePlus]()
    override def trace(resource: AutoCloseablePlus): Unit = ()
    override def untrace(resource: AutoCloseablePlus): Unit = ()
    override def close(resource: AutoCloseablePlus): Unit = _closedResources += resource
    def closedResources: Seq[AutoCloseablePlus] = _closedResources.toSeq
  }

  def trackClosedMonitor = new TrackClosedMonitor

  private def toObject(any: AnyValue) = {
    val writer = new BaseToObjectValueWriter[RuntimeException] {
      override protected def newNodeEntityById(id: Long): Node = ???
      override protected def newRelationshipEntityById(id: Long): Relationship = ???
      override protected def newPoint(crs: CoordinateReferenceSystem, coordinate: Array[Double]): Point = ???
      override protected def newNodeEntityByElementId(elementId: String): Node = ???
      override protected def newRelationshipEntityByElementId(elementId: String): Relationship = ???
    }
    any.writeTo(writer)
    writer.value()
  }

  private def mockCursorFactory: CursorFactory = {
    val factory = mock[CursorFactory]
    when(factory.allocateNodeCursor(any())).thenReturn(mock[NodeCursor])
    when(factory.allocateRelationshipScanCursor(any())).thenReturn(mock[RelationshipScanCursor])
    when(factory.allocatePropertyCursor(any(), any())).thenReturn(mock[PropertyCursor])
    factory
  }
}
