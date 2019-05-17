/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import java.net.URL

import org.hamcrest.Matchers.greaterThan
import org.junit.Assert.assertThat
import org.mockito.Mockito._
import org.neo4j.cypher.internal.javacompat
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection
import org.neo4j.graphdb._
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.internal.kernel.api.Transaction.Type
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.internal.kernel.api.security.SecurityContext.AUTH_DISABLED
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.security.AnonymousContext
import org.neo4j.kernel.impl.api.{ClockContext, KernelStatement, KernelTransactionImplementation, StatementOperationParts}
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.kernel.impl.coreapi.{InternalTransaction, PropertyContainerLocker}
import org.neo4j.kernel.impl.locking.LockTracer
import org.neo4j.kernel.impl.newapi.DefaultCursors
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo
import org.neo4j.kernel.impl.query.{Neo4jTransactionalContext, Neo4jTransactionalContextFactory}
import org.neo4j.storageengine.api.StorageStatement
import org.neo4j.test.TestGraphDatabaseFactory
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP

import scala.collection.JavaConverters._

class TransactionBoundQueryContextTest extends CypherFunSuite {

  var graphOps: GraphDatabaseService = null
  var graph: GraphDatabaseQueryService = null
  var outerTx: InternalTransaction = null
  var statement: KernelStatement = null
  val indexSearchMonitor = mock[IndexSearchMonitor]
  val locker = mock[PropertyContainerLocker]

  override def beforeEach() {
    super.beforeEach()
    graphOps = new TestGraphDatabaseFactory().newImpermanentDatabase()
    graph = new javacompat.GraphDatabaseCypherService(graphOps)
    outerTx = mock[InternalTransaction]
    val kernelTransaction = mock[KernelTransactionImplementation]
    when(kernelTransaction.securityContext()).thenReturn(AUTH_DISABLED)
    val storeStatement = mock[StorageStatement]
    val operations = mock[StatementOperationParts](RETURNS_DEEP_STUBS)
    statement = new KernelStatement(kernelTransaction, null, storeStatement, LockTracer.NONE, operations, new ClockContext(), EmptyVersionContextSupplier.EMPTY)
    statement.initialize(null, PageCursorTracerSupplier.NULL.get())
    statement.acquire()
  }

  override def afterEach() {
    graphOps.shutdown()
  }

  test("should mark transaction successful if successful") {
    // GIVEN
    when(outerTx.failure()).thenThrow(new AssertionError("Shouldn't be called"))
    when(outerTx.transactionType()).thenReturn(Type.`implicit`)
    when(outerTx.securityContext()).thenReturn(AUTH_DISABLED)

    val bridge = mock[ThreadToStatementContextBridge]
    val transaction = mock[KernelTransaction]
    when(transaction.cursors()).thenReturn(new DefaultCursors())
    when(bridge.getKernelTransactionBoundToThisThread(true)).thenReturn(transaction)
    val tc = new Neo4jTransactionalContext(graph, null, bridge, locker, outerTx, statement, null, null)
    val transactionalContext = TransactionalContextWrapper(tc)
    val context = new TransactionBoundQueryContext(transactionalContext)(indexSearchMonitor)
    // WHEN
    context.transactionalContext.close(success = true)

    // THEN
    verify(outerTx).transactionType()
    verify(outerTx).securityContext()
    verify(outerTx).success()
    verify(outerTx).close()
    verifyNoMoreInteractions(outerTx)
  }

  test("should mark transaction failed if not successful") {
    // GIVEN
    when(outerTx.success()).thenThrow(new AssertionError("Shouldn't be called"))
    when(outerTx.transactionType()).thenReturn(Type.`implicit`)
    when(outerTx.securityContext()).thenReturn(AUTH_DISABLED)
    val bridge = mock[ThreadToStatementContextBridge]
    val transaction = mock[KernelTransaction]
    when(transaction.cursors()).thenReturn(new DefaultCursors())
    when(bridge.getKernelTransactionBoundToThisThread(true)).thenReturn(transaction)
    val tc = new Neo4jTransactionalContext(graph, null, bridge, locker, outerTx, statement, null, null)
    val transactionalContext = TransactionalContextWrapper(tc)
    val context = new TransactionBoundQueryContext(transactionalContext)(indexSearchMonitor)
    // WHEN
    context.transactionalContext.close(success = false)

    // THEN
    verify(outerTx).transactionType()
    verify(outerTx).securityContext()
    verify(outerTx).failure()
    verify(outerTx).close()
    verifyNoMoreInteractions(outerTx)
  }

  test("should return fresh but equal iterators") {
    // GIVEN
    val relTypeName = "LINK"
    val node = createMiniGraph(relTypeName)

    val tx = graph.beginTransaction(Type.explicit, AnonymousContext.read())
    val transactionalContext = TransactionalContextWrapper(createTransactionContext(graph, tx))
    val context = new TransactionBoundQueryContext(transactionalContext)(indexSearchMonitor)

    // WHEN
    val iteratorA = context.getRelationshipsForIds(node.getId, SemanticDirection.BOTH, None)
    val iteratorB = context.getRelationshipsForIds(node.getId, SemanticDirection.BOTH, None)

    // THEN
    iteratorA should not equal iteratorB
    val listA = iteratorA.toList
    val listB = iteratorB.toList
    listA should equal(listB)
    listA.size should equal(2)

    transactionalContext.close(true)
    tx.success()
    tx.close()
  }

  test("should deny non-whitelisted URL protocols for loading") {
    // GIVEN
    val tx = graph.beginTransaction(Type.explicit, AnonymousContext.read())
    val transactionalContext = TransactionalContextWrapper(createTransactionContext(graph, tx))
    val context = new TransactionBoundQueryContext(transactionalContext)(indexSearchMonitor)

    // THEN
    context.getImportURL(new URL("http://localhost:7474/data.csv")) should equal(Right(new URL("http://localhost:7474/data.csv")))
    context.getImportURL(new URL("file:///tmp/foo/data.csv")) should equal(Right(new URL("file:///tmp/foo/data.csv")))
    context.getImportURL(new URL("jar:file:/tmp/blah.jar!/tmp/foo/data.csv")) should equal(Left("loading resources via protocol 'jar' is not permitted"))

    transactionalContext.close(true)
    tx.success()
    tx.close()
  }

  test("should deny file URLs when not allowed by config") {
    // GIVEN
    graphOps.shutdown()
    startGraph(GraphDatabaseSettings.allow_file_urls -> "false")
    val tx = graph.beginTransaction(Type.explicit, AnonymousContext.read())
    val transactionalContext = TransactionalContextWrapper(createTransactionContext(graph, tx))
    val context = new TransactionBoundQueryContext(transactionalContext)(indexSearchMonitor)

    // THEN
    context.getImportURL(new URL("http://localhost:7474/data.csv")) should equal (Right(new URL("http://localhost:7474/data.csv")))
    context.getImportURL(new URL("file:///tmp/foo/data.csv")) should equal (Left("configuration property 'dbms.security.allow_csv_import_from_file_urls' is false"))

    transactionalContext.close(true)
    tx.success()
    tx.close()
  }

  test("provide access to kernel statement page cache tracer") {
    val creator = graphOps.beginTx()
    graphOps.createNode()
    graphOps.createNode()
    graphOps.createNode()
    creator.success()
    creator.close()

    val tx = graph.beginTransaction(Type.explicit, LoginContext.AUTH_DISABLED)
    val transactionalContext = TransactionalContextWrapper(createTransactionContext(graph, tx))
    val context = new TransactionBoundQueryContext(transactionalContext)(indexSearchMonitor)

    val tracer = transactionalContext.kernelStatisticProvider
    tracer.getPageCacheHits should equal(0)

    graphOps.getNodeById(2)
    graphOps.getNodeById(1)
    val accesses = tracer.getPageCacheHits + tracer.getPageCacheMisses
    assertThat(Long.box(accesses), greaterThan(Long.box(1L)))

    transactionalContext.close(true)
    tx.close()
  }

  test("should close all resources when closing resources") {
    // GIVEN
    val bridge = mock[ThreadToStatementContextBridge]
    val transaction = mock[KernelTransaction]
    when(transaction.cursors()).thenReturn(new DefaultCursors())
    when(bridge.getKernelTransactionBoundToThisThread(true)).thenReturn(transaction)
    val tc = new Neo4jTransactionalContext(graph, null, bridge, locker, outerTx, statement, null, null)
    val transactionalContext = TransactionalContextWrapper(tc)
    val context = new TransactionBoundQueryContext(transactionalContext)(indexSearchMonitor)
    val resource1 = mock[AutoCloseable]
    val resource2 = mock[AutoCloseable]
    val resource3 = mock[AutoCloseable]
    context.resources.trace(resource1)
    context.resources.trace(resource2)
    context.resources.trace(resource3)

    // WHEN
    context.resources.close(success = true)

    // THEN
    verify(resource1).close()
    verify(resource2).close()
    verify(resource3).close()
  }

  test("should add cursor as resource when calling all") {
    // GIVEN
    val tx = graph.beginTransaction(Type.explicit, AnonymousContext.read())
    val transactionalContext = TransactionalContextWrapper(createTransactionContext(graph, tx))
    val context = new TransactionBoundQueryContext(transactionalContext)(indexSearchMonitor)
    val initSize = context.resources.allResources.size

    // WHEN
    context.nodeOps.all

    // THEN
    context.resources.allResources should have size initSize + 1
    tx.close()
  }

  test("should add cursor as resource when calling allPrimitive") {
    // GIVEN
    val tx = graph.beginTransaction(Type.explicit, AnonymousContext.read())
    val transactionalContext = TransactionalContextWrapper(createTransactionContext(graph, tx))
    val context = new TransactionBoundQueryContext(transactionalContext)(indexSearchMonitor)
    val initSize = context.resources.allResources.size

    // WHEN
    context.nodeOps.allPrimitive

    // THEN
    context.resources.allResources should have size initSize + 1
    tx.close()
  }

  test("should remove cursor after closing resource") {
    // GIVEN
    val tx = graph.beginTransaction(Type.explicit, AnonymousContext.read())
    val transactionalContext = TransactionalContextWrapper(createTransactionContext(graph, tx))
    val context = new TransactionBoundQueryContext(transactionalContext)(indexSearchMonitor)
    val initSize = context.resources.allResources.size

    // WHEN
    context.nodeOps.all
    context.resources.allResources should have size initSize + 1
    context.resources.close(success = true)

    // THEN
    context.resources.allResources shouldBe empty
    tx.close()
  }

  private def startGraph(config:(Setting[_], String)) = {
    val configs = Map[Setting[_], String](config)
    graphOps = new TestGraphDatabaseFactory().newImpermanentDatabase(configs.asJava)
    graph = new GraphDatabaseCypherService(graphOps)
  }

  private def createTransactionContext(graphDatabaseQueryService: GraphDatabaseQueryService, transaction: InternalTransaction) = {
    val contextFactory = Neo4jTransactionalContextFactory.create(graphDatabaseQueryService, new PropertyContainerLocker)
    contextFactory.newContext(ClientConnectionInfo.EMBEDDED_CONNECTION, transaction, "no query", EMPTY_MAP)
  }

  private def createMiniGraph(relTypeName: String): Node = {
    val relType = RelationshipType.withName(relTypeName)
    val tx = graph.beginTransaction(Type.explicit, AnonymousContext.writeToken())
    try {
      val node = graphOps.createNode()
      val other1 = graphOps.createNode()
      val other2 = graphOps.createNode()

      node.createRelationshipTo(other1, relType)
      other2.createRelationshipTo(node, relType)
      tx.success()
      node
    }
    finally {
      tx.close()
    }
  }
}
