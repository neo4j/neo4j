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

import java.lang.Boolean.FALSE
import java.net.URL
import java.util.concurrent.atomic.AtomicReference

import org.hamcrest.Matchers.greaterThan
import org.junit.Assert.assertThat
import org.mockito.Mockito._
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.cypher.internal.javacompat
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb._
import org.neo4j.graphdb.config.Setting
import org.neo4j.internal.kernel.api.AutoCloseablePlus
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.internal.kernel.api.security.SecurityContext.AUTH_DISABLED
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.kernel.api.query.ExecutingQuery
import org.neo4j.kernel.api.security.AnonymousContext
import org.neo4j.kernel.database.TestDatabaseIdRepository
import org.neo4j.kernel.impl.api.{ClockContext, KernelStatement, KernelTransactionImplementation}
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.factory.KernelTransactionFactory
import org.neo4j.kernel.impl.newapi.DefaultPooledCursors
import org.neo4j.kernel.impl.query.{Neo4jTransactionalContext, Neo4jTransactionalContextFactory}
import org.neo4j.lock.LockTracer
import org.neo4j.resources.CpuClock
import org.neo4j.test.TestDatabaseManagementServiceBuilder
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP

import scala.collection.JavaConverters._

class TransactionBoundQueryContextTest extends CypherFunSuite {

  var managementService: DatabaseManagementService = _
  var graphOps: GraphDatabaseService = null
  var graph: GraphDatabaseQueryService = null
  var outerTx: InternalTransaction = null
  var transactionFactory: KernelTransactionFactory = _
  var statement: KernelStatement = null
  val indexSearchMonitor = mock[IndexSearchMonitor]

  override def beforeEach() {
    super.beforeEach()
    managementService = new TestDatabaseManagementServiceBuilder().impermanent().build()
    graphOps = managementService.database(DEFAULT_DATABASE_NAME)
    graph = new javacompat.GraphDatabaseCypherService(graphOps)
    transactionFactory = mock[KernelTransactionFactory]
    outerTx = mock[InternalTransaction]
    val kernelTransaction = mock[KernelTransactionImplementation](RETURNS_DEEP_STUBS)
    when(kernelTransaction.securityContext()).thenReturn(AUTH_DISABLED)
    when(kernelTransaction.acquireStatement()).thenReturn(statement)
    statement = new KernelStatement(kernelTransaction, LockTracer.NONE, new ClockContext(), EmptyVersionContextSupplier.EMPTY,
      new AtomicReference[CpuClock](CpuClock.NOT_AVAILABLE), new TestDatabaseIdRepository().defaultDatabase)
    statement.initialize(null, PageCursorTracerSupplier.NULL.get(), 7)
    statement.acquire()
  }

  override def afterEach() {
    managementService.shutdown()
  }

  test("should mark transaction successful if successful") {
    // GIVEN
    when(outerTx.rollback()).thenThrow(new AssertionError("Shouldn't be called"))
    when(outerTx.transactionType()).thenReturn(Type.`implicit`)
    when(outerTx.securityContext()).thenReturn(AUTH_DISABLED)
    when(outerTx.clientInfo()).thenReturn(ClientConnectionInfo.EMBEDDED_CONNECTION)

    val transaction = mock[KernelTransaction]
    when(transaction.cursors()).thenReturn(new DefaultPooledCursors(null))
    val tc = new Neo4jTransactionalContext(graph, outerTx, statement, mock[ExecutingQuery], transactionFactory)
    val transactionalContext = TransactionalContextWrapper(tc)
    val context = new TransactionBoundQueryContext(transactionalContext, new ResourceManager)(indexSearchMonitor)
    // WHEN
    context.transactionalContext.close()

    // THEN
    verify(outerTx).transactionType()
    verify(outerTx).clientInfo()
    verify(outerTx).securityContext()
    verify(outerTx).kernelTransaction()
    verifyNoMoreInteractions(outerTx)
  }

  test("should mark transaction failed if not successful") {
    // GIVEN
    when(outerTx.commit()).thenThrow(new AssertionError("Shouldn't be called"))
    when(outerTx.transactionType()).thenReturn(Type.`implicit`)
    when(outerTx.securityContext()).thenReturn(AUTH_DISABLED)
    when(outerTx.clientInfo()).thenReturn(ClientConnectionInfo.EMBEDDED_CONNECTION)
    val transaction = mock[KernelTransaction]
    when(transaction.acquireStatement()).thenReturn(statement)
    when(transaction.cursors()).thenReturn(new DefaultPooledCursors(null))
    val tc = new Neo4jTransactionalContext(graph, outerTx, statement, mock[ExecutingQuery], transactionFactory)
    val transactionalContext = TransactionalContextWrapper(tc)
    val context = new TransactionBoundQueryContext(transactionalContext, new ResourceManager)(indexSearchMonitor)
    // WHEN
    context.transactionalContext.close()

    // THEN
    verify(outerTx).transactionType()
    verify(outerTx).clientInfo()
    verify(outerTx).securityContext()
    verify(outerTx).kernelTransaction()
    verifyNoMoreInteractions(outerTx)
  }

  test("should return fresh but equal iterators") {
    // GIVEN
    val relTypeName = "LINK"
    val node = createMiniGraph(relTypeName)

    val tx = graph.beginTransaction(Type.explicit, AnonymousContext.read())
    val transactionalContext = TransactionalContextWrapper(createTransactionContext(graph, tx))
    val context = new TransactionBoundQueryContext(transactionalContext, new ResourceManager)(indexSearchMonitor)

    // WHEN
    val iteratorA = context.getRelationshipsForIds(node.getId, SemanticDirection.BOTH, null)
    val iteratorB = context.getRelationshipsForIds(node.getId, SemanticDirection.BOTH, null)

    // THEN
    iteratorA should not equal iteratorB
    val listA = iteratorA.toList
    val listB = iteratorB.toList
    listA should equal(listB)
    listA.size should equal(2)

    transactionalContext.close()
    tx.close()
  }

  test("should deny non-whitelisted URL protocols for loading") {
    // GIVEN
    val tx = graph.beginTransaction(Type.explicit, AnonymousContext.read())
    val transactionalContext = TransactionalContextWrapper(createTransactionContext(graph, tx))
    val context = new TransactionBoundQueryContext(transactionalContext, new ResourceManager)(indexSearchMonitor)

    // THEN
    context.getImportURL(new URL("http://localhost:7474/data.csv")) should equal(Right(new URL("http://localhost:7474/data.csv")))
    context.getImportURL(new URL("file:///tmp/foo/data.csv")) should equal(Right(new URL("file:///tmp/foo/data.csv")))
    context.getImportURL(new URL("jar:file:/tmp/blah.jar!/tmp/foo/data.csv")) should equal(Left("loading resources via protocol 'jar' is not permitted"))

    transactionalContext.close()
    tx.close()
  }

  test("should deny file URLs when not allowed by config") {
    // GIVEN
    managementService.shutdown()
    startGraph(GraphDatabaseSettings.allow_file_urls -> FALSE)
    val tx = graph.beginTransaction(Type.explicit, AnonymousContext.read())
    val transactionalContext = TransactionalContextWrapper(createTransactionContext(graph, tx))
    val context = new TransactionBoundQueryContext(transactionalContext, new ResourceManager)(indexSearchMonitor)

    // THEN
    context.getImportURL(new URL("http://localhost:7474/data.csv")) should equal (Right(new URL("http://localhost:7474/data.csv")))
    context.getImportURL(new URL("file:///tmp/foo/data.csv")) should equal (Left("configuration property 'dbms.security.allow_csv_import_from_file_urls' is false"))

    transactionalContext.close()
    tx.close()
  }

  test("provide access to kernel statement page cache tracer") {
    val creator = graphOps.beginTx()
    creator.createNode()
    creator.createNode()
    creator.createNode()
    creator.commit()

    val tx = graph.beginTransaction(Type.explicit, LoginContext.AUTH_DISABLED)
    val transactionalContext = TransactionalContextWrapper(createTransactionContext(graph, tx))

    val tracer = transactionalContext.kernelStatisticProvider
    tracer.getPageCacheHits should equal(0)

    tx.getNodeById(2)
    tx.getNodeById(1)
    val accesses = tracer.getPageCacheHits + tracer.getPageCacheMisses
    assertThat(Long.box(accesses), greaterThan(Long.box(1L)))

    transactionalContext.close()
    tx.close()
  }

  test("should close all resources when closing resources") {
    // GIVEN
    val transaction = mock[KernelTransaction]
    when(transaction.acquireStatement()).thenReturn(statement)
    when(transaction.cursors()).thenReturn(new DefaultPooledCursors(null))
    val tc = new Neo4jTransactionalContext(graph, outerTx, statement, mock[ExecutingQuery], transactionFactory)
    val transactionalContext = TransactionalContextWrapper(tc)
    val context = new TransactionBoundQueryContext(transactionalContext, new ResourceManager)(indexSearchMonitor)
    val resource1 = mock[AutoCloseablePlus]
    val resource2 = mock[AutoCloseablePlus]
    val resource3 = mock[AutoCloseablePlus]
    context.resources.trace(resource1)
    context.resources.trace(resource2)
    context.resources.trace(resource3)

    // WHEN
    context.resources.close()

    // THEN
    verify(resource1).close()
    verify(resource2).close()
    verify(resource3).close()
  }

  test("should add cursor as resource when calling all") {
    // GIVEN
    val tx = graph.beginTransaction(Type.explicit, AnonymousContext.read())
    val transactionalContext = TransactionalContextWrapper(createTransactionContext(graph, tx))
    val context = new TransactionBoundQueryContext(transactionalContext, new ResourceManager)(indexSearchMonitor)
    val initSize = context.resources.allResources.size

    // WHEN
    context.nodeOps.all

    // THEN
    context.resources.allResources should have size initSize + 1
    context.resources.close()
    tx.close()
  }

  test("should add cursor as resource when calling allPrimitive") {
    // GIVEN
    val tx = graph.beginTransaction(Type.explicit, AnonymousContext.read())
    val transactionalContext = TransactionalContextWrapper(createTransactionContext(graph, tx))
    val context = new TransactionBoundQueryContext(transactionalContext, new ResourceManager)(indexSearchMonitor)
    val initSize = context.resources.allResources.size

    // WHEN
    context.nodeOps.allPrimitive

    // THEN
    context.resources.allResources should have size initSize + 1
    context.resources.close()
    tx.close()
  }

  test("should remove cursor after closing resource") {
    // GIVEN
    val tx = graph.beginTransaction(Type.explicit, AnonymousContext.read())
    val transactionalContext = TransactionalContextWrapper(createTransactionContext(graph, tx))
    val context = new TransactionBoundQueryContext(transactionalContext, new ResourceManager)(indexSearchMonitor)
    val initSize = context.resources.allResources.size

    // WHEN
    context.nodeOps.all
    context.resources.allResources should have size initSize + 1
    context.resources.close()

    // THEN
    context.resources.allResources shouldBe empty
    tx.close()
  }

  private def startGraph(config:(Setting[_], Object)) = {
    val configs = Map[Setting[_], Object](config)
    managementService = new TestDatabaseManagementServiceBuilder().impermanent().setConfig(configs.asJava).build()
    graphOps = managementService.database(DEFAULT_DATABASE_NAME)
    graph = new GraphDatabaseCypherService(graphOps)
  }

  private def createTransactionContext(graphDatabaseQueryService: GraphDatabaseQueryService, transaction: InternalTransaction) = {
    val contextFactory = Neo4jTransactionalContextFactory.create(graphDatabaseQueryService)
    contextFactory.newContext(transaction, "no query", EMPTY_MAP)
  }

  private def createMiniGraph(relTypeName: String): Node = {
    val relType = RelationshipType.withName(relTypeName)
    val tx = graph.beginTransaction(Type.explicit, AnonymousContext.writeToken())
    try {
      val node = tx.createNode()
      val other1 = tx.createNode()
      val other2 = tx.createNode()

      node.createRelationshipTo(other1, relType)
      other2.createRelationshipTo(node, relType)
      tx.commit()
      node
    }
    finally {
      tx.close()
    }
  }
}
