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
package org.neo4j.cypher.internal.spi.v3_3

import java.net.URL
import java.util.Collections

import org.hamcrest.Matchers.greaterThan
import org.junit.Assert.assertThat
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.helpers.DynamicIterable
import org.neo4j.cypher.internal.frontend.v3_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.spi.v3_3.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService
import org.neo4j.graphdb._
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier
import org.neo4j.kernel.api._
import org.neo4j.kernel.api.security.SecurityContext.AUTH_DISABLED
import org.neo4j.kernel.api.security.{AnonymousContext, SecurityContext}
import org.neo4j.kernel.impl.api.{KernelStatement, KernelTransactionImplementation, StatementOperationParts}
import org.neo4j.kernel.impl.coreapi.{InternalTransaction, PropertyContainerLocker}
import org.neo4j.kernel.impl.factory.CanWrite
import org.neo4j.kernel.impl.locking.LockTracer
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo
import org.neo4j.kernel.impl.query.{Neo4jTransactionalContext, Neo4jTransactionalContextFactory}
import org.neo4j.storageengine.api.StorageStatement
import org.neo4j.test.TestGraphDatabaseFactory

import scala.collection.JavaConverters._

class TransactionBoundQueryContextTest extends CypherFunSuite {

  var graph: GraphDatabaseCypherService = null
  var outerTx: InternalTransaction = null
  var statement: KernelStatement = null
  val indexSearchMonitor = mock[IndexSearchMonitor]
  val locker = mock[PropertyContainerLocker]

  override def beforeEach() {
    super.beforeEach()
    graph = new GraphDatabaseCypherService(new TestGraphDatabaseFactory().newImpermanentDatabase())
    outerTx = mock[InternalTransaction]
    val kernelTransaction = mock[KernelTransactionImplementation]
    when(kernelTransaction.securityContext()).thenReturn(AUTH_DISABLED)
    val storeStatement = mock[StorageStatement]
    val operations = mock[StatementOperationParts](RETURNS_DEEP_STUBS)
    statement = new KernelStatement(kernelTransaction, null, storeStatement, new Procedures(), new CanWrite(), LockTracer.NONE)
    statement.initialize(null, operations, PageCursorTracerSupplier.NULL.get())
    statement.acquire()
  }

  override def afterEach() {
    graph.getGraphDatabaseService.shutdown()
  }

  test("should mark transaction successful if successful") {
    // GIVEN
    when(outerTx.failure()).thenThrow(new AssertionError("Shouldn't be called"))
    when(outerTx.transactionType()).thenReturn(KernelTransaction.Type.`implicit`)
    when(outerTx.securityContext()).thenReturn(AUTH_DISABLED)
    val tc = new Neo4jTransactionalContext(graph, null, null, null, locker, outerTx, statement, null)
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
    when(outerTx.transactionType()).thenReturn(KernelTransaction.Type.`implicit`)
    when(outerTx.securityContext()).thenReturn(AUTH_DISABLED)
    val tc = new Neo4jTransactionalContext(graph, null, null, null, locker, outerTx, statement, null)
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

    val tx = graph.beginTransaction(KernelTransaction.Type.explicit, AnonymousContext.read())
    val transactionalContext = TransactionalContextWrapper(createTransactionContext(graph, tx))
    val context = new TransactionBoundQueryContext(transactionalContext)(indexSearchMonitor)

    // WHEN
    val iterable = DynamicIterable(context.getRelationshipsForIds(node.getId, SemanticDirection.BOTH, None))

    // THEN
    val iteratorA: Iterator[Relationship] = iterable.iterator
    val iteratorB: Iterator[Relationship] = iterable.iterator
    iteratorA should not equal iteratorB
    iteratorA.toList should equal(iteratorB.toList)
    2 should equal(iterable.size)

    tx.success()
    tx.close()
  }

  test("should deny non-whitelisted URL protocols for loading") {
    // GIVEN
    val tx = graph.beginTransaction(KernelTransaction.Type.explicit, AnonymousContext.read())
    val transactionalContext = TransactionalContextWrapper(createTransactionContext(graph, tx))
    val context = new TransactionBoundQueryContext(transactionalContext)(indexSearchMonitor)

    // THEN
    context.getImportURL(new URL("http://localhost:7474/data.csv")) should equal(Right(new URL("http://localhost:7474/data.csv")))
    context.getImportURL(new URL("file:///tmp/foo/data.csv")) should equal(Right(new URL("file:///tmp/foo/data.csv")))
    context.getImportURL(new URL("jar:file:/tmp/blah.jar!/tmp/foo/data.csv")) should equal(Left("loading resources via protocol 'jar' is not permitted"))

    tx.success()
    tx.close()
  }

  test("should deny file URLs when not allowed by config") {
    // GIVEN
    graph.getGraphDatabaseService.shutdown()
    val config = Map[Setting[_], String](GraphDatabaseSettings.allow_file_urls -> "false")
    graph = new GraphDatabaseCypherService(new TestGraphDatabaseFactory().newImpermanentDatabase(config.asJava))
    val tx = graph.beginTransaction(KernelTransaction.Type.explicit, AnonymousContext.read())
    val transactionalContext = TransactionalContextWrapper(createTransactionContext(graph, tx))
    val context = new TransactionBoundQueryContext(transactionalContext)(indexSearchMonitor)

    // THEN
    context.getImportURL(new URL("http://localhost:7474/data.csv")) should equal (Right(new URL("http://localhost:7474/data.csv")))
    context.getImportURL(new URL("file:///tmp/foo/data.csv")) should equal (Left("configuration property 'dbms.security.allow_csv_import_from_file_urls' is false"))

    tx.success()
    tx.close()
  }

  test("provide access to kernel statement page cache tracer") {
    val creator = graph.beginTransaction(KernelTransaction.Type.explicit, SecurityContext.AUTH_DISABLED)
    graph.createNode()
    graph.createNode()
    graph.createNode()
    creator.success()
    creator.close()

    val tx = graph.beginTransaction(KernelTransaction.Type.explicit, SecurityContext.AUTH_DISABLED)
    val transactionalContext = TransactionalContextWrapper(createTransactionContext(graph, tx))
    val context = new TransactionBoundQueryContext(transactionalContext)(indexSearchMonitor)

    val tracer = transactionalContext.kernelStatisticProvider
    tracer.getPageCacheHits should equal(0)

    graph.getNodeById(2)
    graph.getNodeById(1)
    val accesses = tracer.getPageCacheHits + tracer.getPageCacheMisses
    assertThat(Long.box(accesses), greaterThan(Long.box(1L)))

    tx.close()
  }

  private def createTransactionContext(graphDatabaseCypherService: GraphDatabaseCypherService, transaction: InternalTransaction) = {
    val contextFactory = Neo4jTransactionalContextFactory.create(graphDatabaseCypherService, new PropertyContainerLocker)
    contextFactory.newContext(ClientConnectionInfo.EMBEDDED_CONNECTION, transaction, "no query", Collections.emptyMap())
  }

  private def createMiniGraph(relTypeName: String): Node = {
    val relType = RelationshipType.withName(relTypeName)
    val tx = graph.beginTransaction(KernelTransaction.Type.explicit, AnonymousContext.writeToken())
    try {
      val node = graph.createNode()
      val other1 = graph.createNode()
      val other2 = graph.createNode()

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
