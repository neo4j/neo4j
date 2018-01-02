/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.spi.v2_3

import java.net.URL

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v2_3.helpers.DynamicIterable
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.spi.v2_3.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.graphdb._
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.kernel.api._
import org.neo4j.kernel.impl.api.{KernelStatement, KernelTransactionImplementation}
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.test.ImpermanentGraphDatabase

import scala.collection.JavaConverters._

class TransactionBoundQueryContextTest extends CypherFunSuite {

  var graph: ImpermanentGraphDatabase = null
  var outerTx: Transaction = null
  var statement: Statement = null
  val indexSearchMonitor = mock[IndexSearchMonitor]

  override def beforeEach() {
    super.beforeEach()
    graph = new ImpermanentGraphDatabase
    outerTx = mock[Transaction]
    statement = new KernelStatement(mock[KernelTransactionImplementation], null, null, null, null, null, null)
  }

  override def afterEach() {
    graph.shutdown()
  }

  test ("should_mark_transaction_successful_if_successful") {
    // GIVEN
    when (outerTx.failure () ).thenThrow (new AssertionError ("Shouldn't be called") )
    val context = new TransactionBoundQueryContext(graph, outerTx, isTopLevelTx = true, statement)(indexSearchMonitor)

    // WHEN
    context.close(success = true)

    // THEN
    verify (outerTx).success ()
    verify (outerTx).close ()
    verifyNoMoreInteractions (outerTx)
  }

  test ("should_mark_transaction_failed_if_not_successful") {
    // GIVEN
    when (outerTx.success () ).thenThrow (new AssertionError ("Shouldn't be called") )
    val context = new TransactionBoundQueryContext(graph, outerTx, isTopLevelTx = true, statement)(indexSearchMonitor)

    // WHEN
    context.close(success = false)

    // THEN
    verify (outerTx).failure ()
    verify (outerTx).close ()
    verifyNoMoreInteractions (outerTx)
  }

  test ("should_return_fresh_but_equal_iterators") {
    // GIVEN
    val relTypeName = "LINK"
    val node = createMiniGraph(relTypeName)

    val tx = graph.beginTx()
    val stmt = graph.getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge]).get()
    val context = new TransactionBoundQueryContext(graph, tx, isTopLevelTx = true, stmt)(indexSearchMonitor)

    // WHEN
    val iterable = DynamicIterable( context.getRelationshipsForIds(node, SemanticDirection.BOTH, None) )

    // THEN
    val iteratorA: Iterator[Relationship] = iterable.iterator
    val iteratorB: Iterator[Relationship] = iterable.iterator
    iteratorA should not equal iteratorB
    iteratorA.toList should equal (iteratorB.toList)
    2 should equal (iterable.size)

    tx.success()
    tx.close()
  }

  test ("should deny non-whitelisted URL protocols for loading") {
    // GIVEN
    val tx = graph.beginTx()
    val stmt = graph.getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge]).get()
    val context = new TransactionBoundQueryContext(graph, tx, isTopLevelTx = true, stmt)(indexSearchMonitor)

    // THEN
    context.getImportURL(new URL("http://localhost:7474/data.csv")) should equal (Right(new URL("http://localhost:7474/data.csv")))
    context.getImportURL(new URL("file:///tmp/foo/data.csv")) should equal (Right(new URL("file:///tmp/foo/data.csv")))
    context.getImportURL(new URL("jar:file:/tmp/blah.jar!/tmp/foo/data.csv")) should equal (Left("loading resources via protocol 'jar' is not permitted"))

    tx.success()
    tx.finish()
  }

  test ("should deny file URLs when not allowed by config") {
    // GIVEN
    graph.shutdown()
    graph = new ImpermanentGraphDatabase(Map(GraphDatabaseSettings.allow_file_urls.name() -> "false").asJava)
    val tx = graph.beginTx()
    val stmt = graph.getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge]).get()
    val context = new TransactionBoundQueryContext(graph, tx, isTopLevelTx = true, stmt)(indexSearchMonitor)

    // THEN
    context.getImportURL(new URL("http://localhost:7474/data.csv")) should equal (Right(new URL("http://localhost:7474/data.csv")))
    context.getImportURL(new URL("file:///tmp/foo/data.csv")) should equal (Left("configuration property 'allow_file_urls' is false"))

    tx.success()
    tx.finish()
  }

  private def createMiniGraph(relTypeName: String): Node = {
    val relType: DynamicRelationshipType = DynamicRelationshipType.withName(relTypeName)
    val tx = graph.beginTx()
    try {
      val node = graph.createNode()
      val other1 = graph.createNode()
      val other2 = graph.createNode()

      node.createRelationshipTo( other1, relType )
      other2.createRelationshipTo( node, relType )
      tx.success()
      node
    }
    finally { tx.close() }
  }
}
