/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_3.helpers.DynamicIterable
import org.neo4j.graphdb._
import org.mockito.Mockito._
import org.neo4j.kernel.api._
import org.neo4j.kernel.impl.api.{StatementOperationParts, KernelTransactionImplementation, KernelStatement}
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.test.ImpermanentGraphDatabase

class TransactionBoundQueryContextTest extends CypherFunSuite {

  var graph: ImpermanentGraphDatabase = null
  var outerTx: Transaction = null
  var statement: Statement = null

  override def beforeEach() {
    super.beforeEach ()
    graph = new ImpermanentGraphDatabase
    outerTx = mock[Transaction]
    statement = new KernelStatement(mock[KernelTransactionImplementation], null, null, null, null, null)
  }

  test ("should_mark_transaction_successful_if_successful") {
    // GIVEN
    when (outerTx.failure () ).thenThrow (new AssertionError ("Shouldn't be called") )
    val context = new TransactionBoundQueryContext(graph, outerTx, isTopLevelTx = true, statement)

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
    val context = new TransactionBoundQueryContext(graph, outerTx, isTopLevelTx = true, statement)

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
    val context = new TransactionBoundQueryContext(graph, tx, isTopLevelTx = true, stmt)

    // WHEN
    val iterable = DynamicIterable( context.getRelationshipsForIds(node, Direction.BOTH, None) )

    // THEN
    val iteratorA: Iterator[Relationship] = iterable.iterator
    val iteratorB: Iterator[Relationship] = iterable.iterator
    iteratorA should not equal iteratorB
    iteratorA.toList should equal (iteratorB.toList)
    2 should equal (iterable.size)

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
    finally { tx.finish() }
  }
}
