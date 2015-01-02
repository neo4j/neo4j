/**
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
package org.neo4j.cypher.internal.spi.v2_0

import org.neo4j.cypher.internal.helpers.DynamicIterable
import org.neo4j.graphdb._
import org.neo4j.test.ImpermanentGraphDatabase
import org.junit.{Before, Test}
import org.scalatest.Assertions
import org.mockito.Mockito
import org.scalatest.junit.JUnitSuite
import org.scalatest.mock.MockitoSugar
import org.neo4j.kernel.api._
import org.neo4j.kernel.impl.api.{KernelTransactionImplementation, KernelStatement}

class TransactionBoundQueryContextTest extends JUnitSuite with Assertions with MockitoSugar {

  var graph: ImpermanentGraphDatabase = null
  var outerTx: Transaction = null
  var statement: Statement = null

  @Before
  def init() {
    graph = new ImpermanentGraphDatabase
    outerTx = mock[Transaction]
    statement = new KernelStatement(mock[KernelTransactionImplementation], null, null, null, null, null, null, null)
  }

  @Test def should_mark_transaction_successful_if_successful() {
    // GIVEN
    Mockito.when(outerTx.failure()).thenThrow( new AssertionError( "Shouldn't be called" ) )
    val context = new TransactionBoundQueryContext(graph, outerTx, statement)

    // WHEN
    context.close(success = true)

    // THEN
    Mockito.verify(outerTx).success()
    Mockito.verify(outerTx).close()
    Mockito.verifyNoMoreInteractions(outerTx)
  }

  @Test def should_mark_transaction_failed_if_not_successful() {
    // GIVEN
    Mockito.when(outerTx.success()).thenThrow( new AssertionError( "Shouldn't be called" ) )
    val context = new TransactionBoundQueryContext(graph, outerTx, statement)

    // WHEN
    context.close(success = false)

    // THEN
    Mockito.verify(outerTx).failure()
    Mockito.verify(outerTx).close()
    Mockito.verifyNoMoreInteractions(outerTx)
  }

  @Test def should_return_fresh_but_equal_iterators() {
    // GIVEN
    val relTypeName = "LINK"
    val node = createMiniGraph(relTypeName)

    val tx = graph.beginTx()
    val context = new TransactionBoundQueryContext(graph, tx, statement)

    // WHEN
    val iterable = DynamicIterable( context.getRelationshipsFor(node, Direction.BOTH, Seq.empty) )

    // THEN
    val iteratorA: Iterator[Relationship] = iterable.iterator
    val iteratorB: Iterator[Relationship] = iterable.iterator
    assert( iteratorA != iteratorB )
    assert( iteratorA.toList === iteratorB.toList )
    assert( 2 === iterable.size )

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
