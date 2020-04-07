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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherTestSupport
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.kernel.impl.util.ValueUtils
import org.scalatest.mockito.MockitoSugar

trait PipeTestSupport extends CypherTestSupport with MockitoSugar {

  val query = mock[QueryContext]

  def pipeWithResults(f: QueryState => Iterator[CypherRow]): Pipe = new Pipe {
    protected def internalCreateResults(state: QueryState) = f(state)

    // Used by profiling to identify where to report dbhits and rows
    override def id: Id = Id.INVALID_ID
  }

  def row(values: (String, Any)*) = CypherRow.from(values.map(v => (v._1, ValueUtils.of(v._2))): _*)

  def newMockedNode(id: Int) = {
    val node = mock[Node]
    when(node.getId).thenReturn(id)
    node
  }

  def newMockedRelationship(id: Int, startNode: Node, endNode: Node): Relationship = {
    val relationship = mock[Relationship]
    val startId = startNode.getId
    val endId = endNode.getId
    when(relationship.getId).thenReturn(id)
    when(relationship.getStartNode).thenReturn(startNode)
    when(relationship.getStartNodeId).thenReturn(startId)
    when(relationship.getEndNode).thenReturn(endNode)
    when(relationship.getEndNodeId).thenReturn(endId)
    when(relationship.getOtherNode(startNode)).thenReturn(endNode)
    when(relationship.getOtherNode(endNode)).thenReturn(startNode)
    when(relationship.toString).thenReturn(s"($startId)-[$id]->($endId)")
    relationship
  }

  def newMockedPipe(node: String, rows: CypherRow*): Pipe = {
    newMockedPipe(Map(node -> CTNode), rows: _*)
  }

  def newMockedPipe(symbols: Map[String, CypherType], rows: CypherRow*): Pipe = {
    val pipe = mock[Pipe]
    when(pipe.createResults(any())).thenAnswer(new Answer[Iterator[CypherRow]] {
      def answer(invocation: InvocationOnMock) = rows.iterator
    })

    pipe
  }
}
