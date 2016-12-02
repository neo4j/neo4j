/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.pipes

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.compiler.v3_2.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.Id
import org.neo4j.cypher.internal.compiler.v3_2.spi.QueryContext
import org.neo4j.cypher.internal.compiler.v3_2.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v3_2.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_2.symbols.{CypherType, _}
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherTestSupport
import org.neo4j.graphdb.{Node, Relationship}
import org.scalatest.mock.MockitoSugar

trait PipeTestSupport extends CypherTestSupport with MockitoSugar {

  implicit val newMonitor = mock[PipeMonitor]
  val query = mock[QueryContext]

  def pipeWithResults(f: QueryState => Iterator[ExecutionContext]): Pipe = new Pipe {
    protected def internalCreateResults(state: QueryState) = f(state)
    def exists(pred: (Pipe) => Boolean) = ???
    def planDescription = ???
    def symbols: SymbolTable = ???
    def monitor: PipeMonitor = newMonitor
    def dup(sources: List[Pipe]): Pipe = ???
    def sources: Seq[Pipe] = ???

    // Used by profiling to identify where to report dbhits and rows
    override def id: Id = new Id
  }

  def row(values: (String, Any)*) = ExecutionContext.from(values: _*)

  def setUpRelMockingInQueryContext(rels: Relationship*) {
    val relsByStartNode = rels.groupBy(_.getStartNode)
    val relsByEndNode = rels.groupBy(_.getEndNode)
    val relsByNode = (relsByStartNode.keySet ++ relsByEndNode.keySet).map {
      n => n -> (relsByStartNode.getOrElse(n, Seq.empty) ++ relsByEndNode.getOrElse(n, Seq.empty))
    }.toMap

    setUpRelLookupMocking(SemanticDirection.OUTGOING, relsByStartNode)
    setUpRelLookupMocking(SemanticDirection.INCOMING, relsByEndNode)
    setUpRelLookupMocking(SemanticDirection.BOTH, relsByNode)
  }

  def setUpRelLookupMocking(direction: SemanticDirection, relsByNode: Map[Node, Seq[Relationship]]) {
    relsByNode.foreach {
      case (node, rels) =>
        when(query.getRelationshipsForIds(node, direction, None)).thenAnswer(
          new Answer[Iterator[Relationship]] {
            def answer(invocation: InvocationOnMock) = rels.iterator
          })

        when(query.nodeGetDegree(node.getId, direction)).thenReturn(rels.size)
    }
  }

  def newMockedNode(id: Int) = {
    val node = mock[Node]
    when(node.getId).thenReturn(id)
    node
  }

  def newMockedRelationship(id: Int, startNode: Node, endNode: Node): Relationship = {
    val relationship = mock[Relationship]
    when(relationship.getId).thenReturn(id)
    when(relationship.getStartNode).thenReturn(startNode)
    when(relationship.getEndNode).thenReturn(endNode)
    when(relationship.getOtherNode(startNode)).thenReturn(endNode)
    when(relationship.getOtherNode(endNode)).thenReturn(startNode)
    relationship
  }

  def newMockedPipe(node: String, rows: ExecutionContext*): Pipe = {
    newMockedPipe(Map(node -> CTNode), rows: _*)
  }

  def newMockedPipe(symbols: Map[String, CypherType], rows: ExecutionContext*): Pipe = {
    val pipe = mock[Pipe]

    when(pipe.sources).thenReturn(Seq.empty)
    when(pipe.symbols).thenReturn(SymbolTable(symbols))
    when(pipe.createResults(any())).thenAnswer(new Answer[Iterator[ExecutionContext]] {
      def answer(invocation: InvocationOnMock) = rows.iterator
    })

    pipe
  }
}
