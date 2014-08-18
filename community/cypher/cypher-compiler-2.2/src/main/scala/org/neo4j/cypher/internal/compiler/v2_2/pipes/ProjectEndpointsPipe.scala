/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.pipes

import org.neo4j.cypher.internal.compiler.v2_2.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.PlanDescription.Arguments.KeyNames
import org.neo4j.cypher.internal.compiler.v2_2.spi.QueryContext
import org.neo4j.cypher.internal.compiler.v2_2.symbols._
import org.neo4j.graphdb.{Node, Relationship}

case class ProjectEndpointsPipe(source: Pipe, relName: String, start: String, end: String, directed: Boolean = true, varLength: Boolean = false)
                               (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(source, pipeMonitor) {
  val symbols: SymbolTable =
    source.symbols.add(start, CTNode).add(end, CTNode)

  type Projector = (ExecutionContext, QueryContext) => Iterator[ExecutionContext]

  private val projectUndirectedVarLength: Projector = (context: ExecutionContext, qtx: QueryContext) => {
    val (rels, startNode, endNode) = findVarLengthRelEndpoints(context, qtx)
    Iterator(
      context += (start -> startNode) += (end -> endNode),
      context.clone() += (start -> endNode) += (end -> startNode) += (relName -> rels.reverse)
    )
  }

  private val projectDirectedVarLength: Projector = (context: ExecutionContext, qtx: QueryContext) => {
    val (rels, startNode, endNode) = findVarLengthRelEndpoints(context, qtx)
    val context2 = context.clone()
    Iterator(
      context += (start -> startNode) += (end -> endNode)
    )
  }

  private val projectUndirected: Projector = (context: ExecutionContext, qtx: QueryContext) => {
    val (startNode, endNode) = findSimpleLengthRelEndpoints(context, qtx)
    Iterator(
      context.clone() += (start -> startNode) += (end -> endNode),
      context += (start -> endNode) += (end -> startNode)
    )
  }

  private val projectDirected: Projector = (context: ExecutionContext, qtx: QueryContext) => {
    val (startNode, endNode) = findSimpleLengthRelEndpoints(context, qtx)
    Iterator(
      context += (start -> startNode) += (end -> endNode)
    )
  }

  private val projector: Projector =
    (varLength, directed) match {
      case (true, true) => projectDirectedVarLength
      case (true, false) => projectUndirectedVarLength
      case (false, true) => projectDirected
      case (false, false) => projectUndirected
    }

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = {
    val qtx = state.query
    input.flatMap(projector(_, qtx))
  }

  override def planDescription =
    source.planDescription
      .andThen(this, "ProjectEndpoints", KeyNames(Seq(relName, start, end)))

  def dup(sources: List[Pipe]): Pipe = {
    val (source :: Nil) = sources
    copy(source = source)
  }

  private def findSimpleLengthRelEndpoints(context: ExecutionContext, qtx: QueryContext): (Node, Node) = {
    val rel = context(relName).asInstanceOf[Relationship]
    val startNode = qtx.relationshipStartNode(rel)
    val endNode = qtx.relationshipEndNode(rel)
    (startNode, endNode)
  }

  private def findVarLengthRelEndpoints(context: ExecutionContext, qtx: QueryContext): (Seq[Relationship], Node, Node) = {
    val rels = context(relName).asInstanceOf[Seq[Relationship]]
    val startNode = qtx.relationshipStartNode(rels.head)
    val endNode = qtx.relationshipEndNode(rels.last)
    (rels, startNode, endNode)
  }
}
