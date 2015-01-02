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
package org.neo4j.cypher.internal.compiler.v2_2.pipes

import org.neo4j.cypher.internal.compiler.v2_2.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.InternalPlanDescription.Arguments.KeyNames
import org.neo4j.cypher.internal.compiler.v2_2.spi.QueryContext
import org.neo4j.cypher.internal.compiler.v2_2.symbols._
import org.neo4j.cypher.internal.helpers.CollectionSupport
import org.neo4j.graphdb.{Node, Relationship}

case class ProjectEndpointsPipe(source: Pipe, relName: String, start: String, end: String, directed: Boolean = true, simpleLength: Boolean = true)
                               (val estimatedCardinality: Option[Long] = None)(implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(source, pipeMonitor)
  with CollectionSupport
  with RonjaPipe {
  val symbols: SymbolTable =
    source.symbols.add(start, CTNode).add(end, CTNode)

  type Projector = (ExecutionContext) => Iterator[ExecutionContext]

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) =
    input.flatMap(projector(state.query))

  override def planDescription =
    source.planDescription
          .andThen(this, "ProjectEndpoints", identifiers, KeyNames(Seq(relName, start, end)))

  def dup(sources: List[Pipe]): Pipe = {
    val (source :: Nil) = sources
    copy(source = source)(estimatedCardinality)
  }

  def withEstimatedCardinality(estimated: Long) = copy()(Some(estimated))

  private def projector(qtx: QueryContext): Projector =
    if (directed)
      if (simpleLength) projectDirected(qtx) else projectDirectedVarLength(qtx)
    else
      if (simpleLength) projectUndirected(qtx) else projectUndirectedVarLength(qtx)

  private def projectDirectedVarLength(qtx: QueryContext): Projector = (context: ExecutionContext) => {
    val rels = findVarLengthRels(context, qtx)
    if (rels.isEmpty)
      Iterator.empty
    else {
      val (startNode, endNode) = findVarLengthRelEndpoints(rels, qtx)
      Iterator(
        context.newWith2(start, startNode, end, endNode)
      )
    }
  }

  private def projectUndirectedVarLength(qtx: QueryContext): Projector = (context: ExecutionContext) => {
    val rels = findVarLengthRels(context, qtx)
    if (rels.isEmpty)
      Iterator.empty
    else {
      val (startNode, endNode) = findVarLengthRelEndpoints(rels, qtx)
      Iterator(
        context.newWith2(start, startNode, end, endNode),
        context.newWith3(start, endNode, end, startNode, relName, rels.reverse)
      )
    }
  }

  private def projectDirected(qtx: QueryContext): Projector = (context: ExecutionContext) => {
    val (startNode, endNode) = findSimpleLengthRelEndpoints(context, qtx)
    Iterator(
      context.newWith2(start, startNode, end, endNode)
    )
  }

  private def projectUndirected(qtx: QueryContext): Projector = (context: ExecutionContext) => {
    val (startNode, endNode) = findSimpleLengthRelEndpoints(context, qtx)
    Iterator(
      context.newWith2(start, startNode, end, endNode),
      context.newWith2(start, endNode, end, startNode)
    )
  }

  private def findSimpleLengthRelEndpoints(context: ExecutionContext, qtx: QueryContext): (Node, Node) = {
    val rel = context(relName).asInstanceOf[Relationship]
    val startNode = qtx.relationshipStartNode(rel)
    val endNode = qtx.relationshipEndNode(rel)
    (startNode, endNode)
  }

  private def findVarLengthRels(context: ExecutionContext, qtx: QueryContext): Seq[Relationship] =
    makeTraversable(context(relName)).toSeq.asInstanceOf[Seq[Relationship]]

  private def findVarLengthRelEndpoints(rels: Seq[Relationship], qtx: QueryContext): (Node, Node) = {
    val startNode = qtx.relationshipStartNode(rels.head)
    val endNode = qtx.relationshipEndNode(rels.last)
    (startNode, endNode)
  }
}
