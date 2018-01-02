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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{ReadsAllNodes, Effects}
import org.neo4j.cypher.internal.compiler.v2_3.helpers.CollectionSupport
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments.KeyNames
import org.neo4j.cypher.internal.compiler.v2_3.spi.QueryContext
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.graphdb.{Node, Relationship}

case class ProjectEndpointsPipe(source: Pipe, relName: String,
                                start: String, startInScope: Boolean,
                                end: String, endInScope: Boolean,
                                relTypes: Option[LazyTypes], directed: Boolean, simpleLength: Boolean)
                               (val estimatedCardinality: Option[Double] = None)(implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(source, pipeMonitor)
  with CollectionSupport
  with RonjaPipe {
  val symbols: SymbolTable =
    source.symbols.add(start, CTNode).add(end, CTNode)

  override val localEffects = if (!startInScope || !endInScope) Effects(ReadsAllNodes) else Effects()

  type Projector = (ExecutionContext) => Iterator[ExecutionContext]

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) =
    input.flatMap(projector(state.query))

  def planDescriptionWithoutCardinality =
    source.planDescription
          .andThen(this.id, "ProjectEndpoints", identifiers, KeyNames(Seq(relName, start, end)))

  def dup(sources: List[Pipe]): Pipe = {
    val (source :: Nil) = sources
    copy(source = source)(estimatedCardinality)
  }

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))

  private def projector(qtx: QueryContext): Projector =
    if (simpleLength) project(qtx) else projectVarLength(qtx)

  private def projectVarLength(qtx: QueryContext): Projector = (context: ExecutionContext) => {
    findVarLengthRelEndpoints(context, qtx) match {
      case Some((startNode, endNode, _)) if directed =>
        Iterator(
          context.newWith2(start, startNode, end, endNode)
        )
      case Some((startNode, endNode, rels)) if !directed =>
        Iterator(
          context.newWith2(start, startNode, end, endNode),
          context.newWith3(start, endNode, end, startNode, relName, rels.reverse)
        )
      case None =>
        Iterator.empty
    }
  }

  private def project(qtx: QueryContext): Projector = (context: ExecutionContext) => {
    findSimpleLengthRelEndpoints(context, qtx) match {
      case Some((startNode, endNode)) if directed =>
        Iterator(context.newWith2(start, startNode, end, endNode))
      case Some((startNode, endNode)) if !directed =>
        Iterator(
          context.newWith2(start, startNode, end, endNode),
          context.newWith2(start, endNode, end, startNode)
        )
      case None =>
        Iterator.empty
    }
  }

  private def findSimpleLengthRelEndpoints(context: ExecutionContext, qtx: QueryContext): Option[(Node, Node)] = {
    val rel = Some(context(relName).asInstanceOf[Relationship]).filter(hasAllowedType)
    rel.flatMap { rel => pickStartAndEnd(rel, rel, context, qtx)}
  }

  private def findVarLengthRelEndpoints(context: ExecutionContext, qtx: QueryContext): Option[(Node, Node, Seq[Relationship])] = {
    val rels = makeTraversable(context(relName)).toSeq.asInstanceOf[Seq[Relationship]]
    if (rels.nonEmpty && rels.forall(hasAllowedType)) {
      pickStartAndEnd(rels.head, rels.last, context, qtx).map { case (s, e) => (s, e, rels) }
    } else {
      None
    }
  }

  private def hasAllowedType(rel: Relationship): Boolean =
    relTypes.map(_.names.contains(rel.getType.name())).getOrElse(true)

  private def pickStartAndEnd(relStart: Relationship, relEnd: Relationship,
                              context: ExecutionContext, qtx: QueryContext): Option[(Node, Node)] = {
    val startNode = qtx.relationshipStartNode(relStart)
    val endNode = qtx.relationshipEndNode(relEnd)
    Some((startNode, endNode)).filter {
      case (s, e) =>
        (!startInScope || context(start) == s) && (!endInScope || context(end) == e)
    }
  }
}
