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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.helpers.ListSupport
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.Id
import org.neo4j.cypher.internal.spi.v3_3.QueryContext
import org.neo4j.values.virtual.VirtualValues.reverse
import org.neo4j.values.virtual.{EdgeValue, ListValue, NodeValue}

case class ProjectEndpointsPipe(source: Pipe, relName: String,
                                start: String, startInScope: Boolean,
                                end: String, endInScope: Boolean,
                                relTypes: Option[LazyTypes], directed: Boolean, simpleLength: Boolean)
                               (val id: Id = new Id) extends PipeWithSource(source)
  with ListSupport  {
  type Projector = (ExecutionContext) => Iterator[ExecutionContext]

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) =
    input.flatMap(projector(state.query))

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
          context.newWith3(start, endNode, end, startNode, relName, reverse(rels))
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

  private def findSimpleLengthRelEndpoints(context: ExecutionContext, qtx: QueryContext): Option[(NodeValue, NodeValue)] = {
    val rel = Some(context(relName).asInstanceOf[EdgeValue]).filter(hasAllowedType)
    rel.flatMap { rel => pickStartAndEnd(rel, rel, context, qtx)}
  }

  private def findVarLengthRelEndpoints(context: ExecutionContext, qtx: QueryContext): Option[(NodeValue, NodeValue, ListValue)] = {
    val rels = makeTraversable(context(relName))
    if (rels.nonEmpty && allHasAllowedType(rels)) {
      pickStartAndEnd(rels.head.asInstanceOf[EdgeValue], rels.last.asInstanceOf[EdgeValue], context, qtx).map { case (s, e) => (s, e, rels) }
    } else {
      None
    }
  }

  private def allHasAllowedType(rels: ListValue): Boolean = {
    val iterator = rels.iterator()
    while(iterator.hasNext) {
      val next = iterator.next().asInstanceOf[EdgeValue]
      if (!hasAllowedType(next)) return false
    }
    true
  }

  private def hasAllowedType(rel: EdgeValue): Boolean =
    relTypes.forall(_.names.contains(rel.`type`().stringValue()))

  private def pickStartAndEnd(relStart: EdgeValue, relEnd: EdgeValue,
                              context: ExecutionContext, qtx: QueryContext): Option[(NodeValue, NodeValue)] = {
    val startNode = relStart.startNode()
    val endNode = relEnd.endNode()
    Some((startNode, endNode)).filter {
      case (s, e) =>
        (!startInScope || context(start) == s) && (!endInScope || context(end) == e)
    }
  }
}
