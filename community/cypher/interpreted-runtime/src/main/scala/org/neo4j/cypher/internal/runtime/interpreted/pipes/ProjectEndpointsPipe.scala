/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ListSupport
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue
import org.neo4j.values.virtual.VirtualValues

case class ProjectEndpointsPipe(source: Pipe, relName: String,
                                start: String, startInScope: Boolean,
                                end: String, endInScope: Boolean,
                                relTypes: RelationshipTypes,
                                directed: Boolean,
                                simpleLength: Boolean)
                               (val id: Id = Id.INVALID_ID) extends PipeWithSource(source)
  with ListSupport  {
  type Projector = CypherRow => Iterator[CypherRow]

  protected def internalCreateResults(input: ClosingIterator[CypherRow], state: QueryState): ClosingIterator[CypherRow] =
    input.flatMap(projector(state))

  private def projector(state: QueryState): Projector =
    if (simpleLength) project(state) else projectVarLength(state)

  private def projectVarLength(state: QueryState): Projector = (context: CypherRow) => {
    findVarLengthRelEndpoints(context, state) match {
      case Some((InScopeReversed(startNode, endNode), rels)) if !directed =>
        context.set(start, endNode, end, startNode, relName, rels.reverse())
        Iterator(context)
      case Some((NotInScope(startNode, endNode), rels)) if !directed =>
        Iterator(
          rowFactory.copyWith(context, start, startNode, end, endNode),
          rowFactory.copyWith(context, start, endNode, end, startNode, relName, rels.reverse())
        )
      case Some((startAndEnd, rels)) =>
        context.set(start, startAndEnd.start, end, startAndEnd.end)
        Iterator(context)
      case None =>
        Iterator.empty
    }
  }

  private def project(state: QueryState): Projector = (context: CypherRow) => {
    findSimpleLengthRelEndpoints(context, state) match {
      case Some(InScopeReversed(startNode, endNode)) if !directed =>
        context.set(start, endNode, end, startNode)
        Iterator(context)
      case Some(NotInScope(startNode, endNode)) if !directed =>
        Iterator(
          rowFactory.copyWith(context, start, startNode, end, endNode),
          rowFactory.copyWith(context, start, endNode, end, startNode)
        )
      case Some(startAndEnd) =>
        context.set(start, startAndEnd.start, end, startAndEnd.end)
        Iterator(context)
      case None =>
        Iterator.empty
    }
  }

  private def findSimpleLengthRelEndpoints(context: CypherRow,
                                           state: QueryState
                                          ): Option[StartAndEnd] = {

    val relValue = context.getByName(relName) match {
      case relValue: VirtualRelationshipValue => relValue
      case _ => return None
    }
    val qtx = state.query
    val internalCursor = state.cursors.relationshipScanCursor
    state.query.singleRelationship(relValue.id(), internalCursor)
    if (internalCursor.next()) {
      if (!isAllowedType(internalCursor.`type`(), qtx)) {
        None
      } else {
        val start = qtx.nodeById(internalCursor.sourceNodeReference())
        val end = qtx.nodeById(internalCursor.targetNodeReference())
        pickStartAndEnd(start, end, context)
      }
    } else None
  }

  private def findVarLengthRelEndpoints(context: CypherRow,
                                        state: QueryState
                                       ): Option[(StartAndEnd, ListValue)] = {
    val rels = makeTraversable(context.getByName(relName))
    val qtx = state.query
    if (rels.nonEmpty && allHasAllowedType(rels, state)) {
      val internalCursor = state.cursors.relationshipScanCursor
      val firstRel = rels.head match {
        case relValue: VirtualRelationshipValue => relValue
        case _ => throw new CypherTypeException(s"${rels.head()} is not a relationship")
      }
      state.query.singleRelationship(firstRel.id(), internalCursor)
      if (internalCursor.next()) {
        val start = VirtualValues.node(internalCursor.sourceNodeReference())
        val lastRel = rels.last match {
          case relValue: VirtualRelationshipValue => relValue
          case _ => throw new CypherTypeException(s"${rels.last()} is not a relationship")
        }
        state.query.singleRelationship(lastRel.id(), internalCursor)
        if (internalCursor.next()) {
          val end = VirtualValues.node(internalCursor.targetNodeReference())
          return pickStartAndEnd(start, end, context).map(startAndEnd => (startAndEnd, rels))
        }
      }
      None
    } else {
      None
    }
  }

  private def allHasAllowedType(rels: ListValue, state: QueryState): Boolean = {
    val iterator = rels.iterator()
    val qtx = state.query
    while(iterator.hasNext) {
      val next: VirtualRelationshipValue = iterator.next() match {
        case relValue: VirtualRelationshipValue => relValue
        case _ =>  return false
      }
      val internalCursor = state.cursors.relationshipScanCursor
      state.query.singleRelationship(next.id(), internalCursor)
      if (internalCursor.next() && (!isAllowedType(internalCursor.`type`(), qtx)))  {
        return false
      }
    }
    true
  }

  private def isAllowedType(rel: Int, qtx: QueryContext): Boolean = {
    val types = relTypes.types(qtx)
    types == null || types.contains(rel)
  }

  private def pickStartAndEnd(startNode: VirtualNodeValue, endNode: VirtualNodeValue,
                              context: CypherRow): Option[StartAndEnd] = {
    if (!startInScope && !endInScope) Some(NotInScope(startNode, endNode))
    else if ((!startInScope || context.getByName(start) == startNode) && (!endInScope || context.getByName(end) == endNode))
      Some(InScope(startNode, endNode))
    else if (!directed && (!startInScope || context.getByName(start) == endNode ) && (!endInScope || context.getByName(end) == startNode))
      Some(InScopeReversed(startNode, endNode))
    else None
  }

  sealed trait StartAndEnd {
    def start: VirtualNodeValue
    def end: VirtualNodeValue
  }
  case class NotInScope(start: VirtualNodeValue, end: VirtualNodeValue) extends StartAndEnd
  case class InScope(start: VirtualNodeValue, end: VirtualNodeValue) extends StartAndEnd
  case class InScopeReversed(start: VirtualNodeValue, end: VirtualNodeValue) extends StartAndEnd
}
