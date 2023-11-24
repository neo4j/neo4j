/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ClosingLongIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.PrimitiveLongHelper
import org.neo4j.cypher.internal.runtime.RelationshipIterator
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.RelationshipCursorIterator
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.DirectionConverter.toGraphDb
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ExpandIntoPipe.getRowNode
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ExpandIntoPipe.relationshipSelectionCursorIterator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ExpandIntoPipe.traceRelationshipSelectionCursor
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.internal.kernel.api.helpers.CachingExpandInto
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualValues

/**
 * Expand when both end-points are known, find all relationships of the given
 * type in the given direction between the two end-points.
 *
 * This is done by checking both nodes and starts from any non-dense node of the two.
 * If both nodes are dense, we find the degree of each and expand from the smaller of the two
 *
 * This pipe also caches relationship information between nodes for the duration of the query
 */
case class ExpandIntoPipe(
  source: Pipe,
  fromName: String,
  relName: String,
  toName: String,
  dir: SemanticDirection,
  lazyTypes: RelationshipTypes
)(val id: Id = Id.INVALID_ID)
    extends PipeWithSource(source) {
  self =>

  private val kernelDirection = toGraphDb(dir)

  protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    val query = state.query

    val expandInto = new CachingExpandInto(
      query.transactionalContext.kernelQueryContext,
      kernelDirection,
      state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)
    )
    state.query.resources.trace(expandInto)

    input.flatMap {
      row =>
        val fromNode = getRowNode(row, fromName)
        fromNode match {
          case fromNode: VirtualNodeValue =>
            val toNode = getRowNode(row, toName)
            toNode match {
              case IsNoValue() => ClosingIterator.empty
              case n: VirtualNodeValue =>
                val traversalCursor = query.traversalCursor()
                val nodeCursor = query.nodeCursor()
                try {
                  val selectionCursor = expandInto.connectingRelationships(
                    nodeCursor,
                    traversalCursor,
                    fromNode.id(),
                    lazyTypes.types(query),
                    n.id()
                  )
                  traceRelationshipSelectionCursor(query.resources, selectionCursor, traversalCursor)
                  val relationships = relationshipSelectionCursorIterator(selectionCursor, traversalCursor)
                  if (!relationships.hasNext) ClosingIterator.empty
                  else PrimitiveLongHelper.map(
                    relationships,
                    r =>
                      rowFactory.copyWith(
                        row,
                        relName,
                        VirtualValues.relationship(
                          r,
                          relationships.startNodeId(),
                          relationships.endNodeId(),
                          relationships.typeId()
                        )
                      )
                  )
                } finally {
                  nodeCursor.close()
                }
              case value =>
                throw new ParameterWrongTypeException(
                  s"Expected to find a node at '$fromName' but found $value instead"
                )
            }

          case IsNoValue() => ClosingIterator.empty
        }
    }.closing(expandInto)
  }
}

object ExpandIntoPipe {

  def traceRelationshipSelectionCursor(
    resources: ResourceManager,
    selectionCursor: RelationshipTraversalCursor,
    traversalCursor: RelationshipTraversalCursor
  ): Unit = {
    resources.trace(selectionCursor)
    // In case the originating node cursor supports fast relationships these two could be the same object, so we need to do this check
    if (!(traversalCursor eq selectionCursor)) {
      resources.trace(traversalCursor)
    }
  }

  def relationshipSelectionCursorIterator(
    cursor: RelationshipTraversalCursor,
    traversalCursor: RelationshipTraversalCursor
  ): ClosingLongIterator with RelationshipIterator =
    new RelationshipCursorIterator(cursor, traversalCursor)

  @inline
  def getRowNode(row: CypherRow, col: String): AnyValue = {
    row.getByName(col) match {
      case n: VirtualNodeValue => n
      case IsNoValue()         => NO_VALUE
      case value => throw new ParameterWrongTypeException(s"Expected to find a node at '$col' but found $value instead")
    }
  }
}
