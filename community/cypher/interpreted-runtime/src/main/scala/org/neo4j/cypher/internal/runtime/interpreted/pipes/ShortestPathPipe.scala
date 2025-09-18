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
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths.DisallowSameNode
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths.SameNodeMode
import org.neo4j.cypher.internal.logical.plans.TraversalPathMode
import org.neo4j.cypher.internal.logical.plans.TraversalPathMode.Walk
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.interpreted.commands
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.DirectionConverter.toGraphDb
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.True
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.operations.CypherTypeValueMapper
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.exceptions.ShortestPathCommonEndNodesForbiddenException.shortestPathCommonEndNodes
import org.neo4j.internal.kernel.api.helpers.traversal.ShortestPathBFSFactory
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualPathValue
import org.neo4j.values.virtual.VirtualValues

case class ShortestPathPipe(
  source: Pipe,
  sourceNodeName: String,
  targetNodeName: String,
  pathName: String,
  relsName: String,
  types: RelationshipTypes,
  direction: SemanticDirection,
  filteringStep: TraversalPredicates,
  pathPredicates: Seq[commands.predicates.Predicate],
  returnOneShortestPathOnly: Boolean,
  sameNodeMode: SameNodeMode,
  allowZeroLength: Boolean,
  maxDepth: Option[Int],
  needOnlyOnePath: Boolean,
  traversalMode: TraversalPathMode
)(val id: Id = Id.INVALID_ID)
    extends PipeWithSource(source) {
  self =>

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    if (sameNodeMode == DisallowSameNode && sourceNodeName == targetNodeName) {
      throw shortestPathCommonEndNodes()
    } else {

      val memoryTracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)

      val nodeCursor = state.query.nodeCursor()
      state.query.resources.trace(nodeCursor)
      val traversalCursor = state.query.traversalCursor()
      state.query.resources.trace(traversalCursor)

      val pathPredicate = pathPredicates.foldLeft(True(): commands.predicates.Predicate)(_.andWith(_))
      val output = input.flatMap {
        row =>
          {
            (row.getByName(sourceNodeName), row.getByName(targetNodeName)) match {

              case (sourceNode: VirtualNodeValue, targetNode: VirtualNodeValue) =>
                if (
                  filteringStep.filterNode(row, state, sourceNode) && filteringStep.filterNode(row, state, targetNode)
                ) {
                  if (sameNodeMode.shouldReturnEmptyResult(sourceNode.id(), targetNode.id(), allowZeroLength)) {
                    ClosingIterator.empty
                  } else {
                    val bfs = ShortestPathBFSFactory.create(
                      sourceNode.id(),
                      targetNode.id(),
                      types.types(state.query),
                      toGraphDb(direction),
                      maxDepth.getOrElse(Int.MaxValue),
                      state.query.transactionalContext.dataRead,
                      nodeCursor,
                      traversalCursor,
                      memoryTracker,
                      filteringStep.asNodeIdPredicate(row, state),
                      filteringStep.asRelCursorPredicate(row, state),
                      returnOneShortestPathOnly,
                      allowZeroLength,
                      needOnlyOnePath,
                      traversalMode == Walk,
                      null
                    )

                    val shortestPaths = bfs.shortestPathIterator()

                    val outputRows = ClosingIterator.asClosingIterator(shortestPaths).map {
                      (path: VirtualPathValue) =>
                        val rels = VirtualValues.list(path.relationshipIds().map(VirtualValues.relationship): _*)
                        rowFactory.copyWith(row, pathName, path, relsName, rels)

                    }.filter {
                      r => pathPredicate.isTrue(r, state)
                    }.closing(bfs)

                    if (returnOneShortestPathOnly) {
                      if (outputRows.hasNext) {
                        ClosingIterator.single(outputRows.next())
                      } else {
                        ClosingIterator.empty
                      }
                    } else {
                      outputRows
                    }
                  }
                } else {
                  ClosingIterator.empty
                }

              case (IsNoValue(), _) | (_, IsNoValue()) => ClosingIterator.empty

              case (value, _: VirtualNodeValue) =>
                throw CypherTypeException.expectedNodeButGot(
                  value.prettyPrint(),
                  value.getTypeName,
                  CypherTypeValueMapper.valueType(value)
                )

              case (_, value) =>
                throw CypherTypeException.expectedNodeButGot(
                  value.prettyPrint(),
                  value.getTypeName,
                  CypherTypeValueMapper.valueType(value)
                )
            }
          }
      }
      output.closing(traversalCursor).closing(nodeCursor)
    }
  }
}
