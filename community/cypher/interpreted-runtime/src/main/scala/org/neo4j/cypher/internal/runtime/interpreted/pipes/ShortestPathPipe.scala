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
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.interpreted.commands
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.DirectionConverter.toGraphDb
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.True
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.InternalException
import org.neo4j.exceptions.ShortestPathCommonEndNodesForbiddenException
import org.neo4j.gqlstatus.ErrorClassification
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.internal.kernel.api.helpers.traversal.BiDirectionalBFS
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
  needOnlyOnePath: Boolean
)(val id: Id = Id.INVALID_ID)
    extends PipeWithSource(source) {
  self =>

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    if (sameNodeMode == DisallowSameNode && sourceNodeName == targetNodeName) {
      val gql = ErrorGqlStatusObjectImplementation.from(
        GqlStatusInfoCodes.STATUS_51N23
      ).withClassification(ErrorClassification.CLIENT_ERROR)
        .build()
      throw new ShortestPathCommonEndNodesForbiddenException(gql)
    } else {

      val memoryTracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)

      val nodeCursor = state.query.nodeCursor()
      state.query.resources.trace(nodeCursor)
      val traversalCursor = state.query.traversalCursor()
      state.query.resources.trace(traversalCursor)

      // Create empty BiDirectionalBFS here and (re)set with source/target nodes and predicates for each row below.
      val biDirectionalBFS = BiDirectionalBFS.newEmptyBiDirectionalBFS(
        types.types(state.query),
        toGraphDb(direction),
        maxDepth.getOrElse(Int.MaxValue),
        returnOneShortestPathOnly,
        state.query.transactionalContext.dataRead,
        nodeCursor,
        traversalCursor,
        memoryTracker,
        needOnlyOnePath,
        allowZeroLength
      )
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

                    biDirectionalBFS.resetForNewRow(
                      sourceNode.id(),
                      targetNode.id(),
                      filteringStep.asNodeIdPredicate(row, state),
                      filteringStep.asRelCursorPredicate(row, state)
                    )

                    val shortestPaths = biDirectionalBFS.shortestPathIterator()

                    val outputRows = ClosingIterator.asClosingIterator(shortestPaths).map {
                      (path: VirtualPathValue) =>
                        val rels = VirtualValues.list(path.relationshipIds().map(VirtualValues.relationship): _*)
                        rowFactory.copyWith(row, pathName, path, relsName, rels)

                    }.filter {
                      r => pathPredicate.isTrue(r, state)
                    }

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

              case value =>
                throw new InternalException(
                  s"Expected to find a node at '($sourceNodeName, $targetNodeName)' but found $value instead"
                )
            }
          }
      }
      output.closing(traversalCursor).closing(nodeCursor).closing(biDirectionalBFS)
    }
  }
}
