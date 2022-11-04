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

import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.interpreted.commands
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.InternalException
import org.neo4j.exceptions.ShortestPathCommonEndNodesForbiddenException
import org.neo4j.internal.kernel.api.helpers.BiDirectionalBFS
import org.neo4j.values.virtual.PathReference
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualPathValue
import org.neo4j.values.virtual.VirtualValues

import java.util.function.Predicate

case class ShortestPathPipe(
  source: Pipe,
  sourceNodeName: String,
  targetNodeName: String,
  pathName: String,
  relsNameOpt: Option[String],
  types: RelationshipTypes,
  direction: SemanticDirection,
  filteringStep: VarLengthPredicate,
  pathPredicates: Seq[commands.predicates.Predicate],
  returnOneShortestPathOnly: Boolean,
  disallowSameNode: Boolean,
  allowZeroLength: Boolean,
  maxDepth: Option[Int]
)(val id: Id = Id.INVALID_ID)
    extends PipeWithSource(source) {
  self =>

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    if (disallowSameNode && sourceNodeName == targetNodeName) {
      throw new ShortestPathCommonEndNodesForbiddenException
    } else {

      val memoryTracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)

      val nodeCursor = state.query.nodeCursor()
      state.query.resources.trace(nodeCursor)
      val traversalCursor = state.query.traversalCursor()
      state.query.resources.trace(traversalCursor)

      // Create empty BiDirectionalBFS here and (re)set with source/target nodes and predicates for each row below.
      val biDirectionalBFS = BiDirectionalBFS.newEmptyBiDirectionalBFS(
        types.types(state.query),
        direction,
        maxDepth.getOrElse(Int.MaxValue),
        returnOneShortestPathOnly,
        state.query.transactionalContext.dataRead,
        nodeCursor,
        traversalCursor,
        memoryTracker
      )

      val output = input.flatMap {
        row =>
          {
            (row.getByName(sourceNodeName), row.getByName(targetNodeName)) match {

              case (sourceNode: VirtualNodeValue, targetNode: VirtualNodeValue) =>
                val pathPredicateWithContext = new Predicate[PathReference] {
                  def test(path: PathReference): Boolean = {
                    if (pathPredicates.nonEmpty) { // Don't set values in row if not needed
                      row.set(pathName, path)
                      relsNameOpt match {
                        case Some(relsName) => row.set(relsName, path.relationshipsAsList())
                        case _              =>
                      }
                      pathPredicates.forall(pred => pred.isTrue(row, state))
                    } else {
                      true
                    }
                  }
                }

                if (
                  filteringStep.filterNode(row, state)(sourceNode) && filteringStep.filterNode(row, state)(targetNode)
                ) {
                  val pathsIterator =
                    if (sourceNode == targetNode) {
                      if (!allowZeroLength && disallowSameNode) {
                        throw new ShortestPathCommonEndNodesForbiddenException
                      } else if (allowZeroLength) {
                        val path = VirtualValues.pathReference(Array[Long](sourceNode.id()), Array.empty[Long])
                        if (pathPredicateWithContext.test(path)) {
                          ClosingIterator.single(
                            path
                          )
                        } else {
                          ClosingIterator.empty
                        }
                      } else {
                        ClosingIterator.empty
                      }
                    } else {

                      val (nodePredicate, relationshipPredicate) =
                        VarLengthPredicate.createPredicates(filteringStep, state, row)

                      biDirectionalBFS.resetForNewRow(
                        sourceNode.id(),
                        targetNode.id(),
                        nodePredicate,
                        relationshipPredicate
                      )

                      val shortestPaths = biDirectionalBFS.shortestPathIterator(pathPredicateWithContext)
                      if (returnOneShortestPathOnly) {
                        if (shortestPaths.hasNext) {
                          ClosingIterator.single(shortestPaths.next())
                        } else {
                          ClosingIterator.empty
                        }
                      } else {
                        ClosingIterator.asClosingIterator(shortestPaths)
                      }
                    }

                  relsNameOpt match {
                    case Some(relsName) =>
                      pathsIterator.map {
                        case path: VirtualPathValue =>
                          val rels = VirtualValues.list(path.relationshipIds().map(VirtualValues.relationship): _*)
                          rowFactory.copyWith(row, pathName, path, relsName, rels)

                        case value =>
                          throw new InternalException(s"Expected path, got '$value'")
                      }
                    case None =>
                      pathsIterator.map {
                        case path: VirtualPathValue =>
                          rowFactory.copyWith(row, pathName, path)

                        case value =>
                          throw new InternalException(s"Expected path, got '$value'")
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
      output.closing(nodeCursor).closing(traversalCursor).closing(biDirectionalBFS)
    }
  }
}
