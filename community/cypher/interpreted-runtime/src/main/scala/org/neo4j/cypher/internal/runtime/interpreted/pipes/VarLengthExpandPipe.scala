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

import org.eclipse.collections.impl.block.factory.primitive.LongPredicates
import org.neo4j.collection.trackable.HeapTrackingCollections
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.RelationshipContainer
import org.neo4j.cypher.internal.runtime.interpreted.pipes.VarLengthExpandPipe.projectBackwards
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.InternalException
import org.neo4j.function.Predicates
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue
import org.neo4j.values.virtual.VirtualValues
import org.neo4j.values.virtual.VirtualValues.relationship

import java.util.function.LongPredicate
import java.util.function.Predicate

trait VarLengthPredicate {
  def filterNode(row: CypherRow, state: QueryState)(node: VirtualNodeValue): Boolean
  def filterRelationship(row: CypherRow, state: QueryState)(rel: VirtualRelationshipValue): Boolean
}

object VarLengthPredicate {

  val NONE: VarLengthPredicate = new VarLengthPredicate {
    override def filterNode(row: CypherRow, state: QueryState)(node: VirtualNodeValue): Boolean = true
    override def filterRelationship(row: CypherRow, state: QueryState)(rel: VirtualRelationshipValue): Boolean = true
  }

  def createPredicates(
    filteringStep: VarLengthPredicate,
    state: QueryState,
    row: CypherRow
  ): (LongPredicate, Predicate[RelationshipTraversalCursor]) = {

    def toLongPredicate(f: Long => Boolean): LongPredicate = (value: Long) => f(value)

    filteringStep match {
      case VarLengthPredicate.NONE =>
        (LongPredicates.alwaysTrue(), Predicates.alwaysTrue[RelationshipTraversalCursor]())
      case _ =>
        val nodePredicate = toLongPredicate(t => filteringStep.filterNode(row, state)(VirtualValues.node(t)))
        val relationshipPredicate = new Predicate[RelationshipTraversalCursor] {
          override def test(t: RelationshipTraversalCursor): Boolean = {
            filteringStep.filterRelationship(row, state)(relationship(
              t.relationshipReference(),
              t.originNodeReference(),
              t.targetNodeReference(),
              t.`type`()
            ))
          }
        }

        (nodePredicate, relationshipPredicate)
    }
  }
}

case class VarLengthExpandPipe(
  source: Pipe,
  fromName: String,
  relName: String,
  toName: String,
  dir: SemanticDirection,
  projectedDir: SemanticDirection,
  types: RelationshipTypes,
  min: Int,
  max: Option[Int],
  nodeInScope: Boolean,
  filteringStep: VarLengthPredicate = VarLengthPredicate.NONE
)(val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {

  private def varLengthExpand(
    node: VirtualNodeValue,
    state: QueryState,
    maxDepth: Option[Int],
    row: CypherRow
  ): ClosingIterator[(VirtualNodeValue, RelationshipContainer)] = {
    val memoryTracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)
    val stack = HeapTrackingCollections.newArrayDeque[(VirtualNodeValue, RelationshipContainer)](
      memoryTracker
    )
    stack.push((node, RelationshipContainer.empty(memoryTracker)))

    new ClosingIterator[(VirtualNodeValue, RelationshipContainer)] {
      def next(): (VirtualNodeValue, RelationshipContainer) = {
        val (node, rels) = stack.pop()
        if (rels.size < maxDepth.getOrElse(Int.MaxValue) && filteringStep.filterNode(row, state)(node)) {
          val relationships = state.query.getRelationshipsForIds(node.id(), dir, types.types(state.query))

          // relationships get immediately exhausted. Therefore we do not need a ClosingIterator here.
          while (relationships.hasNext) {
            val rel = VirtualValues.relationship(
              relationships.next(),
              relationships.startNodeId(),
              relationships.endNodeId(),
              relationships.typeId()
            )
            if (filteringStep.filterRelationship(row, state)(rel)) {
              val otherNode = VirtualValues.node(relationships.otherNodeId(node.id()))
              if (!rels.contains(rel) && filteringStep.filterNode(row, state)(otherNode)) {
                stack.push((otherNode, rels.append(rel)))
              }
            }
          }
        }
        val projectedRels = {
          if (projectBackwards(dir, projectedDir)) {
            rels.reverse
          } else {
            rels
          }
        }
        rels.close()
        (node, projectedRels)
      }

      def innerHasNext: Boolean = !stack.isEmpty

      override protected[this] def closeMore(): Unit = stack.close()
    }
  }

  protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    def expand(row: CypherRow, n: VirtualNodeValue): ClosingIterator[CypherRow] = {
      if (filteringStep.filterNode(row, state)(n)) {
        val paths = varLengthExpand(n, state, max, row)
        paths.collect {
          case (node, rels) if rels.size >= min && isToNodeValid(row, node) =>
            rowFactory.copyWith(row, relName, rels.asList, toName, node)
        }
      } else {
        ClosingIterator.empty
      }
    }

    input.flatMap {
      row =>
        {
          row.getByName(fromName) match {
            case node: VirtualNodeValue =>
              expand(row, node)

            case IsNoValue() => ClosingIterator.empty
            case value =>
              throw new InternalException(s"Expected to find a node at '$fromName' but found $value instead")
          }
        }
    }
  }

  private def isToNodeValid(row: CypherRow, node: VirtualNodeValue) =
    !nodeInScope || {
      row.getByName(toName) match {
        case toNode: VirtualNodeValue =>
          toNode.id == node.id
        case _ =>
          false
      }
    }
}

object VarLengthExpandPipe {

  def projectBackwards(dir: SemanticDirection, projectedDir: SemanticDirection): Boolean =
    if (dir == SemanticDirection.BOTH) {
      projectedDir == SemanticDirection.INCOMING
    } else {
      dir != projectedDir
    }
}
