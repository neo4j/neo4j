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

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.ast.ExpressionVariable
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint.AllocatedTraversalEndpoint
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint.Endpoint
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression => CompiledExpression}
import org.neo4j.function.Predicates
import org.neo4j.internal.kernel.api.RelationshipTraversalEntities
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue
import org.neo4j.values.virtual.VirtualValues.node
import org.neo4j.values.virtual.VirtualValues.relationship

import java.util.function.LongPredicate
import java.util.function.Predicate

trait TraversalPredicates extends TraversalPredicates.NodePredicate with TraversalPredicates.RelTraversalPredicate {
  def isEmpty: Boolean
  def nonEmpty: Boolean = !isEmpty
}

object TraversalPredicates {

  trait NodePredicate {
    def filterNode(row: ReadableRow, state: QueryState, node: VirtualNodeValue): Boolean

    def asNodeIdPredicate(row: ReadableRow, state: QueryState): LongPredicate =
      id => filterNode(row, state, node(id))
  }

  trait RelTraversalPredicate {

    def filterRelationship(
      row: ReadableRow,
      state: QueryState,
      rel: VirtualRelationshipValue,
      expandedFrom: VirtualNodeValue,
      expandedTo: VirtualNodeValue
    ): Boolean

    def asRelCursorPredicate(row: ReadableRow, state: QueryState): Predicate[RelationshipTraversalEntities] =
      cursor =>
        filterRelationship(
          row,
          state,
          relationship(
            cursor.relationshipReference(),
            cursor.sourceNodeReference(),
            cursor.targetNodeReference(),
            cursor.`type`()
          ),
          node(cursor.originNodeReference()),
          node(cursor.otherNodeReference())
        )
  }

  object NONE extends TraversalPredicates {
    override def filterNode(row: ReadableRow, state: QueryState, node: VirtualNodeValue): Boolean = true

    override def asNodeIdPredicate(
      row: ReadableRow,
      state: QueryState
    ): LongPredicate = Predicates.ALWAYS_TRUE_LONG

    override def filterRelationship(
      row: ReadableRow,
      state: QueryState,
      rel: VirtualRelationshipValue,
      expandedFrom: VirtualNodeValue,
      expandedTo: VirtualNodeValue
    ): Boolean = true

    override def asRelCursorPredicate(
      row: ReadableRow,
      state: QueryState
    ): Predicate[RelationshipTraversalEntities] =
      Predicates.alwaysTrue()

    override def isEmpty: Boolean = true
  }

  def create(
    nodePredicates: Seq[VariablePredicate],
    relationshipPredicates: Seq[VariablePredicate],
    convert: Expression => CompiledExpression
  ): TraversalPredicates = {
    if (nodePredicates.isEmpty && relationshipPredicates.isEmpty) {
      TraversalPredicates.NONE
    } else {
      val expansionNodes = relationshipPredicates.flatMap(x => TraversalEndpoint.extract(x.predicate))

      val compiledNodePredicates: Seq[NodePredicate] =
        nodePredicates.map { case VariablePredicate(variable, predicate) =>
          val command = convert(predicate)
          val ev = ExpressionVariable.cast(variable)

          (context: ReadableRow, state: QueryState, entity: VirtualNodeValue) => {
            state.expressionVariables(ev.offset) = entity
            command(context, state) eq Values.TRUE
          }
        }

      val compiledRelPredicates: Seq[RelTraversalPredicate] =
        relationshipPredicates.map { case VariablePredicate(variable, predicate) =>
          val command = convert(predicate)
          val ev = ExpressionVariable.cast(variable)

          (
            context: ReadableRow,
            state: QueryState,
            entity: VirtualRelationshipValue,
            expandedFrom: VirtualNodeValue,
            expandingTo: VirtualNodeValue
          ) => {
            state.expressionVariables(ev.offset) = entity

            expansionNodes.foreach { case AllocatedTraversalEndpoint(offset, end) =>
              state.expressionVariables(offset) = end match {
                case Endpoint.From => expandedFrom
                case Endpoint.To   => expandingTo
              }
            }

            command(context, state) eq Values.TRUE
          }
        }

      new TraversalPredicates {
        def filterRelationship(
          row: ReadableRow,
          state: QueryState,
          rel: VirtualRelationshipValue,
          expandedFrom: VirtualNodeValue,
          expandedTo: VirtualNodeValue
        ): Boolean =
          compiledRelPredicates.forall(_.filterRelationship(row, state, rel, expandedFrom, expandedTo))

        def filterNode(row: ReadableRow, state: QueryState, node: VirtualNodeValue): Boolean =
          compiledNodePredicates.forall(_.filterNode(row, state, node))

        override def isEmpty: Boolean = false
      }
    }
  }
}
