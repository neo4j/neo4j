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
package org.neo4j.cypher.internal.ir.ordering

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.Asc
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.Desc

/**
 * A candidate for how the rows in a query can be sorted, defined by OrderCandidate.order.
 * A candidate can be required (i.e. the rows need to sorted like this for correctness)
 * or interesting (i.e. sorting the rows like this will allow for performance optimizations).
 *
 * There can be multiple candidates since different parts of a query can leverage different orders for their respective optimizations.
 */
trait OrderCandidate[T <: OrderCandidate[T]] {
  def order: Seq[ColumnOrder]

  def nonEmpty: Boolean = !isEmpty

  def isEmpty: Boolean = order.isEmpty

  def headOption: Option[ColumnOrder] = order.headOption

  def renameColumns(f: Seq[ColumnOrder] => Seq[ColumnOrder]): T

  def asc(expression: Expression, projections: Map[LogicalVariable, Expression] = Map.empty): T

  def desc(expression: Expression, projections: Map[LogicalVariable, Expression] = Map.empty): T

  def asProvidedOrder(providedOrderFactory: ProvidedOrderFactory): ProvidedOrder =
    providedOrderFactory.providedOrder(order, ProvidedOrder.Self)
}

case class RequiredOrderCandidate(order: Seq[ColumnOrder]) extends OrderCandidate[RequiredOrderCandidate] {
  def asInteresting: InterestingOrderCandidate = InterestingOrderCandidate(order)

  override def renameColumns(f: Seq[ColumnOrder] => Seq[ColumnOrder]): RequiredOrderCandidate =
    RequiredOrderCandidate(f(order))

  override def asc(
    expression: Expression,
    projections: Map[LogicalVariable, Expression] = Map.empty
  ): RequiredOrderCandidate =
    RequiredOrderCandidate(order :+ Asc(expression, projections))

  override def desc(
    expression: Expression,
    projections: Map[LogicalVariable, Expression] = Map.empty
  ): RequiredOrderCandidate =
    RequiredOrderCandidate(order :+ Desc(expression, projections))
}

object RequiredOrderCandidate extends OrderCandidateFactory[RequiredOrderCandidate] {

  def asc(expression: Expression, projections: Map[LogicalVariable, Expression] = Map.empty): RequiredOrderCandidate =
    empty.asc(expression, projections)

  def empty: RequiredOrderCandidate = RequiredOrderCandidate(Seq.empty)

  def desc(expression: Expression, projections: Map[LogicalVariable, Expression] = Map.empty): RequiredOrderCandidate =
    empty.desc(expression, projections)
}

case class InterestingOrderCandidate(order: Seq[ColumnOrder]) extends OrderCandidate[InterestingOrderCandidate] {

  override def renameColumns(f: Seq[ColumnOrder] => Seq[ColumnOrder]): InterestingOrderCandidate =
    InterestingOrderCandidate(f(order))

  override def asc(
    expression: Expression,
    projections: Map[LogicalVariable, Expression] = Map.empty
  ): InterestingOrderCandidate = InterestingOrderCandidate(order :+ Asc(expression, projections))

  override def desc(
    expression: Expression,
    projections: Map[LogicalVariable, Expression] = Map.empty
  ): InterestingOrderCandidate = InterestingOrderCandidate(order :+ Desc(expression, projections))
}

object InterestingOrderCandidate extends OrderCandidateFactory[InterestingOrderCandidate] {

  def asc(
    expression: Expression,
    projections: Map[LogicalVariable, Expression] = Map.empty
  ): InterestingOrderCandidate =
    empty.asc(expression, projections)

  def desc(
    expression: Expression,
    projections: Map[LogicalVariable, Expression] = Map.empty
  ): InterestingOrderCandidate =
    empty.desc(expression, projections)

  def empty: InterestingOrderCandidate = InterestingOrderCandidate(Seq.empty)
}

trait OrderCandidateFactory[T <: OrderCandidate[T]] {
  def asc(expression: Expression, projections: Map[LogicalVariable, Expression] = Map.empty): T

  def desc(expression: Expression, projections: Map[LogicalVariable, Expression] = Map.empty): T

  def empty: OrderCandidate[T]
}
