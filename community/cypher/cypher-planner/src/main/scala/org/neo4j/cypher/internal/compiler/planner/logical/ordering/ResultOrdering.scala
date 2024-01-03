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
package org.neo4j.cypher.internal.compiler.planner.logical.ordering

import org.neo4j.cypher.internal.compiler.helpers.AggregationHelper
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.Asc
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.Desc
import org.neo4j.cypher.internal.ir.ordering.DefaultProvidedOrderFactory
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.ir.ordering.NoProvidedOrder
import org.neo4j.cypher.internal.ir.ordering.OrderCandidate
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrderFactory
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderDescending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability.NONE
import org.neo4j.exceptions.InternalException

import scala.annotation.tailrec

/**
 * This object provides some utility methods around InterestingOrder and ProvidedOrder.
 */
object ResultOrdering {

  case class PropertyAndPredicateType(prop: Property, isSingleExactPredicate: Boolean)

  /**
   * @param interestingOrder     the InterestingOrder from the query
   * @param indexProperties      a sequence of the properties (inclusive variable name) of a (composite) index.
   *                             The sequence is length one for non-composite indexes.
   * @param indexOrderCapability the index order capability it provides.
   * @return the order that the index guarantees, if possible in accordance with the given required order.
   */
  def providedOrderForIndexOperator(
    interestingOrder: InterestingOrder,
    indexProperties: Seq[PropertyAndPredicateType],
    indexOrderCapability: IndexOrderCapability,
    providedOrderFactory: ProvidedOrderFactory = DefaultProvidedOrderFactory
  ): (ProvidedOrder, IndexOrder) = {
    def satisfies(
      indexProperty: Property,
      expression: Expression,
      projections: Map[LogicalVariable, Expression]
    ): Boolean =
      AggregationHelper.extractPropertyForValue(expression, projections).contains(indexProperty)

    def getNewProvidedOrderColumn(orderColumn: ColumnOrder, prop: Property): ColumnOrder = orderColumn match {
      case _: Asc  => Asc(prop)
      case _: Desc => Desc(prop)
    }

    if (indexProperties.isEmpty || interestingOrder == InterestingOrder.empty) {
      (ProvidedOrder.empty, IndexOrderNone)
    } else {
      val candidates = interestingOrder.requiredOrderCandidate +: interestingOrder.interestingOrderCandidates

      // Accumulator for the foldLeft
      sealed trait Acc
      final case class OrderNotYetDecided(providedOrderColumns: Seq[ColumnOrder]) extends Acc
      final case class IndexOrderDecided(indexOrder: IndexOrder, providedOrderColumns: Seq[ColumnOrder]) extends Acc
      final case object IndexNotHelpful extends Acc

      def possibleOrdersForCandidate(candidate: OrderCandidate[_]): Acc =
        candidate.order.zipAll(indexProperties, null, null).foldLeft[Acc](OrderNotYetDecided(Seq.empty)) {

          // We decided to use IndexOrderDescending and find another DESC column in the ORDER BY
          case (
              IndexOrderDecided(IndexOrderDescending, poColumns),
              (Desc(expression, projection), PropertyAndPredicateType(prop, _))
            ) if satisfies(prop, expression, projection) && indexOrderCapability == IndexOrderCapability.BOTH =>
            IndexOrderDecided(IndexOrderDescending, poColumns :+ Desc(prop))

          // We have not yet decided on the index order and find a DESC column in the ORDER BY
          case (
              OrderNotYetDecided(providedOrderColumns),
              (Desc(expression, projection), PropertyAndPredicateType(prop, isSingleExactPredicate))
            ) if satisfies(prop, expression, projection) && indexOrderCapability == IndexOrderCapability.BOTH =>
            // If we have an exact predicate here, we do not want to make a decision on the index order yet.
            if (isSingleExactPredicate) {
              OrderNotYetDecided(providedOrderColumns :+ Desc(prop))
            } else {
              IndexOrderDecided(IndexOrderDescending, providedOrderColumns :+ Desc(prop))
            }

          // We decided to use IndexOrderAscending and find another ASC column in the ORDER BY
          case (
              IndexOrderDecided(IndexOrderAscending, poColumns),
              (Asc(expression, projection), PropertyAndPredicateType(prop, _))
            ) if satisfies(prop, expression, projection) && indexOrderCapability == IndexOrderCapability.BOTH =>
            IndexOrderDecided(IndexOrderAscending, poColumns :+ Asc(prop))

          // We have not yet decided on the index order and find an ASC column in the ORDER BY
          case (
              OrderNotYetDecided(providedOrderColumns),
              (Asc(expression, projection), PropertyAndPredicateType(prop, isSingleExactPredicate))
            ) if satisfies(prop, expression, projection) && indexOrderCapability == IndexOrderCapability.BOTH =>
            // If we have an exact predicate here, we do not want to make a decision on the index order yet.
            if (isSingleExactPredicate) {
              OrderNotYetDecided(providedOrderColumns :+ Asc(prop))
            } else {
              IndexOrderDecided(IndexOrderAscending, providedOrderColumns :+ Asc(prop))
            }

          // We find a contradicting order with single exact predicate
          case (IndexOrderDecided(indexOrder, poColumns), (orderColumn, PropertyAndPredicateType(prop, true)))
            if orderColumn != null && satisfies(prop, orderColumn.expression, orderColumn.projections) =>
            IndexOrderDecided(indexOrder, poColumns :+ getNewProvidedOrderColumn(orderColumn, prop))

          // Either
          // * we find a contradicting order without exact predicate,
          // * the index has more columns than the ORDER BY or
          // * the property doesn't match.
          // So we have to add more columns in the same order to the provided order.
          case (IndexOrderDecided(indexOrder, poColumns), (_, PropertyAndPredicateType(prop, _))) =>
            val nextCol = indexOrder match {
              case IndexOrderAscending  => Asc(prop)
              case IndexOrderDescending => Desc(prop)
              case IndexOrderNone => throw new InternalException(
                  s"Expected IndexOrderAscending or IndexOrderDescending but was IndexOrderNone"
                )
            }
            IndexOrderDecided(indexOrder, poColumns :+ nextCol)

          // Index capability and required order don't agree, with single exact predicate
          case (OrderNotYetDecided(providedOrderColumns), (orderColumn, PropertyAndPredicateType(prop, true)))
            if orderColumn != null && satisfies(
              prop,
              orderColumn.expression,
              orderColumn.projections
            ) && indexOrderCapability != NONE =>
            OrderNotYetDecided(providedOrderColumns :+ getNewProvidedOrderColumn(orderColumn, prop))

          // When we have already some provided order columns, but haven't yet decided on the order,
          // that means all previous columns were single exact properties.
          // We can fall into this case when, in addition to the above condition, we either
          // * have more index columns than order candidate columns or
          // * the column has a non-matching property or
          // * the column has a non-matching order
          // In all these cases we make a decision on the order(because that helps the previous single exact property columns).
          case (OrderNotYetDecided(providedOrderColumns), (_, PropertyAndPredicateType(prop, _)))
            if indexOrderCapability != NONE && providedOrderColumns.nonEmpty =>
            IndexOrderDecided(IndexOrderAscending, providedOrderColumns :+ Asc(prop))

          // Either there is no order candidate column or the first column either
          // * has a non-matching property or
          // * non-matching order on a non-(single exact) property or
          // In these cases we can't really use the index to help with this order candidate
          case (OrderNotYetDecided(Seq()), (_, PropertyAndPredicateType(_, _))) if indexOrderCapability != NONE =>
            IndexNotHelpful

          case (x, _) =>
            // Anything else does not influence the index order or the provided order columns
            x
        }

      def toResult(providedOrder: ProvidedOrder, indexOrder: IndexOrder) = providedOrder match {
        case NoProvidedOrder => (ProvidedOrder.empty, IndexOrderNone)
        case providedOrder   => (providedOrder, indexOrder)
      }
      val orderAccPerCandidate: Seq[Acc] = candidates.filter(_.nonEmpty).map(possibleOrdersForCandidate)
      val maybeResult = orderAccPerCandidate.collectFirst {
        case IndexOrderDecided(indexOrder, columns) if columns.nonEmpty =>
          toResult(providedOrderFactory.providedOrder(columns, ProvidedOrder.Self), indexOrder)
        case OrderNotYetDecided(columns) if columns.nonEmpty =>
          val indexOrder = indexOrderCapability match {
            case IndexOrderCapability.NONE => IndexOrderNone
            case IndexOrderCapability.BOTH => IndexOrderAscending
          }
          toResult(providedOrderFactory.providedOrder(columns, ProvidedOrder.Self), indexOrder)
      }

      // If the required order cannot be satisfied, return empty
      maybeResult.getOrElse((ProvidedOrder.empty, IndexOrderNone))
    }
  }

  def providedOrderForLabelScan(
    interestingOrder: InterestingOrder,
    variable: Variable,
    indexOrderCapability: IndexOrderCapability,
    providedOrderFactory: ProvidedOrderFactory = DefaultProvidedOrderFactory
  ): ProvidedOrder =
    providedOrderForScan(interestingOrder, variable, indexOrderCapability, providedOrderFactory)

  def providedOrderForRelationshipTypeScan(
    interestingOrder: InterestingOrder,
    variable: LogicalVariable,
    indexOrderCapability: IndexOrderCapability,
    providedOrderFactory: ProvidedOrderFactory = DefaultProvidedOrderFactory
  ): ProvidedOrder = {
    providedOrderForScan(
      interestingOrder,
      variable,
      indexOrderCapability,
      providedOrderFactory
    )
  }

  private def providedOrderForScan(
    interestingOrder: InterestingOrder,
    variable: LogicalVariable,
    indexOrderCapability: IndexOrderCapability,
    providedOrderFactory: ProvidedOrderFactory
  ): ProvidedOrder = {
    def satisfies(expression: Expression, projections: Map[LogicalVariable, Expression]): Boolean =
      extractVariableForValue(expression, projections).contains(variable)

    indexOrderCapability match {
      case IndexOrderCapability.NONE => ProvidedOrder.empty
      case IndexOrderCapability.BOTH =>
        val candidates = interestingOrder.requiredOrderCandidate +: interestingOrder.interestingOrderCandidates

        candidates.map(_.headOption).collectFirst {
          case Some(Desc(expression, projection)) if satisfies(expression, projection) =>
            providedOrderFactory.desc(variable)
          case Some(Asc(expression, projection)) if satisfies(expression, projection) =>
            providedOrderFactory.asc(variable)
        }.getOrElse(ProvidedOrder.empty)
    }
  }

  @tailrec
  private[ordering] def extractVariableForValue(
    expression: Expression,
    renamings: Map[LogicalVariable, Expression]
  ): Option[Variable] = {
    expression match {
      case variable: Variable =>
        if (renamings.contains(variable) && renamings(variable) != variable)
          extractVariableForValue(renamings(variable), renamings)
        else
          Some(variable)
      case _ => None
    }
  }
}
