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
package org.neo4j.cypher.internal.compiler.planner.logical.ordering

import org.neo4j.cypher.internal.compiler.helpers.AggregationHelper
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.Asc
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.Desc
import org.neo4j.cypher.internal.ir.ordering.DefaultProvidedOrderFactory
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.ir.ordering.OrderCandidate
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrderFactory
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderDescending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability.NONE
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CypherType

import scala.annotation.tailrec

/**
 * This object provides some utility methods around InterestingOrder and ProvidedOrder.
 */
object ResultOrdering {

  case class PropertyAndPredicateType(prop: Property, isSingleExactPredicate: Boolean)

  /**
   * @param interestingOrder the InterestingOrder from the query
   * @param indexProperties  a sequence of the properties (inclusive variable name) of a (composite) index.
   *                         The sequence is length one for non-composite indexes.
   * @param orderTypes       a sequence of the type that the index query compares against for that each property.
   *                         So for `WHERE n.prop = 1 AND n.foo > 'bla'` this will be Seq( CTInt, CTString )
   * @param capabilityLookup a lambda function to ask the index for a (sub)-sequence of types for the order capability it provides.
   *                         With the above example, we would ask the index for its ordering capability for Seq(CTInt, CTString).
   *                         In the future we also want to able to ask it for prefix sequences (e.g. just Seq(CTInt)).
   * @return the order that the index guarantees, if possible in accordance with the given required order.
   */
  def providedOrderForIndexOperator(interestingOrder: InterestingOrder,
                                    indexProperties: Seq[PropertyAndPredicateType],
                                    orderTypes: Seq[CypherType],
                                    capabilityLookup: Seq[CypherType] => IndexOrderCapability,
                                    providedOrderFactory: ProvidedOrderFactory = DefaultProvidedOrderFactory): (ProvidedOrder, IndexOrder) = {
    def satisfies(indexProperty: Property, expression: Expression, projections: Map[String, Expression]): Boolean =
      AggregationHelper.extractPropertyForValue(expression, projections).contains(indexProperty)

    def getNewProvidedOrderColumn(orderColumn: ColumnOrder, prop: Property): ColumnOrder = orderColumn match {
      case _: Asc  => Asc(prop)
      case _: Desc => Desc(prop)
    }

    if (indexProperties.isEmpty || interestingOrder == InterestingOrder.empty) {
      (ProvidedOrder.empty, IndexOrderNone)
    } else {
      val indexOrderCapability: IndexOrderCapability = capabilityLookup(orderTypes)
      val candidates = interestingOrder.requiredOrderCandidate +: interestingOrder.interestingOrderCandidates

      // Accumulator for the foldLeft
      sealed trait Acc
      final case class OrderNotYetDecided(providedOrderColumns: Seq[ColumnOrder]) extends Acc
      final case class IndexOrderDecided(indexOrder: IndexOrder, providedOrderColumns: Seq[ColumnOrder]) extends Acc
      final case object IndexNotHelpful extends Acc

      def possibleOrdersForCandidate(candidate: OrderCandidate[_]): Acc =
        candidate.order.zipAll(indexProperties, null, null).foldLeft[Acc](OrderNotYetDecided(Seq.empty)) {

          // We decided to use IndexOrderDescending and find another DESC column in the ORDER BY
          case (IndexOrderDecided(IndexOrderDescending, poColumns),
                (Desc(expression, projection), PropertyAndPredicateType(prop, _)))
                if satisfies(prop, expression, projection) && indexOrderCapability.desc =>
            IndexOrderDecided(IndexOrderDescending, poColumns :+ Desc(prop))

          // We have not yet decided on the index order and find a DESC column in the ORDER BY
          case (OrderNotYetDecided(providedOrderColumns),
                (Desc(expression, projection), PropertyAndPredicateType(prop, isSingleExactPredicate)))
                if satisfies(prop, expression, projection) && indexOrderCapability.desc =>
            // If we have an exact predicate here, we do not want to make a decision on the index order yet.
            if (isSingleExactPredicate) {
              OrderNotYetDecided(providedOrderColumns :+ Desc(prop))
            } else {
              IndexOrderDecided(IndexOrderDescending, providedOrderColumns :+ Desc(prop))
            }

          // We decided to use IndexOrderAscending and find another ASC column in the ORDER BY
          case (IndexOrderDecided(IndexOrderAscending, poColumns),
                (Asc(expression, projection), PropertyAndPredicateType(prop, _)))
                if satisfies(prop, expression, projection) && indexOrderCapability.asc =>
            IndexOrderDecided(IndexOrderAscending, poColumns :+ Asc(prop))

          // We have not yet decided on the index order and find an ASC column in the ORDER BY
          case (OrderNotYetDecided(providedOrderColumns),
                (Asc(expression, projection), PropertyAndPredicateType(prop, isSingleExactPredicate)))
                if satisfies(prop, expression, projection) && indexOrderCapability.asc =>
            // If we have an exact predicate here, we do not want to make a decision on the index order yet.
            if (isSingleExactPredicate) {
              OrderNotYetDecided(providedOrderColumns :+ Asc(prop))
            } else {
              IndexOrderDecided(IndexOrderAscending, providedOrderColumns :+ Asc(prop))
            }

          // We find a contradicting order with single exact predicate
          case (IndexOrderDecided(indexOrder, poColumns),
                (orderColumn, PropertyAndPredicateType(prop, true))) if orderColumn != null && satisfies(prop, orderColumn.expression, orderColumn.projections) =>
            IndexOrderDecided(indexOrder, poColumns :+ getNewProvidedOrderColumn(orderColumn, prop))

          // Either
          // * we find a contradicting order without exact predicate,
          // * the index has more columns than the ORDER BY or
          // * the property doesn't match.
          // So we have to add more columns in the same order to the provided order.
          case (IndexOrderDecided(indexOrder, poColumns),
                (_, PropertyAndPredicateType(prop, _))) =>
            val nextCol = indexOrder match {
              case IndexOrderAscending => Asc(prop)
              case IndexOrderDescending => Desc(prop)
            }
            IndexOrderDecided(indexOrder, poColumns :+ nextCol)

          // Index capability and required order don't agree, with single exact predicate
          case (OrderNotYetDecided(providedOrderColumns),
                (orderColumn, PropertyAndPredicateType(prop, true))) if orderColumn != null && satisfies(prop, orderColumn.expression, orderColumn.projections) && indexOrderCapability != NONE =>
            OrderNotYetDecided(providedOrderColumns :+ getNewProvidedOrderColumn(orderColumn, prop))

          // When we have already some provided order columns, but haven't yet decided on the order,
          // that means all previous columns were single exact properties.
          // We can fall into this case when, in addition to the above condition, we either
          // * have more index columns than order candidate columns or
          // * the column has a non-matching property or
          // * the column has a non-matching order
          // In all these cases we make a decision on the order(because that helps the previous single exact property columns).
          case (OrderNotYetDecided(providedOrderColumns),
                (_, PropertyAndPredicateType(prop, _))) if indexOrderCapability != NONE && providedOrderColumns.nonEmpty =>
            if (indexOrderCapability.asc)
              IndexOrderDecided(IndexOrderAscending, providedOrderColumns :+ Asc(prop))
            else IndexOrderDecided(IndexOrderDescending, providedOrderColumns :+ Desc(prop))

          // Either there is no order candidate column or the first column either
          // * has a non-matching property or
          // * non-matching order on a non-(single exact) property or
          // In these cases we can't really use the index to help with this order candidate
          case (OrderNotYetDecided(Seq()),
                (_, PropertyAndPredicateType(_, _))) if indexOrderCapability != NONE =>
            IndexNotHelpful

          case (x, _) =>
            // Anything else does not influence the index order or the provided order columns
            x
        }

      val orderAccPerCandidate: Seq[Acc] = candidates.filter(_.nonEmpty).map(possibleOrdersForCandidate)
      val maybeResult = orderAccPerCandidate.collectFirst {
        case IndexOrderDecided(indexOrder, columns) if columns.nonEmpty =>
          (providedOrderFactory.providedOrder(columns, ProvidedOrder.Self), indexOrder)
        case OrderNotYetDecided(columns) if columns.nonEmpty =>
          val indexOrder = if (indexOrderCapability.asc) IndexOrderAscending
                           else if (indexOrderCapability.desc) IndexOrderDescending
                           else IndexOrderNone
          (providedOrderFactory.providedOrder(columns, ProvidedOrder.Self), indexOrder)
      }

      // If the required order cannot be satisfied, return empty
      maybeResult.getOrElse((ProvidedOrder.empty, IndexOrderNone))
    }
  }

  def providedOrderForLabelScan(interestingOrder: InterestingOrder,
                                variable: Variable,
                                providedOrderFactory: ProvidedOrderFactory = DefaultProvidedOrderFactory): ProvidedOrder = {
    providedOrderForScan(interestingOrder, variable, providedOrderFactory)

  }

  def providedOrderForRelationshipTypeScan(interestingOrder: InterestingOrder,
                                           name: String,
                                           providedOrderFactory: ProvidedOrderFactory = DefaultProvidedOrderFactory): ProvidedOrder = {
    providedOrderForScan(interestingOrder, Variable(name)(InputPosition.NONE), providedOrderFactory)
  }

  private def providedOrderForScan(interestingOrder: InterestingOrder,
                                   variable: Variable,
                                   providedOrderFactory: ProvidedOrderFactory): ProvidedOrder = {
    def satisfies(expression: Expression, projections: Map[String, Expression]): Boolean =
      extractVariableForValue(expression, projections).contains(variable)

    val candidates = interestingOrder.requiredOrderCandidate +: interestingOrder.interestingOrderCandidates

    candidates.map(_.headOption).collectFirst {
      case Some(Desc(expression, projection)) if satisfies(expression, projection) =>
        providedOrderFactory.desc(variable)
      case Some(Asc(expression, projection)) if satisfies(expression, projection) =>
        providedOrderFactory.asc(variable)
    }.getOrElse(ProvidedOrder.empty)
  }


  @tailrec
  private[ordering] def extractVariableForValue(expression: Expression,
                                                renamings: Map[String, Expression]): Option[Variable] = {
    expression match {
      case variable@Variable(varName) =>
        if (renamings.contains(varName) && renamings(varName) != variable)
          extractVariableForValue(renamings(varName), renamings)
        else
          Some(variable)
      case _ => None
    }
  }
}
