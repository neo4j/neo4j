/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder.Asc
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder.Desc
import org.neo4j.cypher.internal.ir.ordering.OrderCandidate
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder.Column
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderDescending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability.NONE
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
                                    capabilityLookup: Seq[CypherType] => IndexOrderCapability): (ProvidedOrder, IndexOrder) = {


    def satisfies(indexProperty: Property, expression: Expression, projections: Map[String, Expression]): Boolean =
      AggregationHelper.extractPropertyForValue(expression, projections).contains(indexProperty)

    def getNewProvidedOrderColumn(orderColumn: InterestingOrder.ColumnOrder, prop: Property): Column = orderColumn match {
      case _: Asc  => ProvidedOrder.Asc(prop)
      case _: Desc => ProvidedOrder.Desc(prop)
    }

    if (indexProperties.isEmpty) {
      (ProvidedOrder.empty, IndexOrderNone)
    } else {
      val indexOrderCapability: IndexOrderCapability = capabilityLookup(orderTypes)
      val candidates = interestingOrder.requiredOrderCandidate +: interestingOrder.interestingOrderCandidates

      // Accumulator for the foldLeft
      sealed trait Acc
      case class OrderNotYetDecided(providedOrderColumns: Seq[Column]) extends Acc
      case class IndexOrderDecided(indexOrder: IndexOrder, providedOrderColumns: Seq[Column]) extends Acc

      def possibleOrdersForCandidate(candidate: OrderCandidate[_]): Acc =
        candidate.order.zipAll(indexProperties, null, null).foldLeft[Acc](OrderNotYetDecided(Seq.empty)) {

          // We decided to use IndexOrderDescending and find another DESC column in the ORDER BY
          case (IndexOrderDecided(IndexOrderDescending, poColumns),
                (Desc(expression, projection), PropertyAndPredicateType(prop, _)))
                if satisfies(prop, expression, projection) && indexOrderCapability.desc =>
            IndexOrderDecided(IndexOrderDescending, poColumns :+ ProvidedOrder.Desc(prop))

          // We have not yet decided on the index order and find a DESC column in the ORDER BY
          case (OrderNotYetDecided(providedOrderColumns),
                (Desc(expression, projection), PropertyAndPredicateType(prop, isSingleExactPredicate)))
                if satisfies(prop, expression, projection) && indexOrderCapability.desc =>
            // If we have an exact predicate here, we do not want to make a decision on the index order yet.
            if (isSingleExactPredicate) {
              OrderNotYetDecided(providedOrderColumns :+ ProvidedOrder.Desc(prop))
            } else {
              IndexOrderDecided(IndexOrderDescending, providedOrderColumns :+ ProvidedOrder.Desc(prop))
            }

          // We decided to use IndexOrderAscending and find another ASC column in the ORDER BY
          case (IndexOrderDecided(IndexOrderAscending, poColumns),
                (Asc(expression, projection), PropertyAndPredicateType(prop, _)))
                if satisfies(prop, expression, projection) && indexOrderCapability.asc =>
            IndexOrderDecided(IndexOrderAscending, poColumns :+ ProvidedOrder.Asc(prop))

          // We have not yet decided on the index order and find an ASC column in the ORDER BY
          case (OrderNotYetDecided(providedOrderColumns),
                (Asc(expression, projection), PropertyAndPredicateType(prop, isSingleExactPredicate)))
                if satisfies(prop, expression, projection) && indexOrderCapability.asc =>
            // If we have an exact predicate here, we do not want to make a decision on the index order yet.
            if (isSingleExactPredicate) {
              OrderNotYetDecided(providedOrderColumns :+ ProvidedOrder.Asc(prop))
            } else {
              IndexOrderDecided(IndexOrderAscending, providedOrderColumns :+ ProvidedOrder.Asc(prop))
            }

          // We find a contradicting order with single exact predicate
          case (IndexOrderDecided(indexOrder, poColumns),
                (orderColumn, PropertyAndPredicateType(prop, true))) if orderColumn != null && satisfies(prop, orderColumn.expression, orderColumn.projections) =>
            IndexOrderDecided(indexOrder, poColumns :+ getNewProvidedOrderColumn(orderColumn, prop))

          // We find a contradicting order without exact predicate, the index has more columns than the ORDER BY or the property doesn't match, so we have to add more columns in the same order to the provided order
          case (IndexOrderDecided(indexOrder, poColumns),
                (_, PropertyAndPredicateType(prop, _))) =>
            val nextCol = indexOrder match {
              case IndexOrderAscending => ProvidedOrder.Asc(prop)
              case IndexOrderDescending => ProvidedOrder.Desc(prop)
            }
            IndexOrderDecided(indexOrder, poColumns :+ nextCol)

          // Index capability and required order don't agree, with single exact predicate
          case (OrderNotYetDecided(providedOrderColumns),
                (orderColumn, PropertyAndPredicateType(prop, true))) if orderColumn != null && satisfies(prop, orderColumn.expression, orderColumn.projections) && indexOrderCapability != NONE =>
            OrderNotYetDecided(providedOrderColumns :+ getNewProvidedOrderColumn(orderColumn, prop))

          // Index capability and required order don't agree, the index has more columns than the ORDER BY or the property doesn't match, so we have to add more columns in the same order to the provided order
          case (OrderNotYetDecided(providedOrderColumns),
                (_, PropertyAndPredicateType(prop, _))) if indexOrderCapability != NONE =>
            if (indexOrderCapability.asc) IndexOrderDecided(IndexOrderAscending, providedOrderColumns :+ ProvidedOrder.Asc(prop))
            else IndexOrderDecided(IndexOrderDescending, providedOrderColumns :+ ProvidedOrder.Desc(prop))

          case (x, _) =>
            // Anything else does not influence the index order or the provided order columns
            x
        }

      val orderAccPerCandidate: Seq[Acc] = candidates.filter(_.nonEmpty).map(possibleOrdersForCandidate)
      val maybeResult = orderAccPerCandidate.collectFirst {
        case IndexOrderDecided(indexOrder, columns) if columns.nonEmpty =>
          (ProvidedOrder(columns, ProvidedOrder.Self), indexOrder)
        case OrderNotYetDecided(columns) if columns.nonEmpty =>
          val indexOrder = if (indexOrderCapability.asc) IndexOrderAscending
                           else if (indexOrderCapability.desc) IndexOrderDescending
                           else IndexOrderNone
          (ProvidedOrder(columns, ProvidedOrder.Self), indexOrder)
      }

      // If the required order cannot be satisfied, return the index guaranteed order
      maybeResult.getOrElse {
        if (indexOrderCapability.asc) {
          (ProvidedOrder(indexProperties.map { case PropertyAndPredicateType(prop, _) => ProvidedOrder.Asc(prop) }, ProvidedOrder.Self), IndexOrderAscending)
        } else if (indexOrderCapability.desc) {
          (ProvidedOrder(indexProperties.map { case PropertyAndPredicateType(prop, _) => ProvidedOrder.Desc(prop) }, ProvidedOrder.Self), IndexOrderDescending)
        } else {
          (ProvidedOrder.empty, IndexOrderNone)
        }
      }
    }
  }

  def providedOrderForLabelScan(interestingOrder: InterestingOrder,
                                variable: Variable): ProvidedOrder = {
    def satisfies(expression: Expression, projections: Map[String, Expression]): Boolean =
      extractVariableForValue(expression, projections).contains(variable)

    val candidates = interestingOrder.requiredOrderCandidate +: interestingOrder.interestingOrderCandidates

    candidates.map(_.headOption).collectFirst {
      case Some(Desc(expression, projection)) if satisfies(expression, projection) =>
        ProvidedOrder.desc(variable)
      case Some(Asc(expression, projection)) if satisfies(expression, projection) =>
        ProvidedOrder.asc(variable)
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
