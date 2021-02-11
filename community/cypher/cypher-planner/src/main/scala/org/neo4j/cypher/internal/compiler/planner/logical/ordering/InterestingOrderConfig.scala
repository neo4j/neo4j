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

import org.neo4j.cypher.internal.ir.QueryProjection
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder

import scala.annotation.tailrec

/**
 * When there is an ORDER BY later in the query, it can make sense to solve it earlier.
 * In that case, we are not required to solve it and cannot mark it as solved.
 * To account for this, we need to track both the order to report (which is the one required to solve right now)
 * and the one to solve, which can come from a later point of the query.
 */
final case class InterestingOrderConfig(
  orderToReport: InterestingOrder,
  orderToSolve: InterestingOrder,
)

object InterestingOrderConfig {
  /**
   * An InterestingOrderConfig with the same order to report and order to solve.
   */
  def apply(orderToReportAndSolve: InterestingOrder): InterestingOrderConfig =
    InterestingOrderConfig(orderToReportAndSolve, orderToReportAndSolve)

  val empty: InterestingOrderConfig = InterestingOrderConfig(InterestingOrder.empty)

  def interestingOrderForPart(query: SinglePlannerQuery, isRhs: Boolean, isHorizon: Boolean, disallowSplittingTop: Boolean): InterestingOrderConfig = {
    val readOnly = if (isHorizon) query.tail.forall(_.readOnly) else query.readOnly

    if (isRhs || !readOnly) {
      InterestingOrderConfig(query.interestingOrder.asInteresting)
    } else {
      val orderToReport = query.interestingOrder
      val orderToConsiderSolving = query.findFirstRequiredOrder.fold(orderToReport) { order =>
        // merge interesting order candidates
        val existing = order.interestingOrderCandidates.toSet
        val extraCandidates = orderToReport.interestingOrderCandidates.filterNot(existing.contains)
        order.copy(interestingOrderCandidates = order.interestingOrderCandidates ++ extraCandidates)
      }

      val orderToSolve = if (disallowSplittingTop) {
        requiredOrderOrigin(query) match {
          case Some(SelfOrderByLimit) if !isHorizon => orderToConsiderSolving.asInteresting // Don't plan Sort in QG if it splits a later Top candidate
          case Some(TailOrderByLimit)               => orderToConsiderSolving.asInteresting // Don't plan Sort if it splits a later Top candidate
          case _                                    => orderToConsiderSolving
        }
      } else {
        orderToConsiderSolving
      }

      InterestingOrderConfig(orderToReport = orderToReport, orderToSolve = orderToSolve)
    }
  }

  sealed trait OrderingOrigin
  case object SelfOrderBy extends OrderingOrigin
  case object SelfOrderByLimit extends OrderingOrigin
  case object TailOrderBy extends OrderingOrigin
  case object TailOrderByLimit extends OrderingOrigin

  private def requiredOrderOrigin(query: SinglePlannerQuery): Option[OrderingOrigin] = {
    @tailrec
    def recurse(query: SinglePlannerQuery, inTail: Boolean): Option[OrderingOrigin] = {
      val hasOrderBy = query.interestingOrder.requiredOrderCandidate.nonEmpty
      val hasLimit = query.horizon match {
        case qp: QueryProjection => qp.queryPagination.limit.nonEmpty
        case _                   => false
      }
      (inTail, hasOrderBy, hasLimit) match {
        case (false, true, true)  => Some(SelfOrderByLimit)
        case (true, true, true)  => Some(TailOrderByLimit)
        case (false, true, false) => Some(SelfOrderBy)
        case (true, true, false) => Some(TailOrderBy)
        case (_, false, _)    => query.tail match {
          case Some(tail) => recurse(tail, inTail = true)
          case None       => None
        }
      }
    }
    recurse(query, inTail = false)
  }
}
