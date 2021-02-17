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

import scala.annotation.tailrec

object DisallowSplittingTop {

  def demoteRequiredOrderToInterestingOrder(query: SinglePlannerQuery, isHorizon: Boolean, disallowSplittingTop: Boolean): Boolean = {
    if (disallowSplittingTop) {
      requiredOrderOrigin(query) match {
        case Some(SelfOrderByLimit) if !isHorizon => true // Don't plan Sort in QG if it splits a later Top candidate
        case Some(TailOrderByLimit)               => true // Don't plan Sort if it splits a later Top candidate
        case _                                    => false
      }
    } else {
      false
    }
  }

  sealed trait OrderingOrigin
  case object SelfOrderBy extends OrderingOrigin
  case object SelfOrderByLimit extends OrderingOrigin
  case object TailOrderBy extends OrderingOrigin
  case object TailOrderByLimit extends OrderingOrigin

  private[ordering] def requiredOrderOrigin(query: SinglePlannerQuery): Option[OrderingOrigin] = {
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
