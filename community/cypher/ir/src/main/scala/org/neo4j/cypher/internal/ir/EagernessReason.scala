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
package org.neo4j.cypher.internal.ir

import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.util.Rewritable
import org.neo4j.cypher.internal.util.attribution.Id

sealed trait EagernessReason

object EagernessReason {

  case class Conflict(first: Id, second: Id) extends Rewritable {

    override def dup(children: Seq[AnyRef]): this.type = {
      val a = children(0).asInstanceOf[Id]
      val b = children(1).asInstanceOf[Id]
      if (a != first || b != second) {
        Conflict(a, b).asInstanceOf[this.type]
      } else {
        this
      }
    }
  }

  case object Unknown extends EagernessReason
  case object UpdateStrategyEager extends EagernessReason
  case object WriteAfterCallInTransactions extends EagernessReason
  case object ProcedureCallEager extends EagernessReason

  /**
   * Non-unique reasons can be reported by multiple conflicting plan pairs.
   */
  sealed trait NonUnique extends EagernessReason {
    def withConflict(conflict: Conflict): ReasonWithConflict = ReasonWithConflict(this, conflict)
  }

  final case class LabelReadSetConflict(label: LabelName) extends NonUnique
  final case class TypeReadSetConflict(relType: RelTypeName) extends NonUnique
  final case class LabelReadRemoveConflict(label: LabelName) extends NonUnique
  case object UnknownLabelReadSetConflict extends NonUnique
  case object UnknownLabelReadRemoveConflict extends NonUnique

  /**
   * Note: 
   * We don't usually use Strings to represent Variables in anything that can end up
   * in a LogicalPlan.
   *
   * But `identifier` is on purpose kept as a String and not a variable.
   * This is because it can refer to a variable only declared later in the plan,
   * and would otherwise require work in SlottedRewriter and LivenessAnalysis to
   * work around that fact. Since `identifier` is only for EXPLAIN and does not
   * actually mean that we reference that column here, having it as a String should be OK.
   */
  final case class ReadDeleteConflict(identifier: String) extends NonUnique
  final case class PropertyReadSetConflict(property: PropertyKeyName) extends NonUnique
  case object ReadCreateConflict extends NonUnique
  case object UnknownPropertyReadSetConflict extends NonUnique

  final case class ReasonWithConflict(reason: NonUnique, conflict: Conflict) extends EagernessReason

  /**
   * @param summary For a given non-unique reason, contains a single (arbitrary) conflicting plan pair
   *                and a total number of conflicting pairs with the same reason.
   */
  final case class Summarized(summary: Map[NonUnique, SummaryEntry])
      extends EagernessReason {

    def addReason(r: ReasonWithConflict): Summarized = {
      copy(summary = summary.updatedWith(r.reason) {
        case None        => Some(SummaryEntry(r.conflict, totalConflictCount = 1))
        case Some(entry) => Some(entry.incCount)
      })
    }
  }

  object Summarized {
    val empty: Summarized = Summarized(Map.empty)
  }

  final case class SummaryEntry(conflictToReport: Conflict, totalConflictCount: Int) {
    def incCount: SummaryEntry = copy(totalConflictCount = totalConflictCount + 1)
  }
}
