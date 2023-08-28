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
package org.neo4j.cypher.internal.logical.plans

import scala.collection.mutable

object LogicalPlanTreeRenderer {
  private case class LevelPlanItem(level: Int, plan: LogicalPlan)

  def render(logicalPlan: LogicalPlan, indentSymbol: String, planRepresentation: LogicalPlan => String): String = {
    var childrenStack = LevelPlanItem(0, logicalPlan) :: Nil
    val sb = new mutable.StringBuilder()

    while (childrenStack.nonEmpty) {
      val LevelPlanItem(level, plan) = childrenStack.head
      childrenStack = childrenStack.tail

      sb ++= indentSymbol * level

      val firstLevelRepresentation: String = planRepresentation(plan)
      val correctLevelRepresentation = firstLevelRepresentation.replace("\n", s"\n${" " * indentSymbol.length * level}")
      sb ++= correctLevelRepresentation

      plan.lhs.foreach(lhs => childrenStack ::= LevelPlanItem(level, lhs))
      plan.rhs.foreach(rhs => childrenStack ::= LevelPlanItem(level + 1, rhs))

      if (childrenStack.nonEmpty) sb ++= System.lineSeparator()
    }

    sb.toString()
  }
}
