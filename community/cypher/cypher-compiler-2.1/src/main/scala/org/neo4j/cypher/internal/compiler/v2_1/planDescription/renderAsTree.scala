/**
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_1.planDescription

object renderAsTree extends (PlanDescription => String) {

  def apply(plan: PlanDescription): String = {

    val result = new StringBuilder()
    val newLine = "%n".format()
    val names: Map[PlanDescription, String] = createUniqueNames(plan)


    def accumulateTree(plan: PlanDescription, prepend: String) {
      result.append(names(plan))

      plan.children match {
        case NoChildren =>

        case SingleChild(inner) =>
          result.
            append(newLine).
            append(prepend).append("  |").append(newLine).
            append(prepend).append("  +")
          accumulateTree(inner, prepend + "  ")

        case TwoChildren(lhs, rhs) =>
          result.
            append(newLine).
            append(prepend).append("  |").append(newLine).
            append(prepend).append("  +")
          accumulateTree(lhs, prepend + "  |")

          result.
            append(newLine).
            append(prepend).append("  |").append(newLine).
            append(prepend).append("  +")
          accumulateTree(rhs, prepend + "   ")
      }
    }

    accumulateTree(plan, "")
    result.toString()
  }

  def createUniqueNames(plan: PlanDescription): Map[PlanDescription, String] =
    plan.toSeq.groupBy(_.name).flatMap {
      case (name, plans) if plans.size == 1 =>
        Some(plans.head -> name)

      case (name, plans) =>
        plans.zipWithIndex.map {
          case (p, i) => p -> (p.name + s"($i)")
        }
    }
}
