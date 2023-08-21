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
package org.neo4j.cypher.internal.plandescription

import org.neo4j.cypher.internal.plandescription.Arguments.GlobalMemory
import org.neo4j.cypher.internal.plandescription.InternalPlanDescription.TotalHits

object renderSummary extends (InternalPlanDescription => String) {

  def apply(plan: InternalPlanDescription): String = {
    val memStr = memory(plan).map(bytes => s", total allocated memory: $bytes").getOrElse("")
    s"Total database accesses: ${dbhits(plan)}$memStr"
  }

  private def dbhits(plan: InternalPlanDescription): String = {
    plan.totalDbHits match {
      case TotalHits(0, false) => "0"
      case TotalHits(0, true)  => "?"
      case TotalHits(x, false) => x.toString
      case TotalHits(x, true)  => s"$x + ?"
    }
  }

  private def memory(plan: InternalPlanDescription): Option[String] = {
    plan.arguments.collectFirst {
      case GlobalMemory(x) => x.toString
    }
  }
}
