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
package org.neo4j.cypher.internal.compiler

import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Union

/**
 * The execution model of how a runtime executes a query.
 * This information can be used in planning.
 */
sealed trait ExecutionModel {
  /**
   * @return true if the execution model does not maintain provided order for a specific logical plan (not recursive)
   *
   * This mainly targets plans that in a particular execution model invalidates the order of arguments rows on
   * the right-hand side of an Apply. In addition to this there are also other more general rules for how plans
   * affects provided order.
   *
   * The check is invoked on each plan under an Apply and the implementation is not expected to recurse into it children.
   */
  def invalidatesProvidedOrder(plan: LogicalPlan): Boolean
}

object ExecutionModel {
  val default: ExecutionModel = Batched

  case object Volcano extends ExecutionModel {
    override def invalidatesProvidedOrder(plan: LogicalPlan): Boolean = false
  }

  case object Batched extends ExecutionModel {
    override def invalidatesProvidedOrder(plan: LogicalPlan): Boolean = plan match {
      case _: Union =>
        true
      case _ =>
        false
    }
  }
}
