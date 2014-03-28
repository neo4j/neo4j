/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.SingleRow
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.AllNodesScan
import Metrics._

class SimpleCostModel(cardinality: cardinalityEstimator) extends costModel {

  def apply(plan: LogicalPlan): Int = plan match {
    case _: SingleRow =>
      cardinality(plan)

    case _: AllNodesScan =>
      cardinality(plan)

    case _: NodeIndexSeek =>
      cardinality(plan) * 3

    case _: NodeIndexUniqueSeek =>
      cardinality(plan) * 3

    case _: NodeByLabelScan =>
      cardinality(plan) * 2

    case _: NodeByIdSeek =>
      cardinality(plan)

    case _: DirectedRelationshipByIdSeek =>
      cardinality(plan)

    case _: UndirectedRelationshipByIdSeek =>
      cardinality(plan)

    case projection: Projection =>
      cost(projection.left) + (cardinality(projection.left) * 0.01 * projection.numExpressions).toInt

    case selection: Selection =>
      cost(selection.left) + (cardinality(selection.left) * .2 * selection.numPredicates).toInt

    case cartesian: CartesianProduct =>
      cardinality(cartesian.left) * cost(cartesian.right) + cost(cartesian.left)

    case expand: Expand =>
      cost(expand.left) + cardinality(expand)

    case _: NodeHashJoin =>
      cardinality(plan) * 2
  }

  private def cost(plan: LogicalPlan) = apply(plan)
}
