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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_2.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{CartesianProduct, LogicalPlan}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.CandidateList
import org.neo4j.cypher.internal.compiler.v2_2.planner.QueryGraph
import org.neo4j.cypher.internal.helpers.Converge.iterateUntilConverged

object cartesianProduct extends CandidateGenerator[PlanTable] {
  def apply(planTable: PlanTable, ignored: QueryGraph)(implicit context: LogicalPlanningContext): CandidateList = {
    val usablePlans = iterateUntilConverged { usablePlans: Set[LogicalPlan] =>
      val cartesianProducts = for (planA <- usablePlans; planB <- usablePlans if planA != planB) yield planCartesianProduct(planA, planB)
      if (cartesianProducts.isEmpty) {
        usablePlans
      } else {
        val worstCartesianProduct = cartesianProducts.minBy(p => context.cost(p, context.inboundCardinality))
        usablePlans - worstCartesianProduct.left - worstCartesianProduct.right + worstCartesianProduct
      }
    } (planTable.plans.filter(_.solved.graph.argumentIds.isEmpty).toSet)
    context.metrics.candidateListCreator(usablePlans.toSeq.collect { case c: CartesianProduct => c })
  }
}
