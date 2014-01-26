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
package org.neo4j.cypher.internal.compiler.v2_1.planner

import org.neo4j.cypher.internal.compiler.v2_1.spi.PlanContext

/*
This plan generator adds plans by taking all existing plans and adding any query relationships
from the query graph to the plan.
 */
class ExpansionPlanGenerator(calculator: CostCalculator, estimator: CardinalityEstimator) extends PlanGenerator {

  def generatePlan(planContext: PlanContext, qg: QueryGraph, currentPlan: PlanTable): PlanTable = {
    val newPlans: Seq[AbstractPlan] = for (plan <- currentPlan.plans;
                                           id <- plan.coveredIds if qg.graphRelsById.contains(id);
                                           rel <- qg.graphRelsById(id) if !plan.coveredIds(rel.other(id))) yield {
      // construct plan and candidate
      val relTypeTokens = rel.types.map(t => planContext.relationshipTypeGetId(t.name))
      val cardinality = estimator.estimateExpandRelationship(Seq.empty, relTypeTokens, rel.direction)
      ExpandRelationships(plan, rel.direction, calculator.costForExpandRelationship(cardinality), rel.other(id))
    }
    currentPlan.add(newPlans)
  }
}
