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
from the query grap to the plan.
 */
class ExpansionPlanGenerator(calculator: CostCalculator, estimator: CardinalityEstimator) extends PlanGenerator {

  def generatePlan(planContext: PlanContext, qg: QueryGraph, currentPlan: PlanTable): PlanTable = {
    //    var currentPlan: PlanTable = buildInitialTable(planContext, qg)

//    // Loop until we've found a single plan to use.
//      val cheapestNewPlan: PartialPlan = findImprovedPartialPlan(planContext, currentPlan, qg)
//      currentPlan = currentPlan.addAndRemove(cheapestNewPlan)
//    }
//
//    currentPlan.plan
//  }
//
//  private def findImprovedPartialPlan(planContext: PlanContext, table: PlanTable, qg: QueryGraph): PartialPlan = {
//    var result: Option[PartialPlan] = None
//
//    for (ids <- table.m.keys) {
//      val plan = table(ids)
//
//      // find extensions
//      for (id <- ids) {
//        for (rel <- qg.graphRelsById(id)) {
//          val otherId = rel.other(id)
//
//          if (! ids(otherId)) {
//            // construct plan and candidate
//            val relTypeTokens = rel.types.map(t => planContext.relationshipTypeGetId(t.name))
//            val cardinality = estimator.estimateExpandRelationship(Seq.empty, relTypeTokens, rel.direction)
//            val expandedPlan = ExpandRelationships(plan, rel.direction, calculator.costForExpandRelationship(cardinality))
//            val candidate = PartialPlan(ids, Set(otherId), expandedPlan)
//
//            result = candidate.update(result)
//          }
//        }
//      }
//
//    }
//
//    result.get
//  }
    ???
  }
}

case class PartialPlan(lhs: Set[Id], rhs: Set[Id], plan: AbstractPlan) {
  def update(current: Option[PartialPlan]): Option[PartialPlan] = current match {
    case None                                         => Some(this)
    case Some(best) if best.plan.effort > plan.effort => Some(this)
    case _                                            => current
  }
}