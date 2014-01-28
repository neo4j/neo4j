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
To eagerly drop plans, we first order the plans by cost. Going from cheapest to most expensive, for every plan found,
call it X, go over the other plans and remove any plan that is covered by X. Here is an example. We start with the
table already ordered by cost.
 */
case class CullingPlanGenerator() extends PlanGenerator {
  def generatePlan(planContext: PlanContext, qg: QueryGraph, planTable: PlanTable): PlanTable = {

    if (planTable.size < 2)
      return planTable

    val sortedPlans: Seq[AbstractPlan] = sortPlans(planTable)
    val culledPlans = cullPlans(null, sortedPlans)

    PlanTable(culledPlans)
  }


  def sortPlans(planTable: PlanTable): Seq[AbstractPlan] = {
    planTable.plans.sortWith {
      case (plan1, plan2) => plan1.effort < plan2.effort
    }
  }

  private def cullPlans(current: AbstractPlan, plans: Seq[AbstractPlan]): Seq[AbstractPlan] =
    if (plans.size < 2 || current == plans.last)
      plans
    else {
      val nextPlan = plans.apply(plans.indexOf(current) + 1)
      val newPlans = plans removePlansCoveredBy nextPlan
      cullPlans(nextPlan, newPlans)
    }

  implicit class RichPlan(inner: Seq[AbstractPlan]) {
    def removePlansCoveredBy(that: AbstractPlan) = inner.filter {
      case plan if that.covers(plan) && that != plan => false
      case _ => true
    }
  }
}
