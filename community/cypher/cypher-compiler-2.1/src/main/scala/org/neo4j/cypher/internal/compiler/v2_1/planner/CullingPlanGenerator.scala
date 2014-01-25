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

    var sortedPlans = planTable.m.sortWith {
      case (plan1, plan2) => plan1.effort < plan2.effort
    }

    var currentPlan: AbstractPlan = null

    while (currentPlan != sortedPlans.last) {
      // Get the next plan, or the first if we don't have a current plan
      currentPlan = sortedPlans.apply(sortedPlans.indexOf(currentPlan) + 1)

      sortedPlans = sortedPlans.filter {
        case plan if currentPlan.covers(plan) && currentPlan != plan => false
        case _ => true
      }
    }

    PlanTable(sortedPlans)
  }
}
