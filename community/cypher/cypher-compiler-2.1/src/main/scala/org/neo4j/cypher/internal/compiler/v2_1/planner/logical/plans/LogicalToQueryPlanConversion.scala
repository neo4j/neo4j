package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans

// TODO: Eventually kill uses and move back to tests
object LogicalToQueryPlanConversion {
   def apply(plan: LogicalPlan) = plan match {
    case logicalPlan: AllNodesScan =>
      AllNodesScan.queryPlan(logicalPlan)

    case _ =>
      QueryPlan(plan)
  }
}
