package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans

import org.neo4j.cypher.internal.compiler.v2_1.planner.QueryGraph

// TODO: Eventually kill uses and move back to tests
object LogicalToQueryPlanConversion {
   def apply(plan: LogicalPlan): QueryPlan = plan match {

    // leave plans

    case logicalPlan: AllNodesScan =>
      AllNodesScan.queryPlan(logicalPlan)

    case logicalPlan: NodeByLabelScan =>
      NodeByLabelScan.queryPlan(logicalPlan)

    case logicalPlan: NodeByIdSeek =>
      NodeByIdSeek.queryPlan(logicalPlan)

    case logicalPlan: DirectedRelationshipByIdSeek =>
      DirectedRelationshipByIdSeek.queryPlan(logicalPlan)

    case logicalPlan: UndirectedRelationshipByIdSeek =>
      UndirectedRelationshipByIdSeek.queryPlan(logicalPlan)

    case logicalPlan: NodeIndexUniqueSeek =>
      NodeIndexUniqueSeek.queryPlan(logicalPlan)

    case logicalPlan: NodeIndexSeek =>
      NodeIndexSeek.queryPlan(logicalPlan)

    case logicalPlan: SingleRow =>
      SingleRow.queryPlan(logicalPlan)

    case _ =>
      QueryPlan(plan)
  }
}
