package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.idp

import org.neo4j.cypher.internal.compiler.v2_3.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{LogicalPlanningSupport, LogicalPlanningContext}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{LogicalPlan, PatternRelationship}

case class AdaptiveSolverStep(qg: QueryGraph, patternLengthThreshold: Int) extends IDPSolverStep[PatternRelationship, LogicalPlan, LogicalPlanningContext] {

  val join = joinSolverStep(qg)
  val expand = expandSolverStep(qg)

  override def apply(registry: IdRegistry[PatternRelationship], goal: Goal, table: IDPCache[LogicalPlan])
                    (implicit context: LogicalPlanningContext): Iterator[LogicalPlan] = {
    if (goal.size >= patternLengthThreshold)
      expand(registry, goal, table)
    else
      expand(registry, goal, table) ++ join(registry, goal, table)
  }
}
