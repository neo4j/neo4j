package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_1.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.{LogicalPlan, OuterHashJoin}
import org.neo4j.cypher.internal.compiler.v2_1.planner.QueryGraph

object outerJoin {
  def apply(planTable: PlanTable)(implicit context: LogicalPlanContext): CandidateList = {

    val outerJoinPlans: Seq[OuterHashJoin] = for {
      optionalQG <- context.queryGraph.optionalMatches
      lhs <- planTable.plans if applicable(lhs, optionalQG)
    } yield {
      val rhs = context.strategy.plan(context.copy(queryGraph = optionalQG, argumentIds = Set.empty))
      OuterHashJoin(optionalQG.argumentIds.head, lhs, rhs)
    }

    CandidateList(outerJoinPlans)
  }

  private def applicable(outerPlan: LogicalPlan, optionalQG: QueryGraph) = {
    val singleArgument = optionalQG.argumentIds.size == 1
    val coveredByLHS = singleArgument && outerPlan.coveredIds(optionalQG.argumentIds.head)
    val isSolved = (optionalQG.coveredIds -- outerPlan.coveredIds).isEmpty

    singleArgument && coveredByLHS && !isSolved
  }
}
