package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.compiler.v2_1.ast.Expression

object initialiser extends Transformer2[Unit, PlanTable] {
  def apply(ignored: Unit)(implicit context: LogicalPlanContext): PlanTable = {
    val predicates: Seq[Expression] = context.queryGraph.selections.flatPredicates
    val labelPredicateMap = context.queryGraph.selections.labelPredicates

    val leafPlanners = Seq(
      idSeekLeafPlanner(predicates),
      uniqueIndexSeekLeafPlanner(predicates, labelPredicateMap),
      indexSeekLeafPlanner(predicates, labelPredicateMap),
      labelScanLeafPlanner(labelPredicateMap),
      allNodesLeafPlanner()
    )

    val plans: Seq[LogicalPlan] = leafPlanners.flatMap(_.apply)

    val candidateLists: Iterable[CandidateList] = plans.foldLeft(Map[Set[IdName], CandidateList]()) {
      case (acc, plan) =>
        val candidatesForThisId = acc.getOrElse(plan.coveredIds, CandidateList(Seq.empty)) + plan
        acc + (plan.coveredIds -> candidatesForThisId)
    }.values

    candidateLists.foldLeft(PlanTable()) {
      case (planTable, candidateList) => planTable + candidateList.topPlan
    }
  }
}
