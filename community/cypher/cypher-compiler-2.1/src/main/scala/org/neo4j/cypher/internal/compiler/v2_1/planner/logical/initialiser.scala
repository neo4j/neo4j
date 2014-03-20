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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.compiler.v2_1.ast.Expression

class initialiser(applySelections: SelectionApplicator) extends NodeIdentifierInitialiser {
  def apply()(implicit context: LogicalPlanContext): PlanTable = {
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
      case (planTable, candidateList) => planTable + applySelections(candidateList.topPlan)
    }
  }
}
