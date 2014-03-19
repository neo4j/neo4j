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

import org.neo4j.cypher.internal.compiler.v2_1.planner._
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import scala.annotation.tailrec

object SimpleLogicalPlanner {

  trait LeafPlanner {
    def apply()(implicit context: LogicalPlanContext): CandidateList
  }

  case class CandidateList(plans: Seq[LogicalPlan]) {
    def pruned: CandidateList = {
      def overlap(a: Set[IdName], b: Set[IdName]) = !a.intersect(b).isEmpty

      @tailrec
      def recurse(covered: Set[IdName], todo: Seq[LogicalPlan], result: Seq[LogicalPlan]): Seq[LogicalPlan] = todo match {
        case entry :: tail if overlap(covered, entry.coveredIds) =>
          recurse(covered, tail, result)
        case entry :: tail =>
          recurse(covered ++ entry.coveredIds, tail, result :+ entry)
        case _ =>
          result
      }

      CandidateList(recurse(Set.empty, plans, Seq.empty))
    }

    def sorted = CandidateList(plans.sortBy(_.cardinality))

    def ++(other: CandidateList): CandidateList = CandidateList(plans ++ other.plans)
  }

  def plan(qg: QueryGraph)(implicit context: LogicalPlanContext): LogicalPlan = {
    val initialPlanTable = initialisePlanTable(qg)

    val bestPlanEntry = if (initialPlanTable.isEmpty)
      SingleRow()
    else {
      val convergedPlans = if (initialPlanTable.size > 1) {
        expandAndJoin(initialPlanTable)
      } else {
        initialPlanTable
      }

      val bestPlanEntry = convergedPlans.plans.head
      if (!qg.selections.coveredBy(bestPlanEntry.solvedPredicates))
        throw new CantHandleQueryException

      bestPlanEntry
    }

    ProjectionPlanner.amendPlan(qg, bestPlanEntry)
  }

  private def initialisePlanTable(qg: QueryGraph)(implicit context: LogicalPlanContext): PlanTable = {
    val predicates: Seq[Expression] = qg.selections.flatPredicates
    val labelPredicateMap = qg.selections.labelPredicates

    val leafPlanners = Seq(
      idSeekLeafPlanner(predicates),
      uniqueIndexSeekLeafPlanner(predicates, labelPredicateMap),
      indexSeekLeafPlanner(predicates, labelPredicateMap),
      labelScanLeafPlanner(qg, labelPredicateMap),
      allNodesLeafPlanner(qg)
    )
    val candidateList = leafPlanners.map(_.apply).fold(CandidateList(Seq.empty))(_ ++ _)
    PlanTable() ++ candidateList.sorted.pruned
  }
}
