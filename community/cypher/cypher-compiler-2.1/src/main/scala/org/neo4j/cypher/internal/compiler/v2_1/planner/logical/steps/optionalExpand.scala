/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_1.planner.{Selections, QueryGraph}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps.QueryPlanProducer._
import org.neo4j.cypher.internal.compiler.v2_1.ast.PatternExpression

object optionalExpand extends CandidateGenerator[PlanTable] {

  def apply(planTable: PlanTable, queryGraph: QueryGraph)(implicit context: LogicalPlanningContext, subQueriesLookupTable: Map[PatternExpression, QueryGraph]): CandidateList = {

    val outerJoinPlans: Seq[QueryPlan] = for {
      optionalQG <- queryGraph.optionalMatches
      lhs <- planTable.plans
      patternRel <- findSinglePatternRelationship(lhs, optionalQG)
      argumentId = optionalQG.argumentIds.head
      otherSide = patternRel.otherSide( argumentId )
      if canSolveAllPredicates(optionalQG.selections, lhs.availableSymbols + otherSide + patternRel.name)
    } yield {
      val dir = patternRel.directionRelativeTo(argumentId)
      planOptionalExpand(lhs, argumentId, dir, patternRel.types, otherSide, patternRel.name, patternRel.length, optionalQG.selections.flatPredicates, optionalQG)
    }

    CandidateList(outerJoinPlans)
  }

  private def canSolveAllPredicates(selections:Selections, ids:Set[IdName]) = selections.predicatesGiven(ids) == selections.flatPredicates

  private def findSinglePatternRelationship(outerPlan: QueryPlan, optionalQG: QueryGraph): Option[PatternRelationship] = {
    val singleArgumentAvailable = optionalQG.argumentIds.size == 1 && outerPlan.plan.availableSymbols(optionalQG.argumentIds.head)
    val isSolved = outerPlan.solved.graph.optionalMatches.contains(optionalQG)
    val hasOnlyOnePatternRelationship = optionalQG.patternRelationships.size == 1

    if (singleArgumentAvailable && !isSolved && hasOnlyOnePatternRelationship) {
      optionalQG.patternRelationships.headOption
    } else {
      None
    }
  }
}
