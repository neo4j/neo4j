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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_1
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import v2_1.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.CandidateList
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.PatternRelationship
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.LogicalPlanContext
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.PlanTable

object optionalExpand extends CandidateGenerator[PlanTable] {

  def apply(planTable: PlanTable)(implicit context: LogicalPlanContext): CandidateList = {

    val outerJoinPlans: Seq[QueryPlan] = for {
      optionalQG <- context.queryGraph.optionalMatches
      lhs <- planTable.plans
      patternRel <- findSinglePatternRelationship(lhs.plan, optionalQG)
      argumentId = optionalQG.argumentIds.head
      otherSide = patternRel.otherSide( argumentId )
      if optionalQG.selections.predicatesGiven(lhs.coveredIds + otherSide + patternRel.name) == optionalQG.selections.flatPredicates
    } yield {
      val dir = patternRel.directionRelativeTo(argumentId)
      OptionalExpandPlan(lhs, argumentId, dir, patternRel.types, otherSide, patternRel.name, patternRel.length, optionalQG.selections.flatPredicates, optionalQG)
    }

    CandidateList(outerJoinPlans)
  }

  private def findSinglePatternRelationship(outerPlan: LogicalPlan, optionalQG: QueryGraph): Option[PatternRelationship] = {
    val singleArgument = optionalQG.argumentIds.size == 1
    val coveredByLHS = singleArgument && outerPlan.coveredIds.contains(optionalQG.argumentIds.head)
    val isSolved = (optionalQG.coveredIds -- outerPlan.coveredIds).isEmpty
    val hasOnlyOnePatternRelationship = optionalQG.patternRelationships.size == 1
    if (singleArgument && coveredByLHS && !isSolved && hasOnlyOnePatternRelationship) {
      optionalQG.patternRelationships.headOption
    } else {
      None
    }
  }
}
