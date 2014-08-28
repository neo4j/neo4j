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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_2.InputPosition.NONE
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.planner.{PlannerQuery, QueryGraph}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.QueryPlanProducer.planEndpointProjection
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{CandidateGenerator, CandidateList, LogicalPlanningContext, PlanTable}
import org.neo4j.cypher.internal.helpers.CollectionSupport

object projectEndpoints extends CandidateGenerator[PlanTable] with CollectionSupport {

  def apply(planTable: PlanTable, queryGraph: QueryGraph)(implicit context: LogicalPlanningContext): CandidateList = {
    val projectedEndpointPlans = for {
      plan <- planTable.plans
      patternRel <- queryGraph.patternRelationships
      if canProjectPatternRelationshipEndpoints(plan, patternRel)
    } yield {
      val (start, end) = patternRel.inOrder
      val (projectedStart, optStartPredicate) = projectAndSelectIfNecessary(plan.availableSymbols, start)
      val (projectedEnd, optEndPredicate) = projectAndSelectIfNecessary(plan.availableSymbols, end)
      val optRelPredicate = patternRel.types.asNonEmptyOption.map(hasType(patternRel.name))
      val predicates = Seq(optStartPredicate, optEndPredicate, optRelPredicate).flatten
      planEndpointProjection(plan, projectedStart, projectedEnd, predicates, patternRel)
    }
    CandidateList(projectedEndpointPlans)
  }

  private def projectAndSelectIfNecessary(inScope: Set[IdName], node: IdName): (IdName, Option[Expression]) =
    if (inScope(node)) {
      val projected = freshName(node)
      (projected, Some(areEqual(node, projected)))
    } else
      (node, None)


  private def hasType(rel: IdName)(relTypeNames: Seq[RelTypeName]): Expression = {
    In(
      FunctionInvocation(FunctionName("type")(NONE), Identifier(rel.name)(NONE))(NONE),
      Collection(relTypeNames.map(relType => StringLiteral(relType.name)(relType.position)))(NONE)
    )(NONE)
  }

  private def areEqual(left: IdName, right: IdName) = Equals(Identifier(left.name)(NONE), Identifier(right.name)(NONE))(NONE)

  private def freshName(idName: IdName) = IdName(idName.name + "$$$_")

  private def canProjectPatternRelationshipEndpoints(plan: QueryPlan, patternRel: PatternRelationship) = {
    val inScope = plan.availableSymbols(patternRel.name)
    val solved = plan.solved.graph.patternRelationships(patternRel)
    inScope && !solved
  }
}

