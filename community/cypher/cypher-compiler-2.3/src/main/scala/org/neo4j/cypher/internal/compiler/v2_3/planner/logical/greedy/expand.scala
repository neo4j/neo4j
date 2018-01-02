/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.greedy

import org.neo4j.cypher.internal.frontend.v2_3.ast.{AllIterablePredicate, FilterScope, Identifier}
import org.neo4j.cypher.internal.compiler.v2_3.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{CandidateGenerator, LogicalPlanningContext}

object expand extends CandidateGenerator[GreedyPlanTable] {
  def apply(planTable: GreedyPlanTable, queryGraph: QueryGraph)(implicit context: LogicalPlanningContext): Seq[LogicalPlan] = {
    for {
      plan <- planTable.plans
      nodeId <- plan.solved.graph.patternNodes
      patternRel <- queryGraph.findRelationshipsEndingOn(nodeId)
      if !plan.availableSymbols(patternRel.name)
    } yield {
      val dir = patternRel.directionRelativeTo(nodeId)
      val otherSide = patternRel.otherSide(nodeId)
      val overlapping = plan.availableSymbols.contains(otherSide)
      val mode = if (overlapping) ExpandInto else ExpandAll

      patternRel.length match {
        case SimplePatternLength =>
          context.logicalPlanProducer.planSimpleExpand(plan, nodeId, dir, otherSide, patternRel, mode)

        case length: VarPatternLength =>
          val availablePredicates = queryGraph.selections.predicatesGiven(plan.availableSymbols + patternRel.name)
          val (predicates, allPredicates) = availablePredicates.collect {
            case all@AllIterablePredicate(FilterScope(identifier, Some(innerPredicate)), relId@Identifier(patternRel.name.name))
              if identifier == relId || !innerPredicate.dependencies(relId) =>
              (identifier, innerPredicate) -> all
          }.unzip
          context.logicalPlanProducer.planVarExpand(plan, nodeId, dir, otherSide, patternRel, predicates, allPredicates, mode)
      }
    }
  }
}
