/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.compiler.v2_2.ast.{Identifier, FilterScope, AllIterablePredicate}
import org.neo4j.cypher.internal.compiler.v2_2.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer._

object expandTableSolver extends ExhaustiveTableSolver {

  override def apply(qg: QueryGraph, goal: Set[Solvable], table: (Set[Solvable]) => Option[LogicalPlan]): Iterator[LogicalPlan] = {
    val result =
      for (solvable <- goal.iterator;
           pattern <- solvable.solvedRelationship;
           solved = goal - solvable;
           plan <- table(solved)
      ) yield {
        Iterator(
          planSinglePatternSide(qg, pattern, plan, pattern.left),
          planSinglePatternSide(qg, pattern, plan, pattern.right)
        ).flatten
      }
      result.flatten
  }

  def planSinglePatternSide(qg: QueryGraph, patternRel: PatternRelationship, plan: LogicalPlan, nodeId: IdName): Option[LogicalPlan] = {
    val availableSymbols = plan.availableSymbols
    if (availableSymbols(nodeId)) {
      val dir = patternRel.directionRelativeTo(nodeId)
      val otherSide = patternRel.otherSide(nodeId)
      val overlapping = availableSymbols.contains(otherSide)
      val mode = if (overlapping) ExpandInto else ExpandAll

      patternRel.length match {
        case SimplePatternLength =>
          Some(planSimpleExpand(plan, nodeId, dir, otherSide, patternRel, mode))

        case length: VarPatternLength =>
          val availablePredicates = qg.selections.predicatesGiven(availableSymbols + patternRel.name)
          val (predicates, allPredicates) = availablePredicates.collect {
            case all@AllIterablePredicate(FilterScope(identifier, Some(innerPredicate)), relId@Identifier(patternRel.name.name))
              if identifier == relId || !innerPredicate.dependencies(relId) =>
              (identifier, innerPredicate) -> all
          }.unzip
          Some(planVarExpand(plan, nodeId, dir, otherSide, patternRel, predicates, allPredicates, mode))
      }
    } else {
      None
    }
  }

}
