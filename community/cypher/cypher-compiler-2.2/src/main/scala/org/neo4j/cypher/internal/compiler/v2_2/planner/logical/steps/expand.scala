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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer._

object expand extends CandidateGenerator[PlanTable] {
  def apply(planTable: PlanTable, queryGraph: QueryGraph)(implicit context: LogicalPlanningContext): Seq[LogicalPlan] = {
    for {
      plan <- planTable.plans
      nodeId <- plan.solved.graph.patternNodes
      patternRel <- queryGraph.findRelationshipsEndingOn(nodeId)
      if !plan.availableSymbols(patternRel.name)
    } yield {
      val dir = patternRel.directionRelativeTo(nodeId)
      val otherSide = patternRel.otherSide(nodeId)
      val overlapping = plan.availableSymbols.contains(otherSide)

      patternRel.length match {
        case SimplePatternLength =>
          val mode = if (overlapping) ExpandInto else ExpandAll
          planSimpleExpand(plan, nodeId, dir, otherSide, patternRel, mode)

        case length: VarPatternLength =>
          val availablePredicates = queryGraph.selections.predicatesGiven(plan.availableSymbols + patternRel.name)
          val (predicates, allPredicates) = availablePredicates.collect {
            case all@AllIterablePredicate(FilterScope(identifier, Some(innerPredicate)), relId@Identifier(patternRel.name.name))
              if identifier == relId || !innerPredicate.dependencies(relId) =>
              (identifier, innerPredicate) -> all
          }.unzip

          val expandF =
            (otherSide: IdName) =>
              planVarExpand(plan, nodeId, dir, otherSide, patternRel, predicates, allPredicates, ExpandAll)

          if (overlapping)
            expandIntoAlreadyExistingNode(expandF, otherSide)
          else
            expandF(otherSide)
      }
    }
  }

  /*
  If we are expanding into an identifier already in scope, we instead expand into a temporary identifier, and then check
  that we expanded into the correct node.

  Example:
  MATCH (a)-[r]->(a)

  is solved by something that looks like
  MATCH (a)-[r]->($TEMP) WHERE $TEMP = a
   */
  private def expandIntoAlreadyExistingNode(f: IdName => LogicalPlan, otherSide: IdName)
                                           (implicit context: LogicalPlanningContext): LogicalPlan = {
    val temp = IdName(otherSide.name + "$$$")
    val expand = f(temp)
    val left = Identifier(otherSide.name)(null)
    val right = Identifier(temp.name)(null)
    planHiddenSelection(Seq(Equals(left, right)(null)), expand)
  }
}
