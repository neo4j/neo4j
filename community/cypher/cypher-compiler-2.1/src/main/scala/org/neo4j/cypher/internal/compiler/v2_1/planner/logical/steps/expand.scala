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

import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.SimpleLogicalPlanner._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.{CandidateList, LogicalPlanContext, PlanTable}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.{LogicalPlan, Selection, IdName, Expand}
import org.neo4j.cypher.internal.compiler.v2_1.ast.{RelTypeName, Identifier, Equals}
import org.neo4j.graphdb.Direction

object expand extends PlanCandidateGenerator {
  def apply(inputPlan: PlanTable)(implicit context: LogicalPlanContext): CandidateList = {
    val expandPlans = for {
      plan <- inputPlan.plans
      nodeId <- plan.coveredIds
      patternRel <- context.queryGraph.findRelationshipsEndingOn(nodeId)
      if !plan.coveredIds(patternRel.name)
      dir = patternRel.directionRelativeTo(nodeId)
    } yield {
      val otherSide = patternRel.otherSide(nodeId)
      if (plan.coveredIds.contains(otherSide)) {
        expandAndCheck(plan, nodeId, patternRel.types, dir, patternRel.name, otherSide.name)
      }
      else
        Expand(plan, nodeId, dir, patternRel.types, otherSide, patternRel.name)
    }

    CandidateList(expandPlans)
  }

  /*
  If we are expanding into an identifier already in scope, we instead expand into a temporary identifier, and then check
  that we expanded into the correct node.

  Example:
  MATCH (a)-[r]->(a)

  is solved by something that looks like
  MATCH (a)-[r]->($TEMP) WHERE $TEMP = a
   */
  private def expandAndCheck(source: LogicalPlan,
                             fromNode: IdName,
                             types: Seq[RelTypeName],
                             dir: Direction,
                             relName: IdName,
                             otherSide: String)(implicit context: LogicalPlanContext): LogicalPlan = {
    val temp = IdName(otherSide + "$$$")
    val expand = Expand(source, fromNode, dir, types, temp, relName)
    val left = Identifier(otherSide)(null)
    val right = Identifier(temp.name)(null)
    Selection(Seq(Equals(left, right)(null)), expand)
  }
}
