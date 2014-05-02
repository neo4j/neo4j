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

import org.neo4j.cypher.internal.compiler.v2_1.ast.Equals
import org.neo4j.cypher.internal.compiler.v2_1.ast.Identifier
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.{CandidateList, PlanTable, LogicalPlanContext, CandidateGenerator}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.CandidateList
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Selection
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_1.ast.Equals
import org.neo4j.cypher.internal.compiler.v2_1.ast.Identifier
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.LogicalPlanContext
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Expand

object expand extends CandidateGenerator[PlanTable] {
  def apply(planTable: PlanTable)(implicit context: LogicalPlanContext): CandidateList = {
    val expandPlans = for {
      plan <- planTable.plans
      nodeId <- plan.coveredIds
      patternRel <- context.queryGraph.findRelationshipsEndingOn(nodeId)
      if !plan.solved.patternRelationships(patternRel)
    } yield {
      val dir = patternRel.directionRelativeTo(nodeId)
      val otherSide = patternRel.otherSide(nodeId)
      val expandF = (otherSide: IdName) => Expand(plan.plan, nodeId, dir, patternRel.types,
                                                  otherSide, patternRel.name, patternRel.length)(patternRel)

      if (plan.coveredIds.contains(otherSide)) {
        expandIntoAlreadyExistingNode(expandF, otherSide)
      }
      else
        expandF(otherSide)
    }
    CandidateList(expandPlans.map(QueryPlan))
  }

  /*
  If we are expanding into an identifier already in scope, we instead expand into a temporary identifier, and then check
  that we expanded into the correct node.

  Example:
  MATCH (a)-[r]->(a)

  is solved by something that looks like
  MATCH (a)-[r]->($TEMP) WHERE $TEMP = a
   */
  private def expandIntoAlreadyExistingNode(f: IdName => Expand, otherSide: IdName)
                                           (implicit context: LogicalPlanContext): LogicalPlan = {
    val temp = IdName(otherSide.name + "$$$")
    val expand = f(temp)
    val left = Identifier(otherSide.name)(null)
    val right = Identifier(temp.name)(null)
    Selection(Seq(Equals(left, right)(null)), expand, hideSelections = true)
  }
}
