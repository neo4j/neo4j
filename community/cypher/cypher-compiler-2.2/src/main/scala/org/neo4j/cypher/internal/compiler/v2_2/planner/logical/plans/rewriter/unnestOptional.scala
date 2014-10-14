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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.planner.{Selections, PlannerQuery}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._

case object unnestOptional extends Rewriter {
  private def applicable(outerPlan:LogicalPlan, optional: Optional) = {
    val qg = optional.inputPlan.solved.graph
    val singleArgumentAvailable = qg.argumentIds.size == 1 && outerPlan.availableSymbols(qg.argumentIds.head) && qg.patternNodes(qg.argumentIds.head)
    qg.patternRelationships.size == 1 && singleArgumentAvailable
  }
  private def canSolveAllPredicates(selections:Selections, ids:Set[IdName]) = selections.predicatesGiven(ids) == selections.flatPredicates

  private val instance: Rewriter = Rewriter.lift {


    case apply@Apply(lhs,
      Optional(
      Expand(_:SingleRow, from, dir, _, types, to, relName, length, _))) =>
          OptionalExpand(lhs, from, dir, types, to, relName, length, Seq.empty)(apply.solved)

    case apply@Apply(lhs, optional:Optional) if (applicable(lhs, optional)) =>
      val qg = optional.inputPlan.solved.graph
      val patternRel = qg.patternRelationships.head
      val argumentId = qg.argumentIds.head
      val dir = patternRel.directionRelativeTo(argumentId)
      val otherSide = patternRel.otherSide(argumentId)

      if (canSolveAllPredicates(qg.selections, lhs.availableSymbols + otherSide + patternRel.name)) {
        OptionalExpand(lhs, argumentId, dir, patternRel.types, otherSide, patternRel.name, patternRel.length,
          qg.selections.flatPredicates)(lhs.solved.updateGraph(_.withAddedOptionalMatch(qg)))
      } else {
        apply
      }
  }

  override def apply(input: AnyRef) = bottomUp(instance).apply(input)
}
