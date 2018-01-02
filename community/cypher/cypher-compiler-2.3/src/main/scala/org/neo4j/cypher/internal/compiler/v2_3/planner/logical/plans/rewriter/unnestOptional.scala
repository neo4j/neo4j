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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.frontend.v2_3.ast.Expression
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_3.planner.{CardinalityEstimation, PlannerQuery}
import org.neo4j.cypher.internal.frontend.v2_3.{Rewriter, bottomUp}

case object unnestOptional extends Rewriter {

  def apply(input: AnyRef) = bottomUp(instance).apply(input)

  private val instance: Rewriter = Rewriter.lift {
    case apply@Apply(lhs,
      Optional(
      e@Expand(_: Argument, _, _, _, _, _, _))) =>
        optionalExpand(e, lhs)(Seq.empty)(apply.solved)

    case apply@Apply(lhs,
      Optional(
      Selection(predicates,
      e@Expand(_: Argument, _, _, _, _, _, _)))) =>
        optionalExpand(e, lhs)(predicates)(apply.solved)
  }

  private def optionalExpand(e: Expand, lhs: LogicalPlan): (Seq[Expression] => PlannerQuery with CardinalityEstimation => OptionalExpand) =
    predicates => OptionalExpand(lhs, e.from, e.dir, e.types, e.to, e.relName, e.mode, predicates)
}
