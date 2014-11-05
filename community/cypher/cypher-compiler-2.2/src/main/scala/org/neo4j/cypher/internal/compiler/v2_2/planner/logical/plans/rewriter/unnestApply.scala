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
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import Foldable._

case object unnestApply extends Rewriter {

  /*
  Glossary:
    Ax : Apply
    L,R: Arbitrary operator, named Left and Right
    σ  : Selection
    π  : Projection
    Arg: Argument
    EXP: Expand
    LOJ: Left Outer Join
   */

  private val instance: Rewriter = Rewriter.lift {
    // Arg0 Ax R => R iff Arg0 introduces no arguments
    case Apply(sr: SingleRow, rhs) if sr.argumentIds.isEmpty =>
      rhs

    // L Ax Arg => L
    case Apply(lhs, _: SingleRow) =>
      lhs

    // L Ax (Arg Ax R) => L Ax R
    case original@Apply(lhs, Apply(_: SingleRow, rhs)) =>
      Apply(lhs, rhs)(original.solved)

    // L Ax (σ R) => σ(L Ax R)
    case o@Apply(lhs, sel@Selection(predicates, rhs)) =>
      Selection(predicates, Apply(lhs, rhs)(o.solved))(o.solved)

    // L Ax (π R) => π(L Ax R)
    case origApply@Apply(lhs, p@Projection(rhs, _)) =>
      val newApply = Apply(lhs, rhs)(origApply.solved)
      p.copy(left = newApply)(origApply.solved)

    // L Ax (EXP R) => EXP( L Ax R )
    case apply@Apply(lhs, expand@Expand(rhs, _, _, _, _, _, _, _, _)) =>
      val newApply = apply.copy(right = rhs)(apply.solved)
      expand.copy(left = newApply)(apply.solved)

    // L Ax (Arg LOJ R) => L LOJ R
    case apply@Apply(lhs, join@OuterHashJoin(_, _:SingleRow, rhs)) =>
      join.copy(left = lhs)(apply.solved)
  }

  override def apply(input: AnyRef) = bottomUp(instance).apply(input)
}
