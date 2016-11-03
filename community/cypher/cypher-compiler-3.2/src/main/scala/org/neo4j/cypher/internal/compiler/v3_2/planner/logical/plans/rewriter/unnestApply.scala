/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_2.{Rewriter, topDown}

case object unnestApply extends Rewriter {

  /*
  Based on the paper
  Parameterized Queries and Nesting Equivalences by C. A. Galindo-Legaria

  Glossary:
    Ax : Apply
    L,R: Arbitrary operator, named Left and Right
    σ  : Selection
    π  : Projection
    Arg: Argument
    EXP: Expand
    LOJ: Left Outer Join
    SR : SingleRow - operator that produces single row with no columns
    CN : CreateNode
    FE : Foreach
   */

  private val instance: Rewriter = topDown(Rewriter.lift {
    // SR Ax R => R
    case Apply(_: SingleRow, rhs) =>
      rhs

    // Arg Ax R => R
    case Apply(_: Argument, rhs) =>
      rhs

    // L Ax Arg => L
    case Apply(lhs, _: Argument) =>
      lhs

    // L Ax SR => L
    case Apply(lhs, _: SingleRow) =>
      lhs

    // L Ax (Arg Ax R) => L Ax R
    case original@Apply(lhs, Apply(_: Argument, rhs)) =>
      Apply(lhs, rhs)(original.solved)

    // L Ax (Arg FE R) => L FE R
    case original@Apply(lhs, foreach@ForeachApply(_: Argument, rhs, _, _)) =>
      foreach.copy(left = lhs, right = rhs)(original.solved)

    // L Ax (SR FE R) => L FE R
    case original@Apply(lhs, foreach@ForeachApply(_: SingleRow, rhs, _, _)) =>
      foreach.copy(left = lhs, right = rhs)(original.solved)

    // L Ax (Arg Ax R) => L Ax R
    case original@AntiConditionalApply(lhs, Apply(_: Argument, rhs), _) =>
      original.copy(lhs, rhs)(original.solved)

    // L Ax (σ R) => σ(L Ax R)
    case o@Apply(lhs, sel@Selection(predicates, rhs)) =>
      Selection(predicates, Apply(lhs, rhs)(o.solved))(o.solved)

    // L Ax ((σ L2) Ax R) => (σ L) Ax (L2 Ax R) iff σ does not have dependencies on L
    case original@Apply(lhs, Apply(sel@Selection(predicates, lhs2), rhs))
      if predicates.forall(lhs.satisfiesExpressionDependencies)=>
      val selectionLHS = Selection(predicates, lhs)(original.solved)
      val apply2 = Apply(lhs2, rhs)(original.solved)
      Apply(selectionLHS, apply2)(original.solved)

    // L Ax (π R) => π(L Ax R)
    case origApply@Apply(lhs, p@Projection(rhs, _)) =>
      val newApply = Apply(lhs, rhs)(origApply.solved)
      p.copy(left = newApply)(origApply.solved)

    // L Ax (EXP R) => EXP( L Ax R ) (for single step pattern relationships)
    case apply@Apply(lhs, expand: Expand) =>
      val newApply = apply.copy(right = expand.left)(apply.solved)
      expand.copy(left = newApply)(apply.solved)

    // L Ax (EXP R) => EXP( L Ax R ) (for varlength pattern relationships)
    case apply@Apply(lhs, expand: VarExpand) =>
      val newApply = apply.copy(right = expand.left)(apply.solved)
      expand.copy(left = newApply)(apply.solved)

    // L Ax (Arg LOJ R) => L LOJ R
    case apply@Apply(lhs, join@OuterHashJoin(_, _:Argument, rhs)) =>
      join.copy(left = lhs)(apply.solved)

    // L Ax (CN R) => CN Ax (L R)
    case apply@Apply(lhs, create@CreateNode(rhs, name, labels, props)) =>
      CreateNode(Apply(lhs, rhs)(apply.solved), name, labels, props)(apply.solved)
  })

  override def apply(input: AnyRef) = instance.apply(input)
}
