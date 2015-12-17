/*
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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_0.{Rewriter, bottomUp}

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
    SR : SingleRow - operator that produces single row with no columns
    CN : CreateNode
    D : Delete
    E : Eager
    M : Merge
   */

  private val instance: Rewriter = Rewriter.lift {
//    case x => x
    // SR Ax R => R iff Arg0 introduces no arguments
    case Apply(_: SingleRow, rhs) =>
      rhs

    // L Ax Arg => L
    case Apply(lhs, _: Argument) =>
      lhs

    // L Ax (Arg Ax R) => L Ax R
    case original@Apply(lhs, Apply(_: Argument, rhs)) =>
      Apply(lhs, rhs)(original.solved)

    // L Ax (σ R) => σ(L Ax R)
    case o@Apply(lhs, sel@Selection(predicates, rhs)) =>
      Selection(predicates, Apply(lhs, rhs)(o.solved))(o.solved)

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

    // L Ax (D R) => D Ax (L R)
    case apply@Apply(lhs, delete@DeleteNode(rhs, expr)) =>
      DeleteNode(Apply(lhs, rhs)(apply.solved), expr)(apply.solved)

    // L Ax (M R) => M Ax (L R)
    case apply@Apply(lhs, merge@AsMergePlan(source, sourceReplacer)) =>
      sourceReplacer(Apply(lhs, source)(apply.solved))

    // L Ax (E R) => E Ax (L R)
    case apply@Apply(lhs, eager@Eager(rhs)) =>
      Eager(Apply(lhs, rhs)(apply.solved))(apply.solved)
  }

  override def apply(input: AnyRef) = bottomUp(instance).apply(input)
}

object AsMergePlan {
  def unapply(v: Any): Option[(LogicalPlan, LogicalPlan => LogicalPlan)] = v match {
      // Full merge plan pattern
    case topMergePlan@AntiConditionalApply(condApply@ConditionalApply(apply@Apply(lhs, mergeRead), onMatch, name1), mergeCreate, name2) =>
      Some(lhs, (plan: LogicalPlan) => AntiConditionalApply(ConditionalApply(Apply(plan, mergeRead)(apply.solved), onMatch, name1)(condApply.solved), mergeCreate, name2)(topMergePlan.solved))
      // Merge plan pattern when there is no ON MATCH part
    case topMergePlan@AntiConditionalApply(apply@Apply(lhs, mergeRead), mergeCreate, name) =>
      Some(lhs, (plan: LogicalPlan) => AntiConditionalApply(Apply(plan, mergeRead)(apply.solved), mergeCreate, name)(topMergePlan.solved))
    case _ =>
      None
  }
}

