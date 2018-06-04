/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.Solveds
import org.opencypher.v9_0.util.attribution.{Attributes, SameId}
import org.opencypher.v9_0.util.{Rewriter, topDown}
import org.neo4j.cypher.internal.v3_5.logical.plans._

case class unnestApply(solveds: Solveds, attributes: Attributes) extends Rewriter {

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
    ROJ: Right Outer Join
    CN : CreateNode
    FE : Foreach
   */

  private val instance: Rewriter = topDown(Rewriter.lift {
    // Arg Ax R => R
    case Apply(_: Argument, rhs) =>
      rhs

    // L Ax Arg => L
    case Apply(lhs, _: Argument) =>
      lhs

    // L Ax (Arg Ax R) => L Ax R
    case original@Apply(lhs, Apply(_: Argument, rhs)) =>
      Apply(lhs, rhs)(SameId(original.id))

    // L Ax (Arg FE R) => L FE R
    case original@Apply(lhs, foreach@ForeachApply(_: Argument, rhs, _, _)) =>
      val res = foreach.copy(left = lhs, right = rhs)(attributes.copy(foreach.id))
      solveds.copy(original.id, res.id)
      res

    // L Ax (Arg Ax R) => L Ax R
    case original@AntiConditionalApply(lhs, Apply(_: Argument, rhs), _) =>
      original.copy(lhs, rhs)(SameId(original.id))

    // L Ax (σ R) => σ(L Ax R)
    case o@Apply(lhs, sel@Selection(predicates, rhs)) =>
      val res = Selection(predicates, Apply(lhs, rhs)(SameId(o.id)))(attributes.copy(sel.id))
      solveds.copy(o.id, res.id)
      res

    // L Ax ((σ L2) Ax R) => (σ L) Ax (L2 Ax R) iff σ does not have dependencies on L
    case original@Apply(lhs, Apply(sel@Selection(predicates, lhs2), rhs))
      if predicates.forall(lhs.satisfiesExpressionDependencies)=>
      val selectionLHS = Selection(predicates, lhs)(attributes.copy(sel.id))
      solveds.copy(original.id, selectionLHS.id)
      val apply2 = Apply(lhs2, rhs)(attributes.copy(lhs.id))
      solveds.copy(original.id, apply2.id)
      Apply(selectionLHS, apply2)(SameId(original.id))

    // L Ax (π R) => π(L Ax R)
    case origApply@Apply(lhs, p@Projection(rhs, _)) =>
      val newApply = Apply(lhs, rhs)(SameId(origApply.id))
      val res = p.copy(source = newApply)(attributes.copy(p.id))
      solveds.copy(origApply.id, res.id)
      res

    // L Ax (EXP R) => EXP( L Ax R ) (for single step pattern relationships)
    case apply@Apply(lhs, expand: Expand) =>
      val newApply = apply.copy(right = expand.source)(SameId(apply.id))
      val res = expand.copy(source = newApply)(attributes.copy(expand.id))
      solveds.copy(apply.id, res.id)
      res

    // L Ax (EXP R) => EXP( L Ax R ) (for varlength pattern relationships)
    case apply@Apply(lhs, expand: VarExpand) =>
      val newApply = apply.copy(right = expand.source)(SameId(apply.id))
      val res = expand.copy(source = newApply)(attributes.copy(expand.id))
      solveds.copy(apply.id, res.id)
      res

    // L Ax (Arg LOJ R) => L LOJ R
    case apply@Apply(lhs, join@LeftOuterHashJoin(_, _:Argument, _)) =>
      val res = join.copy(left = lhs)(attributes.copy(join.id))
      solveds.copy(apply.id, res.id)
      res

    // L Ax (L2 ROJ Arg) => L2 ROJ L
    case apply@Apply(lhs, join@RightOuterHashJoin(_, _, _:Argument)) =>
      val res = join.copy(right = lhs)(attributes.copy(join.id))
      solveds.copy(apply.id, res.id)
      res

    // L Ax (Cr R) => Cr Ax (L R)
    case apply@Apply(lhs, create@Create(rhs, nodes, relationships)) =>
      val res = Create(Apply(lhs, rhs)(SameId(apply.id)), nodes, relationships)(attributes.copy(create.id))
      solveds.copy(apply.id, res.id)
      res
  })

  override def apply(input: AnyRef): AnyRef = instance.apply(input)
}
