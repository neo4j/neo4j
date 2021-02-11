/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.Create
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.LogicalBinaryPlan
import org.neo4j.cypher.internal.logical.plans.LogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalUnaryPlan
import org.neo4j.cypher.internal.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.helpers.fixedPoint
import org.neo4j.cypher.internal.util.topDown

case class unnestApply(solveds: Solveds, cardinalities: Cardinalities, attributes: Attributes[LogicalPlan]) extends Rewriter {

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
    OEX: Optional Expand
    LOJ: Left Outer Join
    ROJ: Right Outer Join
    CN : CreateNode
    FE : Foreach
    UP : Unary Plan
    BP : Binary Plan
   */

  private val instance: Rewriter = topDown(Rewriter.lift {
    // Arg Ax R => R
    case Apply(arg: Argument, rhs) =>
      assertArgumentHasCardinality1(arg)
      rhs

    // L Ax Arg => L
    case Apply(lhs, _: Argument) =>
      lhs

    // L Ax (Arg FE R) => L FE R
    case apply@Apply(lhs, foreach@ForeachApply(arg: Argument, _, _, _)) =>
      assertArgumentHasCardinality1(arg)
      unnestRightBinaryLeft(apply, lhs, foreach)

    // L Ax (σ R) => σ(L Ax R)
    case apply@Apply(lhs, sel:Selection) =>
      unnestRightUnary(apply, lhs, sel)

    // L Ax ((σ L2) Ax R) => (σ L) Ax (L2 Ax R) iff σ does not have dependencies on L
    case original@Apply(lhs, Apply(sel@Selection(predicate, lhs2), rhs))
      if predicate.exprs.forall(lhs.satisfiesExpressionDependencies) =>
      val maybeSelectivity = cardinalities(sel.id) / cardinalities(lhs2.id)
      val selectionLHS = Selection(predicate, lhs)(attributes.copy(sel.id))
      solveds.copy(original.id, selectionLHS.id)
      cardinalities.set(selectionLHS.id, maybeSelectivity.fold(cardinalities(sel.id))(cardinalities(lhs.id) * _))

      val apply2 = Apply(lhs2, rhs)(attributes.copy(lhs.id))
      solveds.copy(original.id, apply2.id)
      cardinalities.set(apply2.id, cardinalities(lhs2.id) * cardinalities(rhs.id))

      Apply(selectionLHS, apply2)(SameId(original.id))

    // L Ax (π R) => π(L Ax R)
    case apply@Apply(lhs, p:Projection) =>
      unnestRightUnary(apply, lhs, p)

    // L Ax (EXP R) => EXP( L Ax R ) (for single step pattern relationships)
    case apply@Apply(lhs, expand: Expand) =>
      unnestRightUnary(apply, lhs, expand)

    // L Ax (EXP R) => EXP( L Ax R ) (for varlength pattern relationships)
    case apply@Apply(lhs, expand: VarExpand) =>
      unnestRightUnary(apply, lhs, expand)

    // L Ax (Arg LOJ R) => L LOJ R
    case apply@Apply(lhs, join@LeftOuterHashJoin(_, arg:Argument, _)) =>
      assertArgumentHasCardinality1(arg)
      unnestRightBinaryLeft(apply, lhs, join)

    // L Ax (L2 ROJ Arg) => L2 ROJ L
    case apply@Apply(lhs, join@RightOuterHashJoin(_, _, arg:Argument)) =>
      assertArgumentHasCardinality1(arg)
      unnestRightBinaryRight(apply, lhs, join)

    // L Ax (OEX Arg) => OEX L
    case apply@Apply(lhs, oex@OptionalExpand(_:Argument, _, _, _, _, _, _, _)) =>
      unnestRightUnary(apply, lhs, oex)

    // L Ax (Cr R) => Cr (L Ax R)
    case apply@Apply(lhs, create:Create) =>
      unnestRightUnary(apply, lhs, create)

    // π (Arg) Ax R => π (R) // if R is leaf and R is not using columns from π
    case apply@Apply(projection@Projection(Argument(_), projections), rhsLeaf: LogicalLeafPlan)
      if !projections.keys.exists(rhsLeaf.usedVariables.contains) =>
      val rhsCopy = rhsLeaf.withoutArgumentIds(projections.keySet)
      val res = projection.copy(rhsCopy, projections)(attributes.copy(projection.id))
      solveds.copy(projection.id, res.id)
      cardinalities.copy(apply.id, res.id)
      res
  })

  // L Ax (UP R) => UP (L Ax R)
  private def unnestRightUnary(apply: Apply, lhs: LogicalPlan, rhs: LogicalUnaryPlan): LogicalPlan = {
    val newApply = apply.copy(right = rhs.source)(attributes.copy(apply.id))
    solveds.copy(apply.id, newApply.id)
    cardinalities.set(newApply.id, cardinalities(lhs.id) * cardinalities(rhs.source.id))

    val res = rhs.withLhs(newApply)(attributes.copy(rhs.id))
    solveds.copy(apply.id, res.id)
    cardinalities.set(res.id, cardinalities(lhs.id) * cardinalities(rhs.id))
    res
  }

  // L Ax (_ BP R) => L BP R
  private def unnestRightBinaryLeft(apply: Apply, lhs: LogicalPlan, rhs: LogicalBinaryPlan): LogicalPlan = {
    val res = rhs.withLhs(lhs)(attributes.copy(rhs.id))
    solveds.copy(apply.id, res.id)
    cardinalities.copy(apply.id, res.id)
    res
  }

  // L Ax (L2 BP _) => L2 BP L
  private def unnestRightBinaryRight(apply: Apply, lhs: LogicalPlan, rhs: LogicalBinaryPlan): LogicalPlan = {
    val res = rhs.withRhs(lhs)(attributes.copy(rhs.id))
    solveds.copy(apply.id, res.id)
    cardinalities.copy(apply.id, res.id)
    res
  }

  private def assertArgumentHasCardinality1(arg: Argument): Unit = {
    // Argument plans are always supposed to have a Cardinality of 1.
    // If this should not hold, we would need to multiply Cardinality for this rewrite rule.
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(cardinalities(arg.id) == Cardinality.SINGLE, s"Argument plans should always have Cardinality 1. Had: ${cardinalities(arg.id)}")
  }

  override def apply(input: AnyRef): AnyRef = fixedPoint(instance).apply(input)
}
