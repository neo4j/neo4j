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
import org.neo4j.cypher.internal.logical.plans.ApplyPlan
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.LogicalBinaryPlan
import org.neo4j.cypher.internal.logical.plans.LogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalUnaryPlan
import org.neo4j.cypher.internal.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.helpers.fixedPoint
import org.neo4j.cypher.internal.util.topDown

import scala.annotation.tailrec

case class unnestApply(override val solveds: Solveds,
                       override val cardinalities: Cardinalities,
                       override val providedOrders: ProvidedOrders,
                       override val attributes: Attributes[LogicalPlan]) extends Rewriter with UnnestingRewriter {

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
    CN : CreateNode
    FE : Foreach
    UP : Unary Plan
    BP : Binary Plan
   */

  private val instance: Rewriter = topDown(Rewriter.lift {
    // Arg Ax R => R
    case RemovableApply(arg: Argument, rhs, _) if arg.argumentIds.subsetOf(rhs.availableSymbols) =>
      assertArgumentHasCardinality1(arg)
      rhs

    // L Ax Arg => L
    case RemovableApply(lhs, _: Argument, _) =>
      lhs

    // L Ax (Arg FE R) => L FE R
    case apply@RemovableApply(lhs, foreach@ForeachApply(arg: Argument, _, _, _), _) =>
      assertArgumentHasCardinality1(arg)
      unnestRightBinaryLeft(apply, lhs, foreach)

    // L Ax (σ R) => σ(L Ax R)
    // L Ax (π R) => π(L Ax R)
    // L Ax (EXP R) => EXP( L Ax R ) (for single step pattern relationships)
    // L Ax (EXP R) => EXP( L Ax R ) (for varlength pattern relationships)
    case apply@Apply(lhs, UnnestableUnaryPlan(p), _) =>
      unnestRightUnary(apply, lhs, p)

    // L Ax ((σ L2) Ax R) => (σ L) Ax (L2 Ax R) iff σ does not have dependencies on L2
    case original@Apply(lhs, Apply(sel@Selection(predicate, lhs2), rhs, isSubquery1), isSubquery2)
      if predicate.exprs.forall(lhs.satisfiesExpressionDependencies) =>
      val maybeSelectivity = cardinalities(sel.id) / cardinalities(lhs2.id)
      val selectionLHS = Selection(predicate, lhs)(attributes.copy(sel.id))
      solveds.copy(original.id, selectionLHS.id)
      cardinalities.set(selectionLHS.id, maybeSelectivity.fold(cardinalities(sel.id))(cardinalities(lhs.id) * _))
      providedOrders.copy(lhs.id, selectionLHS.id)

      val apply2 = Apply(lhs2, rhs, isSubquery1)(attributes.copy(lhs.id))
      solveds.copy(original.id, apply2.id)
      cardinalities.set(apply2.id, cardinalities(lhs2.id) * cardinalities(rhs.id))
      providedOrders.copy(lhs2.id, apply2.id)

      Apply(selectionLHS, apply2, isSubquery2)(SameId(original.id))

    // L Ax (OEX Arg) => OEX (L Ax Arg)
    case apply@Apply(lhs, oex@OptionalExpand(_:Argument, _, _, _, _, _, _, _), _) =>
      unnestRightUnary(apply, lhs, oex)


    // π (Arg) Ax R => π (R) // if projections are simple and R is leaf and R is not using columns from π
    case apply@RemovableApply(projection@Projection(Argument(_), projections), rhsLeaf: LogicalLeafPlan, _)
      if !projections.keys.exists(rhsLeaf.usedVariables.contains) && projections.values.forall(_.isSimple) =>
      val rhsCopy = rhsLeaf.withoutArgumentIds(projections.keySet)
      val res = projection.copy(rhsCopy, projections)(attributes.copy(projection.id))
      solveds.copy(projection.id, res.id)
      cardinalities.copy(apply.id, res.id)
      providedOrders.copy(rhsLeaf.id, res.id)
      res

    // L Ax (L2 Ax R) => (L2 (L)) Ax R iff L2 isUnnestableUnaryPlanTree
    case apply@Apply(lhs1, Apply(lhs2, rhs2, fromSubquery2), fromSubquery1) if isUnnestableUnaryPlanTree(lhs2) =>
      val res = Apply(putOnTopOf(lhs1, lhs2), rhs2, fromSubquery1 || fromSubquery2)(attributes.copy(apply.id))
      solveds.copy(apply.id, res.id)
      cardinalities.set(res.id, cardinalities(apply.id))
      providedOrders.copy(apply.id, res.id)
      res

    // L Ax (L2 Ax R) => (L2 (L)) Ax R iff L2 isUnnestableUnaryPlanTree && Ax.fromSubquery = false
    case apply@RemovableApply(lhs1, innerApplyPlan@ApplyPlan(lhs2, _), _) if isUnnestableUnaryPlanTree(lhs2) =>
      val res = innerApplyPlan.withLhs(putOnTopOf(lhs1, lhs2))(attributes.copy(apply.id))
      solveds.copy(apply.id, res.id)
      cardinalities.set(res.id, cardinalities(apply.id))
      providedOrders.copy(apply.id, res.id)
      res
  })

  private def putOnTopOf(bottom: LogicalPlan, top: LogicalPlan): LogicalPlan = {
    top match {
      case _:Argument => bottom
      case p:LogicalUnaryPlan =>
        val inner = putOnTopOf(bottom, p.source)
        val res = p.withLhs(inner)(attributes.copy(p.id))
        solveds.copy(p.id, res.id)
        cardinalities.set(res.id, cardinalities(p.id) * cardinalities(bottom.id))
        providedOrders.copy(bottom.id, res.id)
        res
    }
  }

  @tailrec
  private def isUnnestableUnaryPlanTree(plan: LogicalPlan): Boolean = {
    plan match {
      case UnnestableUnaryPlan(p) => isUnnestableUnaryPlanTree(p.source)
      case _: Argument => true
      case _ => false
    }
  }

  override def apply(input: AnyRef): AnyRef = fixedPoint(instance).apply(input)
}

object UnnestableUnaryPlan {
  /**
   * Plans whose behavior (result) would not change, if they were moved from RHS of Apply to LHS of same Apply.
   * E.g., Distinct is NOT unnestable because on RHS of Apply it returns distinct rows PER ARGUMENT, where on LHS they are globally distinct.
   */
  def unapply(p: LogicalPlan): Option[LogicalUnaryPlan] = p match {
    case p: Selection => Some(p)
    case p: Projection => Some(p)
    case p: Expand => Some(p)
    case p: VarExpand => Some(p)
    case _ => None
  }
}

object RemovableApply {
  /**
   * An 'Apply' is removable if 'fromSubquery' is false.
   * When 'fromSubquery' is true, the 'Apply' can only be _replaced_ with another 'Apply' that preserves that 'fromSubquery' is true.
   */
  def unapply(v: Apply): Option[(LogicalPlan, LogicalPlan, Boolean)] = v match {
    case Apply(lhs, rhs, fromSubquery) if !fromSubquery => Some((lhs, rhs, fromSubquery))
    case Apply(lhs, rhs: Argument, fromSubquery) => Some((lhs, rhs, fromSubquery))
    case _ => None
  }
}

trait UnnestingRewriter {
  def solveds: Solveds
  def cardinalities: Cardinalities
  def providedOrders: ProvidedOrders
  def attributes: Attributes[LogicalPlan]

  // L Ax (UP R) => UP (L Ax R)
  protected def unnestRightUnary(apply: Apply, lhs: LogicalPlan, rhs: LogicalUnaryPlan): LogicalPlan = {
    val newApply = apply.copy(right = rhs.source)(attributes.copy(apply.id))
    solveds.copy(apply.id, newApply.id)
    cardinalities.set(newApply.id, cardinalities(lhs.id) * cardinalities(rhs.source.id))
    providedOrders.copy(apply.id, newApply.id)

    val res = rhs.withLhs(newApply)(attributes.copy(rhs.id))
    solveds.copy(apply.id, res.id)
    cardinalities.set(res.id, cardinalities(lhs.id) * cardinalities(rhs.id))
    providedOrders.copy(apply.id, res.id)
    res
  }

  // L Ax (_ BP R) => L BP R
  protected def unnestRightBinaryLeft(apply: Apply, lhs: LogicalPlan, rhs: LogicalBinaryPlan): LogicalPlan = {
    val res = rhs.withLhs(lhs)(attributes.copy(rhs.id))
    solveds.copy(apply.id, res.id)
    cardinalities.copy(apply.id, res.id)
    providedOrders.copy(rhs.id, res.id)
    res
  }

  protected def assertArgumentHasCardinality1(arg: Argument): Unit = {
    // Argument plans are always supposed to have a Cardinality of 1.
    // If this should not hold, we would need to multiply Cardinality for this rewrite rule.
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(cardinalities(arg.id) == Cardinality.SINGLE, s"Argument plans should always have Cardinality 1. Had: ${cardinalities(arg.id)}")
  }
}
