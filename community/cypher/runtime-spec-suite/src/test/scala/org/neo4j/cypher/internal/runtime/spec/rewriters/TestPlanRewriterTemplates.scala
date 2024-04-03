/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.spec.rewriters

import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.AssertSameRelationship
import org.neo4j.cypher.internal.logical.plans.Input
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.runtime.spec.rewriters.TestPlanCombinationRewriterConfig.PlanRewriterStepConfig
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.RewriterWithParent
import org.neo4j.cypher.internal.util.bottomUpWithParent
import org.neo4j.cypher.internal.util.topDown

import scala.util.Random

object TestPlanRewriterTemplates {

  // --------------------------------------------------------------------------
  // Rewriter templates
  // --------------------------------------------------------------------------
  def everywhere(
    ctx: PlanRewriterContext,
    config: PlanRewriterStepConfig,
    rewritePlan: LogicalPlan => LogicalPlan
  ): Rewriter = everywhere(config.weight, rewritePlan)

  def everywhere(
    weight: Double,
    rewritePlan: LogicalPlan => LogicalPlan,
    random: Random = Random
  ): Rewriter = {
    bottomUpWithParent(
      RewriterWithParent.lift {
        case (pr: ProduceResult, _) =>
          pr
        case (p: LogicalPlan, parent: Option[LogicalPlan])
          if isParentOkToInterject(parent) && randomShouldApply(weight, random) =>
          rewritePlan(p)
      },
      onlyRewriteLogicalPlansStopper
    )
  }

  def onTop(
    ctx: PlanRewriterContext,
    config: PlanRewriterStepConfig,
    rewritePlan: LogicalPlan => LogicalPlan
  ): Rewriter = topDown(
    Rewriter.lift {
      case ProduceResult(source, _) if isLeftmostLeafOkToMove(source) && randomShouldApply(config) =>
        rewritePlan(source)
    },
    onlyRewriteLogicalPlansStopper
  )

  // --------------------------------------------------------------------------
  // Conditions
  // --------------------------------------------------------------------------
  def randomShouldApply(stepConfig: PlanRewriterStepConfig): Boolean = {
    randomShouldApply(stepConfig.weight)
  }

  def randomShouldApply(weight: Double, random: Random = Random): Boolean = {
    weight match {
      case 0.0 =>
        false
      case 1.0 =>
        true
      case w =>
        random.nextDouble() < w
    }
  }

  def isLeftmostLeafOkToMove(plan: LogicalPlan): Boolean = {
    plan.leftmostLeaf match {
      case _: Input =>
        false

      case _ =>
        true
    }
  }

  def isParentOkToInterject(parent: Option[LogicalPlan]): Boolean = {
    parent match {
      case Some(_: AssertSameNode | _: AssertSameRelationship) =>
        // AssertSameNode and AssertSameRelationship are only supported by rewriter in pipelined, and it relies on assumptions about the possible plans,
        // so we cannot insert a plan between it and its children
        false
      case _ =>
        true
    }
  }

  // --------------------------------------------------------------------------
  // Stoppers
  // --------------------------------------------------------------------------
  def onlyRewriteLogicalPlansStopper(a: AnyRef): Boolean = a match {
    // Only rewrite logical plans
    case _: LogicalPlan => false
    case _              => true
  }
}
