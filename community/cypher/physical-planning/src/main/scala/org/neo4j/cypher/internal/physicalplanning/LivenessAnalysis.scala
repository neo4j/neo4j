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
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.logical.plans.LogicalBinaryPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NestedPlanExpression
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.LiveVariables
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.attribution.Id

/**
 * Used to find 'live' variables at plan level. Variables that are not live
 * can be removed during query execution to free memory.
 */
object LivenessAnalysis {

  /**
   *
   * @param currentlyLive variables that are currently live while walking the plan
   * @param liveFromLhs variables that are live on the lhs when traversing the rhs
   * @param result plan id and the resulting live variables
   */
  private case class Acc(currentlyLive: Set[String], liveFromLhs: Map[Id, Set[String]], result: Seq[(Id, Set[String])])

  def computeLiveVariables(root: LogicalPlan, breakingPolicy: PipelineBreakingPolicy): LiveVariables = {
    // Use reverseTreeFold so that we go into RHS before LHS
    val Acc(_, _, result) = root.folder.reverseTreeFold(Acc(Set.empty, Map.empty, Seq.empty)) {
      case p: LogicalPlan => acc =>
          // Variables used by this plan.
          val varsInPlan = findVariablesInPlan(p, p.id)

          // On LHS of a binary plans, this is the live variables of the parent.
          val liveFromParent = acc.liveFromLhs.getOrElse(p.id, Set.empty)

          val newLive = (acc.currentlyLive ++ liveFromParent).intersect(p.availableSymbols.map(_.name)) ++ varsInPlan
          val newMap =
            if (breakingPolicy.canBeDiscardingPlan(p)) acc.result.appended(p.id -> newLive)
            else acc.result
          p match {
            case binaryPlan: LogicalBinaryPlan =>
              // Carry LHS variables through RHS, to keep them even if they are not available during RHS
              val newLiveFromLhs = acc.liveFromLhs.removed(p.id).updated(binaryPlan.left.id, newLive)
              TraverseChildren(Acc(newLive, newLiveFromLhs, newMap))
            case _ =>
              val newLiveFromLhs = acc.liveFromLhs.removed(p.id)
              TraverseChildren(Acc(newLive, newLiveFromLhs, newMap))
          }
      case _ => acc => SkipChildren(acc) // We ignore nested plan expressions
    }

    val live = new LiveVariables()
    result.foreach { case (planId, liveForPlan) =>
      AssertMacros.checkOnlyWhenAssertionsAreEnabled(!live.isDefinedAt(planId))
      live.set(planId, liveForPlan)
    }
    live
  }

  private def findVariablesInPlan(plan: AnyRef, planId: Id): Set[String] = {
    plan.folder.treeFold(Set.empty[String]) {
      case otherPlan: LogicalPlan if otherPlan.id != planId => acc => SkipChildren(acc)
      case Projection(_, projections) => acc => SkipChildren(acc ++ findVariablesInPlan(projections, planId))
      case v: LogicalVariable         => acc => SkipChildren(acc + v.name)
      case np: NestedPlanExpression   =>
        // Nested plan expressions can have dependencies to the outer plan.
        // Only through Argument, but lets be conservative here.
        acc => SkipChildren(acc ++ findAllVariables(np))
    }
  }

  private def findAllVariables(root: AnyRef): Set[String] =
    root.folder.treeFold(Set.empty[String]) { case v: LogicalVariable => acc => SkipChildren(acc + v.name) }
}
