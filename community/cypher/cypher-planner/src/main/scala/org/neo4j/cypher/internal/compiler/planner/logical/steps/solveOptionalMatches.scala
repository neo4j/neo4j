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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.ast.UsingJoinHint
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.idp.BestResults
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.unnestOptional
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.helpers.CachedFunction
import org.neo4j.cypher.internal.logical.plans.AggregatingPlan
import org.neo4j.cypher.internal.logical.plans.CachedProperties
import org.neo4j.cypher.internal.logical.plans.LogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.macros.AssertMacros3
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp

trait OptionalSolverFactory {

  /**
   * Return a Solver for an OPTIONAL MATCH.
   *
   * @param optionalQg             the query graph of the OPTIONAL MATCH
   * @param enclosingQg            the query graph enclosing the `optionalQg`
   * @param interestingOrderConfig the InterestingOrderConfig
   * @param context                the LogicalPlanningContext
   * @return a Solver that given a plan for the `enclosingQg` and any so far connected components or other OPTIONAL MATCHES
   *         returns an Iterator of plan candidates solving the OPTIONAL MATCH.
   */
  def solver(
    optionalQg: QueryGraph,
    enclosingQg: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): OptionalSolverFactory.Solver
}

object OptionalSolverFactory {

  trait Solver {

    /**
     * Solve an OPTIONAL MATCH.
     *
     * @param lp the plan for `enclosingQg` and any so far connected components or other OPTIONAL MATCHES
     * @return an Iterator of plan candidates solving the OPTIONAL MATCH.
     */
    def connect(lp: LogicalPlan): Iterator[LogicalPlan]
  }
}

case object ApplyOptionalSolverFactory extends OptionalSolverFactory {

  override def solver(
    optionalQg: QueryGraph,
    enclosingQg: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): OptionalSolverFactory.Solver = {
    new ApplyOptionalSolver(optionalQg, enclosingQg, interestingOrderConfig, context)
  }

  private class ApplyOptionalSolver(
    optionalQg: QueryGraph,
    enclosingQg: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ) extends OptionalSolverFactory.Solver {

    private val innerContext: LogicalPlanningContext =
      context.withModifiedPlannerState(_.withFusedLabelInfo(enclosingQg.selections.labelInfo))

    private def doPlan(previouslyCachedProperties: CachedProperties): BestPlans = {
      context.staticComponents.queryGraphSolver.plan(
        optionalQg,
        removeColumnsWithoutDependencies(interestingOrderConfig),
        innerContext.withModifiedPlannerState(_.withPreviouslyCachedProperties(previouslyCachedProperties))
      )
    }

    // The case without previously cached properties is computed as a lazy val and not the cache function.
    // This case is handled separately, since non-sharded databases will never have previously cached properties.
    // This should avoid the overhead of the cache function and any regressions in planning times for non-sharded deployments.
    private lazy val innerPlanWithoutPreviouslyCachedProperties: BestPlans =
      context.staticComponents.queryGraphSolver.plan(
        optionalQg,
        removeColumnsWithoutDependencies(interestingOrderConfig),
        innerContext
      )

    private val cachedPlanInnerOfOptionalMatch = CachedFunction(doPlan _)

    override def connect(lhs: LogicalPlan): Iterator[LogicalPlan] = {
      // Prefetch properties used on RHS.
      // This avoids:
      // - Doing a remote call for each argument.
      // - Fetching properties under Optional, which means we might be missing some values later on.
      val lhsWithPrefetchedProperties =
        context.settings.remoteBatchPropertiesStrategy
          .planRemotePropertiesBeforeApplyOptional(optionalQg, lhs, context)

      generateCandidates(lhsWithPrefetchedProperties)
    }

    private def generateCandidates(lhs: LogicalPlan): Iterator[LogicalPlan] = {
      val lhsSymbols = lhs.availableSymbols
      val lhsCachedProperties = context.staticComponents.planningAttributes.cachedPropertiesPerPlan(lhs.id)
      val inner =
        if (lhsCachedProperties.isEmpty)
          innerPlanWithoutPreviouslyCachedProperties
        else {
          cachedPlanInnerOfOptionalMatch(lhsCachedProperties)
        }
      inner.allResults.iterator.map { inner =>
        val innerWithFixedArguments = inner.endoRewrite(bottomUp(
          Rewriter.lift {
            case llp: LogicalLeafPlan => llp.addArgumentIds(lhsSymbols)
            case ap: AggregatingPlan  => ap.addGroupingExpressions(lhsSymbols.map(s => s -> s).toMap)
            case p: LogicalPlan =>
              AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
                lhsSymbols.subsetOf(p.availableSymbols),
                s"""RHS of optional must maintain LHS available symbols.
                   |
                   |LHS: (available symbols: ${lhsSymbols.map(_.name).mkString("`", "`, `", "`")})
                   |$lhs
                   |
                   |RHS: (available symbols: ${p.availableSymbols.map(_.name).mkString("`", "`, `", "`")})
                   |$inner
                   |
                   |fails at: $p
                   |""".stripMargin
              )
              p
          },
          stopper = !_.isInstanceOf[LogicalPlan]
        ))

        val rhs = context.staticComponents.logicalPlanProducer.planOptionalMatch(
          innerWithFixedArguments,
          lhsSymbols,
          innerContext,
          optionalQg
        )
        // since inner is solved before the lhs, we are unable to carry the cached properties from lhs to the rhs.
        // therefore, we need to use a union to get the cached properties from both the lhs and rhs.
        val lhsPlanCachedProperties =
          context.staticComponents.planningAttributes.cachedPropertiesPerPlan.get(lhs.id)
        val rhsPlanCachedProperties =
          context.staticComponents.planningAttributes.cachedPropertiesPerPlan.get(rhs.id)
        val cachedPropertiesForOptional = lhsPlanCachedProperties.union(rhsPlanCachedProperties)
        val applied = context.staticComponents.logicalPlanProducer.planApplyWithCachedProperties(
          lhs,
          rhs,
          context,
          cachedPropertiesForOptional
        )

        // Often the Apply can be rewritten into an OptionalExpand. We want to do that before cost estimating against the hash joins, otherwise that
        // is not a fair comparison (as they cannot be rewritten to something cheaper).
        unnestOptional(applied).asInstanceOf[LogicalPlan]
      }
    }

    /**
     * Projecting a column without dependencies under Optional might incorrectly set it to NULL.
     */
    private def removeColumnsWithoutDependencies(interestingOrderConfig: InterestingOrderConfig)
      : InterestingOrderConfig = {
      InterestingOrderConfig(
        orderToReportAndSolve =
          interestingOrderConfig
            .orderToSolve // we don't verify solved InterestingOrder for Optional anyway
            .mapOrderCandidates(_.takeWhile(column => column.dependencies.nonEmpty))
      )
    }
  }
}

case object OuterHashJoinSolverFactory extends OptionalSolverFactory {

  override def solver(
    optionalQg: QueryGraph,
    enclosingQg: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): OptionalSolverFactory.Solver = {
    val joinNodes = optionalQg.argumentIds

    // It is not allowed to plan a join on the RHS of an Apply if any of the nodes we are joining on comes from the LHS of the Apply.
    // The easiest way to ensure that this doesn't happen is to check that we are in the "first part" of a query (i.e. not planning a tail).
    // We can check this with "context.outerPlan.isEmpty".
    if (
      joinNodes.intersect(enclosingQg.argumentIds).isEmpty &&
      joinNodes.nonEmpty &&
      joinNodes.subsetOf(optionalQg.patternNodes)
    ) {
      val solvedHints =
        optionalQg.joinHints
          .filter(_.variables.forall(joinNodes))
      val rhsQG =
        optionalQg
          .removeArguments()
          .removeHints(solvedHints)

      val BestResults(side2Plan, side2SortedPlan, side2ExtraPropertiesPlan) =
        context.staticComponents.queryGraphSolver.plan(rhsQG, interestingOrderConfig, context)

      (side1Plan: LogicalPlan) => {
        if (joinNodes.subsetOf(side1Plan.availableSymbols)) {
          Iterator(
            leftOuterJoin(context, joinNodes, side1Plan, side2Plan, solvedHints),
            rightOuterJoin(context, joinNodes, side1Plan, side2Plan, solvedHints)
          ) ++
            side2ExtraPropertiesPlan.map { side2PlanWithExtraProps =>
              Iterator(
                leftOuterJoin(context, joinNodes, side1Plan, side2PlanWithExtraProps, solvedHints),
                rightOuterJoin(context, joinNodes, side1Plan, side2PlanWithExtraProps, solvedHints)
              )
            }.getOrElse(Iterator.empty[LogicalPlan]) ++
            side2SortedPlan.map(leftOuterJoin(context, joinNodes, side1Plan, _, solvedHints))
        } else {
          Iterator.empty
        }
      }
    } else {
      (_: LogicalPlan) => Iterator.empty
    }
  }

  private def leftOuterJoin(
    context: LogicalPlanningContext,
    joinNodes: Set[LogicalVariable],
    lhs: LogicalPlan,
    rhs: LogicalPlan,
    solvedHints: Set[UsingJoinHint]
  ): LogicalPlan =
    context.staticComponents.logicalPlanProducer.planLeftOuterHashJoin(joinNodes, lhs, rhs, solvedHints, context)

  private def rightOuterJoin(
    context: LogicalPlanningContext,
    joinNodes: Set[LogicalVariable],
    rhs: LogicalPlan,
    lhs: LogicalPlan,
    solvedHints: Set[UsingJoinHint]
  ): LogicalPlan =
    context.staticComponents.logicalPlanProducer.planRightOuterHashJoin(joinNodes, lhs, rhs, solvedHints, context)
}
