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
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.ast.CountIRExpression
import org.neo4j.cypher.internal.ir.ast.ExistsIRExpression
import org.neo4j.cypher.internal.ir.ast.ListIRExpression
import org.neo4j.cypher.internal.ir.helpers.CachedFunction
import org.neo4j.cypher.internal.logical.plans.AggregatingPlan
import org.neo4j.cypher.internal.logical.plans.CachedProperties
import org.neo4j.cypher.internal.logical.plans.LogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NestedPlanExpression
import org.neo4j.cypher.internal.macros.AssertMacros3
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
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
      // #13924: evaluate outer-only COUNT subqueries once per outer row, before the
      // OPTIONAL RHS fan-out, instead of once per RHS candidate row (see hoistOuterOnlyCounts).
      val hoisting = hoistOuterOnlyCounts(lhs, optionalQg)
      val outerPlan = hoisting.map(_.outerPlan).getOrElse(lhs)
      val rhsQueryGraph = hoisting.map(_.rhsQueryGraph).getOrElse(optionalQg)
      val lhsSymbols = outerPlan.availableSymbols
      val lhsCachedProperties = context.staticComponents.planningAttributes.cachedPropertiesPerPlan(outerPlan.id)
      val inner =
        if (hoisting.isEmpty && lhsCachedProperties.isEmpty)
          innerPlanWithoutPreviouslyCachedProperties
        else if (hoisting.isEmpty)
          cachedPlanInnerOfOptionalMatch(lhsCachedProperties)
        else
          planInnerWithRewrittenSelections(rhsQueryGraph, lhsCachedProperties)
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
                   |$outerPlan
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
          rhsQueryGraph
        )
        // since inner is solved before the lhs, we are unable to carry the cached properties from lhs to the rhs.
        // therefore, we need to use a union to get the cached properties from both the lhs and rhs.
        val lhsPlanCachedProperties =
          context.staticComponents.planningAttributes.cachedPropertiesPerPlan.get(outerPlan.id)
        val rhsPlanCachedProperties =
          context.staticComponents.planningAttributes.cachedPropertiesPerPlan.get(rhs.id)
        val cachedPropertiesForOptional = lhsPlanCachedProperties.union(rhsPlanCachedProperties)
        val applied = context.staticComponents.logicalPlanProducer.planApplyWithCachedProperties(
          outerPlan,
          rhs,
          context,
          cachedPropertiesForOptional
        )
        // If COUNT predicates were hoisted, the solved query still records their
        // rewritten form (against the introduced count variables). Restore the
        // original predicates, exactly like planSelection records original
        // expressions while filtering on rewritten ones, so that VerifyBestPlan
        // still sees the input query as solved.
        val appliedWithRestoredSolved = hoisting match {
          case Some(h) =>
            context.staticComponents.logicalPlanProducer.fixupOptionalHoistedSolved(
              applied,
              h.originalPredicates,
              h.rewrittenPredicates,
              h.introducedVariables
            )
          case None => applied
        }

        // Often the Apply can be rewritten into an OptionalExpand. We want to do that before cost estimating against the hash joins, otherwise that
        // is not a fair comparison (as they cannot be rewritten to something cheaper).
        unnestOptional(appliedWithRestoredSolved).asInstanceOf[LogicalPlan]
      }
    }

    /**
     * Plan the OPTIONAL RHS for a rewritten optional query graph. Used only when
     * [[hoistOuterOnlyCounts]] moved predicates, so the shared inner-plan caches
     * (built for the original selections) must not be reused. Like [[doPlan]],
     * but for the rewritten query graph. Planning cost only; no runtime impact.
     */
    private def planInnerWithRewrittenSelections(
      rhsQueryGraph: QueryGraph,
      previouslyCachedProperties: CachedProperties
    ): BestPlans = {
      context.staticComponents.queryGraphSolver.plan(
        rhsQueryGraph,
        removeColumnsWithoutDependencies(interestingOrderConfig),
        innerContext.withModifiedPlannerState(_.withPreviouslyCachedProperties(previouslyCachedProperties))
      )
    }

    /**
     * Result of [[hoistOuterOnlyCounts]] when at least one COUNT predicate was hoisted.
     *
     * @param outerPlan the outer LHS with one `Apply` per hoisted predicate appended.
     * @param rhsQueryGraph the OPTIONAL query graph with hoisted predicates rewritten
     *                      to reference the computed count variables.
     * @param originalPredicates the original predicate expressions, in the same order
     *                           as `rewrittenPredicates`.
     * @param rewrittenPredicates the rewritten predicate expressions now solved inside
     *                            the OPTIONAL RHS.
     * @param introducedVariables the count variables introduced on the outer plan,
     *                            also added to the RHS argument IDs.
     */
    private case class OuterCountHoisting(
      outerPlan: LogicalPlan,
      rhsQueryGraph: QueryGraph,
      originalPredicates: Seq[Expression],
      rewrittenPredicates: Seq[Expression],
      introducedVariables: Set[LogicalVariable]
    )

    /**
     * Solve COUNT subqueries whose dependencies are already satisfied by the
     * outer LHS before the OPTIONAL RHS fan-out (#13924).
     *
     * Without this, an outer-only `COUNT { ... }` in `OPTIONAL MATCH ... WHERE`
     * is solved inside the OPTIONAL RHS, on one side of its internal Cartesian
     * product. It is then re-evaluated once per RHS candidate row instead of
     * once per outer row, even though its value is constant across the fan-out
     * of one outer row. For an expensive nested plan such as `LOAD CSV` this
     * dominates execution, while an in-memory `UNWIND` hides the multiplicity.
     *
     * This solves each eligible COUNT on `lhs` via an `Apply` (the same
     * [[SubqueryExpressionSolver]] machinery used for ordinary WHERE subqueries),
     * so it runs once per outer row, and rewrites the OPTIONAL selections to
     * reference the computed count variable. The rewritten predicate application
     * stays inside the OPTIONAL RHS, preserving null-extension semantics: when the
     * predicate is false (or null) for an outer row, every RHS candidate of that
     * row is still eliminated and the OPTIONAL still emits exactly one null row.
     *
     * Only predicates whose nested subqueries are all outer-only, read-only
     * `CountIRExpression`s are eligible. Anything else (RHS-correlated COUNTs,
     * EXISTS/list subqueries, blocked contexts solved as per-row nested pipes,
     * updating subqueries) keeps the existing in-RHS evaluation. In particular the
     * evaluation count per outer row can already vary today with plan shape (which
     * Cartesian side hosts the COUNT Apply), so once-per-outer-row evaluation
     * introduces no new semantic latitude.
     */
    private def hoistOuterOnlyCounts(
      lhs: LogicalPlan,
      rhsQueryGraph: QueryGraph
    ): Option[OuterCountHoisting] = {
      val cancellationChecker = context.staticComponents.cancellationChecker
      // Fast path: most OPTIONAL MATCHes have no COUNT subquery at all. A single
      // traversal avoids any per-predicate work in that common case, so the added
      // planning cost here is otherwise linear in the total selection size.
      val mentionsCount = rhsQueryGraph.selections.predicates.folder(cancellationChecker).treeExists {
        case _: CountIRExpression => true
      }
      if (!mentionsCount) {
        None
      } else {
        hoistOuterOnlyCountsSlowPath(lhs, rhsQueryGraph, cancellationChecker)
      }
    }

    private def hoistOuterOnlyCountsSlowPath(
      lhs: LogicalPlan,
      rhsQueryGraph: QueryGraph,
      cancellationChecker: CancellationChecker
    ): Option[OuterCountHoisting] = {
      val lhsSymbols = lhs.availableSymbols
      // Selections are stored per conjunct; sorted for a deterministic Apply-chaining order.
      val orderedPredicates = rhsQueryGraph.selections.predicates.toSeq.sorted
      val eligible = orderedPredicates.filter { pred =>
        val counts = pred.expr.folder(cancellationChecker).findAllByClass[CountIRExpression]
        counts.nonEmpty &&
        !pred.expr.folder(cancellationChecker).treeExists {
          case _: ExistsIRExpression => true
          case _: ListIRExpression   => true
        } &&
        counts.forall(_.dependencies.subsetOf(lhsSymbols)) &&
        counts.forall(_.query.readOnly)
      }
      if (eligible.isEmpty) {
        None
      } else {
        var current = lhs
        // (original, rewritten) only for predicates that actually took the Apply path.
        val hoistedPairs = eligible.flatMap { pred =>
          val solver = SubqueryExpressionSolver.solverFor(current, context)
          val solved = solver.solve(pred.expr)
          val next = solver.rewrittenPlan()
          // Accept only the directly-unnested Apply path. A COUNT in a blocked
          // context (e.g. inside CASE) comes back as a per-row NestedPlanExpression
          // with no plan change and must keep its existing in-RHS evaluation.
          val noCountLeft = !solved.folder(cancellationChecker).treeExists { case _: CountIRExpression => true }
          val noNestedPlan = !solved.folder(cancellationChecker).treeExists { case _: NestedPlanExpression => true }
          if ((next ne current) && noCountLeft && noNestedPlan) {
            current = next
            Some((pred.expr, solved))
          } else {
            None
          }
        }
        if (hoistedPairs.isEmpty) {
          None
        } else {
          val (originals, rewritten) = hoistedPairs.unzip
          // Expose the introduced count variables to the RHS planning as arguments,
          // exactly like any other outer variable referenced from the OPTIONAL RHS.
          val introduced = current.availableSymbols -- lhs.availableSymbols
          val newSelections = rhsQueryGraph.selections -- originals ++ rewritten
          val newQueryGraph = rhsQueryGraph
            .withSelections(newSelections)
            .withArgumentIds(rhsQueryGraph.argumentIds ++ introduced)
          Some(OuterCountHoisting(current, newQueryGraph, originals, rewritten, introduced))
        }
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
