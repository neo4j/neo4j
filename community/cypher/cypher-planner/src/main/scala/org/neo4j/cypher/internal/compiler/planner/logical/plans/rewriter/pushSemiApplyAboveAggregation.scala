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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.logical.plans.AbstractSemiApply
import org.neo4j.cypher.internal.logical.plans.AggregatingPlan
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.OrderedAggregation
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.Rewriter.BottomUpMergeableRewriter
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.bottomUp

/**
 * Pushes a `SemiApply` / `AntiSemiApply` above a grouping aggregation when the apply's
 * right-hand side depends only on variables the aggregation groups by.
 *
 * {{{
 *   Aggregation(group = G, agg = A)            (Anti)SemiApply
 *   |                                =>        | \
 *   (Anti)SemiApply(L, R)                      |  R
 *   | \                                        Aggregation(group = G', agg = A)
 *   L  R                                       |
 *                                              L
 * }}}
 *
 * An existential subquery `R` whose dependencies are all grouping keys produces a constant result
 * within each group, so it filters whole groups rather than individual rows. The aggregation
 * deduplicates its input on the grouping keys, so evaluating `R` above the aggregation runs it once
 * per group instead of once per (far more numerous) pre-aggregation row. A group is either wholly
 * kept or wholly dropped, so per-group aggregates are unchanged and the rewrite is result-preserving.
 *
 * This is the bottleneck in LDBC BI Q18 ("friend recommendation"): the
 * `NOT EXISTS { (p1)-[:KNOWS]-(p2) }` anti-join is planned below the `count(DISTINCT mid)`
 * aggregation and re-evaluated once per `(p1, mid, p2)` triple (~440K) instead of once per distinct
 * `(p1, p2)` pair (~65K) — a 7-8x db-hit reduction when pushed above.
 *
 * Both [[Aggregation]] and [[OrderedAggregation]] are handled. A `SemiApply`/`AntiSemiApply`
 * preserves the order of its left-hand side, so the order an `OrderedAggregation` leverages is the
 * same whether it sits above or below the apply; the leveraged order is carried over unchanged.
 *
 * == Functional dependency ==
 * Q18 groups by `p1.id, p2.id` (from `RETURN p1.id, ...`), while `R` reads the `p1`, `p2` nodes.
 * Those nodes are not grouping keys directly, but they are functionally determined by the grouping
 * when `id` carries a node-key constraint (or a uniqueness constraint together with an existence
 * constraint): `p1.id` then uniquely identifies `p1`. In that case the node variables are added to
 * the grouping — a no-op on the groups under the constraint — so they survive the aggregation and
 * `R` can run above it.
 *
 * Existence is required as well as uniqueness: a uniqueness constraint alone permits multiple NULLs,
 * so for null-valued nodes `p1.id` would not determine `p1`, and adding `p1` to the grouping could
 * split a NULL group and change the per-group aggregates. Requiring existence rules that out.
 *
 * == Cost guard ==
 * Pushing the apply above the aggregation never increases db-hits — the left-hand side is unchanged
 * and the existential subquery is evaluated once per group instead of once per (more numerous) row.
 * But when there is little fan-out (groups ≈ rows) it saves nothing, and it would make the aggregation
 * buffer state for groups the existential later drops — wasteful for a memory-heavy aggregate. So the
 * rewrite only fires when each group averages at least [[pushSemiApplyAboveAggregation.MinFanOut]]
 * rows, estimated as `card(semiApply) / card(aggregation)`.
 */
object pushSemiApplyAboveAggregation {

  /**
   * Minimum estimated rows-per-group (filtered rows / groups) for the rewrite to be worthwhile: the
   * existential subquery must run at least this many times less often above the aggregation than below.
   */
  private val MinFanOut = 2.0
}

case class pushSemiApplyAboveAggregation(
  planContext: PlanContext,
  solveds: Solveds,
  cardinalities: Cardinalities,
  providedOrders: ProvidedOrders,
  attributes: Attributes[LogicalPlan]
) extends Rewriter with BottomUpMergeableRewriter {

  override def apply(input: AnyRef): AnyRef = instance.apply(input)

  import pushSemiApplyAboveAggregation.MinFanOut

  override val innerRewriter: Rewriter = Rewriter.lift {
    case agg @ Aggregation(semiApply: AbstractSemiApply, groupingExpressions, aggregationExpressions) =>
      pushAbove(
        agg,
        semiApply,
        groupingExpressions,
        newGrouping =>
          Aggregation(semiApply.left, newGrouping, aggregationExpressions)(attributes.copy(agg.id))
      )

    case agg @ OrderedAggregation(semiApply: AbstractSemiApply, groupingExpressions, aggregationExpressions, order) =>
      pushAbove(
        agg,
        semiApply,
        groupingExpressions,
        newGrouping =>
          OrderedAggregation(semiApply.left, newGrouping, aggregationExpressions, order)(attributes.copy(agg.id))
      )
  }

  /**
   * If the apply's RHS depends only on the grouping (possibly via a functional dependency), rebuild
   * the aggregation below the apply — consuming the apply's LHS, with the grouping augmented to
   * expose the RHS dependencies — and lift the apply above it. `buildAggregation` reconstructs the
   * concrete aggregation plan (plain or ordered) with the augmented grouping. Otherwise return the
   * aggregation unchanged.
   */
  private def pushAbove(
    agg: AggregatingPlan,
    semiApply: AbstractSemiApply,
    groupingExpressions: Map[LogicalVariable, Expression],
    buildAggregation: Map[LogicalVariable, Expression] => LogicalPlan
  ): LogicalPlan = {
    groupingExposingRhsDeps(agg, semiApply, groupingExpressions) match {
      case Some(newGrouping) if worthwhile(agg, semiApply) =>
        // Both replacements get fresh ids (the attribute framework is set-once, so we must not
        // re-set attributes on an existing id). The aggregation now consumes the apply's LHS.
        val newAgg = buildAggregation(newGrouping)
        solveds.copy(agg.id, newAgg.id)
        cardinalities.copy(agg.id, newAgg.id)
        providedOrders.copy(agg.id, newAgg.id)

        // The existential subquery filters the (now deduplicated) grouped rows.
        val newSemiApply = semiApply.withLhs(newAgg)(attributes.copy(semiApply.id))
        solveds.copy(agg.id, newSemiApply.id)
        cardinalities.copy(agg.id, newSemiApply.id)
        providedOrders.copy(agg.id, newSemiApply.id)
        newSemiApply

      case _ => agg
    }
  }

  /**
   * Cost guard: fire only when the aggregation collapses at least [[MinFanOut]] rows into each group,
   * so the existential subquery runs meaningfully fewer times above the aggregation than below. Fan-out
   * is estimated as filtered-rows-per-group, `card(semiApply) / card(aggregation)`. When the group
   * estimate is unavailable (`0`, e.g. in unit tests) the guard does not block.
   */
  private def worthwhile(agg: AggregatingPlan, semiApply: AbstractSemiApply): Boolean = {
    val filteredRows = cardinalities.get(semiApply.id).amount
    val groups = cardinalities.get(agg.id).amount
    groups <= 0.0 || filteredRows >= groups * MinFanOut
  }

  /**
   * If every outer variable the apply's RHS reads is exposed by the aggregation — directly as a
   * grouping key, or as a node variable functionally determined by a constrained property already in
   * the grouping — return the grouping augmented with those node variables (so they survive the
   * aggregation). Otherwise `None` (do not rewrite).
   */
  private def groupingExposingRhsDeps(
    agg: AggregatingPlan,
    semiApply: AbstractSemiApply,
    groupingExpressions: Map[LogicalVariable, Expression]
  ): Option[Map[LogicalVariable, Expression]] = {
    val rhsDeps = semiApply.right.localAvailableSymbols intersect semiApply.left.localAvailableSymbols
    if (rhsDeps.isEmpty) return None
    val groupingKeys = groupingExpressions.keySet

    val solvedQg = solveds.get(agg.id) match {
      case spq: SinglePlannerQuery => spq.queryGraph
      case _                       => return None
    }

    var additions = Map.empty[LogicalVariable, Expression]
    val allExposed = rhsDeps.forall { v =>
      groupingKeys.contains(v) || groupingExpressions.values.exists {
        case Property(vv: LogicalVariable, pk) if vv == v && isFunctionallyDetermined(v, pk.name, solvedQg) =>
          additions += (v -> v)
          true
        case _ => false
      }
    }
    if (allExposed) Some(groupingExpressions ++ additions) else None
  }

  /**
   * True if `v.prop` uniquely identifies the `v` node: some guaranteed label of `v` carries both a
   * uniqueness constraint and an existence constraint on `prop` (i.e. a node key, or the two
   * separately). Existence is required for soundness — a uniqueness constraint alone permits multiple
   * NULLs, for which `v.prop` would not determine `v`.
   */
  private def isFunctionallyDetermined(v: LogicalVariable, prop: String, qg: QueryGraph): Boolean =
    qg.allPossibleLabelsOnNode(v).exists { label =>
      planContext.rangeIndexGetForLabelAndProperties(label.name, Seq(prop)).exists(_.isUnique) &&
      planContext.hasNodePropertyExistenceConstraint(label.name, prop)
    }

  private val instance: Rewriter = bottomUp(innerRewriter)
}
