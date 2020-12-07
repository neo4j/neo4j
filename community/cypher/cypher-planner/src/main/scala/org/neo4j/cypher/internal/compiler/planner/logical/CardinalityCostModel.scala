/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CostModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.ir.LazyMode
import org.neo4j.cypher.internal.ir.StrictnessMode
import org.neo4j.cypher.internal.logical.plans.AbstractLetSemiApply
import org.neo4j.cypher.internal.logical.plans.AbstractSemiApply
import org.neo4j.cypher.internal.logical.plans.AggregatingPlan
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.ApplyPlan
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.EagerLogicalPlan
import org.neo4j.cypher.internal.logical.plans.ExhaustiveLimit
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.ExpandInto
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.NodeIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.logical.plans.ProjectEndpoints
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Cost
import org.neo4j.cypher.internal.util.CostPerRow
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.Multiplier
import org.neo4j.cypher.internal.util.Selectivity

object CardinalityCostModel extends CostModel {

  val DEFAULT_COST_PER_ROW: CostPerRow = 0.1
  val PROBE_BUILD_COST: CostPerRow = 3.1
  val PROBE_SEARCH_COST: CostPerRow = 2.4
  val EAGERNESS_MULTIPLIER: Multiplier = 2.0
  // A property has at least 2 db hits, even though it could even have many more.
  val PROPERTY_ACCESS_DB_HITS = 2
  val LABEL_CHECK_DB_HITS = 1
  val EXPAND_INTO = 6.4

  private def costPerRow(plan: LogicalPlan, cardinality: Cardinality, semanticTable: SemanticTable): CostPerRow = plan match {
    /*
     * These constants are approximations derived from test runs,
     * see ActualCostCalculationTest
     */

    case _: NodeByLabelScan |
         _: NodeIndexScan |
         _: ProjectEndpoints
    => 1.0

    case Selection(predicate, _)
    => costPerRowFor(predicate, semanticTable)

    case _: AllNodesScan
    => 1.2

    case e: Expand if e.mode == ExpandInto
    => EXPAND_INTO

    case e: VarExpand if e.mode == ExpandInto
    => EXPAND_INTO

    case _: Expand |
         _: VarExpand
    => 1.5

    case _: NodeUniqueIndexSeek |
         _: NodeIndexSeek |
         _: NodeIndexContainsScan |
         _: NodeIndexEndsWithScan
    => 1.9

    case _: NodeByIdSeek |
         _: DirectedRelationshipByIdSeek |
         _: UndirectedRelationshipByIdSeek
    => 6.2

    case _: NodeHashJoin |
         _: AggregatingPlan |
         _: AbstractLetSemiApply |
         _: Limit |
         _: ExhaustiveLimit |
         _: Optional |
         _: Argument |
         _: LeftOuterHashJoin |
         _: RightOuterHashJoin |
         _: AbstractSemiApply |
         _: Skip |
         _: Union |
         _: ValueHashJoin |
         _: UnwindCollection |
         _: ProcedureCall
    => DEFAULT_COST_PER_ROW

    case _:Sort =>
      // Sorting is O(n * log(n)). Therefore the cost per row is O(log(n))
      // This means:
      // Sorting 1 row has cost 0.03 per row.
      // Sorting 9 rows has cost 0.1 per row (DEFAULT_COST_PER_ROW).
      // Sorting 99 rows has cost 0.2 per row.
      DEFAULT_COST_PER_ROW * Math.log(cardinality.amount + 1)

    case _: FindShortestPaths
    => 12.0

    case _ // Default
    => DEFAULT_COST_PER_ROW
  }

  private def cardinalityForPlan(plan: LogicalPlan, cardinalities: Cardinalities): Cardinality =
    plan.lhs.map(p => cardinalities.get(p.id)).getOrElse(cardinalities.get(plan.id))

  def costPerRowFor(expression: Expression, semanticTable: SemanticTable): CostPerRow = {
    val noOfStoreAccesses = expression.treeFold(0) {
      case x: Property if semanticTable.isNodeNoFail(x.map) || semanticTable.isRelationshipNoFail(x.map) => count => TraverseChildren(count + PROPERTY_ACCESS_DB_HITS)
      case _: HasLabels => count => TraverseChildren(count + LABEL_CHECK_DB_HITS)
      case _ => count => TraverseChildren(count)
    }
    if (noOfStoreAccesses > 0)
      CostPerRow(noOfStoreAccesses)
    else
      DEFAULT_COST_PER_ROW
  }

  def costFor(plan: LogicalPlan,
              input: QueryGraphSolverInput,
              semanticTable: SemanticTable,
              cardinalities: Cardinalities): Cost =
    calculateCost(plan, input.limitSelectivity, input.strictness, cardinalities, semanticTable)

  private final case class EffectiveCardinalities(currentPlanWorkload: Cardinality, lhs: Cardinality, rhs: Cardinality)

  private def calculateCost(plan: LogicalPlan,
                            limitSelectivity: Selectivity,
                            strictness: Option[StrictnessMode],
                            cardinalities: Cardinalities,
                            semanticTable: SemanticTable): Cost = {
    val (lhsLimitSelectivity, rhsLimitSelectivity) = childrenLimitSelectivities(plan, limitSelectivity, cardinalities)

    val lhsCost = plan.lhs.map(p => calculateCost(p, lhsLimitSelectivity, strictness, cardinalities, semanticTable)) getOrElse Cost.ZERO
    val rhsCost = plan.rhs.map(p => calculateCost(p, rhsLimitSelectivity, strictness, cardinalities, semanticTable)) getOrElse Cost.ZERO

    val effectiveCardinalitiess = EffectiveCardinalities(
      cardinalityForPlan(plan, cardinalities) * lhsLimitSelectivity,
      plan.lhs.map(p => cardinalities.get(p.id) * lhsLimitSelectivity) getOrElse Cardinality.EMPTY,
      plan.rhs.map(p => cardinalities.get(p.id) * rhsLimitSelectivity) getOrElse Cardinality.EMPTY
    )

    val cost = combinedCostForPlan(plan, effectiveCardinalitiess, lhsCost, rhsCost, semanticTable)

    strictness match {
      case Some(LazyMode) if !LazyMode(plan) => cost * EAGERNESS_MULTIPLIER
      case _ => cost
    }
  }

  private def childrenLimitSelectivities(plan: LogicalPlan, incomingLimitSelectivity: Selectivity, cardinalities: Cardinalities): (Selectivity, Selectivity) = plan match {
    case _: CartesianProduct =>
      val sqrt = Selectivity.of(math.sqrt(incomingLimitSelectivity.factor)).getOrElse(Selectivity.ONE)
      (sqrt, sqrt)

    //NOTE: we don't match on ExhaustiveLimit here since that doesn't affect the cardinality of earlier plans
    case limit: Limit =>
      val sourceCardinality = cardinalities.get(limit.source.id)
      val limitCardinality = cardinalities.get(limit.id)
      val s = (limitCardinality / sourceCardinality) getOrElse Selectivity.ONE
      (s, s)

    case HashJoin() =>
      (Selectivity.ONE, incomingLimitSelectivity)

    case _: EagerLogicalPlan =>
      (Selectivity.ONE, Selectivity.ONE)

    case _ =>
      (incomingLimitSelectivity, incomingLimitSelectivity)
  }

  private def combinedCostForPlan(plan: LogicalPlan,
                                  effectiveCardinalities: EffectiveCardinalities,
                                  lhsCost: Cost,
                                  rhsCost: Cost,
                                  semanticTable: SemanticTable): Cost = plan match {
    case _: CartesianProduct =>
      val lhsCardinality = Cardinality.max(Cardinality.SINGLE, effectiveCardinalities.lhs)
      lhsCost + lhsCardinality * rhsCost

    case _: ApplyPlan =>
      // the rCost has already been multiplied by the lhs cardinality
      lhsCost + rhsCost

    case HashJoin() =>
      lhsCost + rhsCost +
        effectiveCardinalities.lhs * PROBE_BUILD_COST +
        effectiveCardinalities.rhs * PROBE_SEARCH_COST

    case _ =>
      val rowCost = costPerRow(plan, effectiveCardinalities.currentPlanWorkload, semanticTable)
      val costForThisPlan = effectiveCardinalities.currentPlanWorkload * rowCost
      costForThisPlan + lhsCost + rhsCost
  }

  object HashJoin {
    def unapply(x: LogicalPlan): Boolean = x match {
      case _: NodeHashJoin |
           _: LeftOuterHashJoin |
           _: RightOuterHashJoin |
           _: ValueHashJoin => true
      case _ => false
    }
  }
}
