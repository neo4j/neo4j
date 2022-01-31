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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.ExecutionModel.SelectedBatchSize
import org.neo4j.cypher.internal.compiler.ExecutionModel.VolcanoBatchSize
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.EffectiveCardinalities
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.HashJoin
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.PROBE_BUILD_COST
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.PROBE_SEARCH_COST
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.costPerRow
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.effectiveCardinalities
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.getEffectiveBatchSize
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CostModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.expressions.CachedHasProperty
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.HasLabelsOrTypes
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.logical.plans.AbstractLetSemiApply
import org.neo4j.cypher.internal.logical.plans.AbstractSemiApply
import org.neo4j.cypher.internal.logical.plans.AggregatingPlan
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.ApplyPlan
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.ExhaustiveLimit
import org.neo4j.cypher.internal.logical.plans.ExhaustiveLogicalPlan
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.ExpandInto
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LimitingLogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalBinaryPlan
import org.neo4j.cypher.internal.logical.plans.LogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalUnaryPlan
import org.neo4j.cypher.internal.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.NodeIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.OrderedUnion
import org.neo4j.cypher.internal.logical.plans.PartialSort
import org.neo4j.cypher.internal.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.logical.plans.ProjectEndpoints
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.SingleFromRightLogicalPlan
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Cost
import org.neo4j.cypher.internal.util.CostPerRow
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.Multiplier
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.cypher.internal.util.WorkReduction

case class CardinalityCostModel(executionModel: ExecutionModel) extends CostModel {

  override def costFor(plan: LogicalPlan,
                       input: QueryGraphSolverInput,
                       semanticTable: SemanticTable,
                       cardinalities: Cardinalities,
                       providedOrders: ProvidedOrders,
                       monitor: CostModelMonitor): Cost = {
    // The plan we use here to select the batch size will obviously not be the final plan for the whole query.
    // So it could very well be that we select the small chunk size here for cost estimation purposes, but the cardinalities increase
    // in the later course of the plan and it will actually be executed with the big chunk size.
    val batchSize = executionModel.selectBatchSize(plan, cardinalities)
    calculateCost(plan, WorkReduction(input.limitSelectivity), cardinalities, providedOrders, semanticTable, plan, batchSize, monitor)
  }

  /**
   * Recursively calculate the cost of a plan
   *
   * @param plan          the plan
   * @param workReduction expected work reduction due to limits and laziness
   * @param rootPlan      the whole plan currently calculating cost for.
   */
  private def calculateCost(plan: LogicalPlan,
                            workReduction: WorkReduction,
                            cardinalities: Cardinalities,
                            providedOrders: ProvidedOrders,
                            semanticTable: SemanticTable,
                            rootPlan: LogicalPlan,
                            batchSize: SelectedBatchSize,
                            monitor: CostModelMonitor): Cost = {

    val effectiveBatchSize = getEffectiveBatchSize(batchSize, plan, providedOrders)

    val effectiveCard = effectiveCardinalities(plan, workReduction, effectiveBatchSize, cardinalities)

    val lhsCost = plan.lhs.map(p => calculateCost(p, effectiveCard.lhsReduction, cardinalities, providedOrders, semanticTable, rootPlan, batchSize, monitor)) getOrElse Cost.ZERO
    val rhsCost = plan.rhs.map(p => calculateCost(p, effectiveCard.rhsReduction, cardinalities, providedOrders, semanticTable, rootPlan, batchSize, monitor)) getOrElse Cost.ZERO

    val cost = combinedCostForPlan(plan, effectiveCard, cardinalities, lhsCost, rhsCost, semanticTable, effectiveBatchSize)

    monitor.reportPlanCost(rootPlan, plan, cost)
    monitor.reportPlanEffectiveCardinality(rootPlan, plan, effectiveCard.outputCardinality)

    cost
  }

  /**
   * Calculate the combined cost of a plan, including the costs of its children.
   *
   * @param plan                   the plan
   * @param effectiveCardinalities the effective cardinalities for this plan, the LHS and the RHS
   * @param cardinalities          the cardinalities without applying any limit selectivities to them
   */
  private def combinedCostForPlan(plan: LogicalPlan,
                                  effectiveCardinalities: EffectiveCardinalities,
                                  cardinalities: Cardinalities,
                                  lhsCost: Cost,
                                  rhsCost: Cost,
                                  semanticTable: SemanticTable,
                                  batchSize: SelectedBatchSize): Cost = plan match {
    case _: CartesianProduct =>
      val lhsCardinality = Cardinality.max(Cardinality.SINGLE, effectiveCardinalities.lhs)

      // Batched: The RHS is executed for each batch of LHS rows
      // Volcano: The RHS is executed for each LHS row
      val rhsExecutions = batchSize.numBatchesFor(lhsCardinality)
      lhsCost + rhsExecutions * rhsCost

    case _: ApplyPlan =>
      val lhsCardinality = Cardinality.max(Cardinality.SINGLE, effectiveCardinalities.lhs)
      // The RHS is executed for each LHS row
      lhsCost + lhsCardinality * rhsCost

    case HashJoin() =>
      lhsCost + rhsCost +
        effectiveCardinalities.lhs * PROBE_BUILD_COST +
        effectiveCardinalities.rhs * PROBE_SEARCH_COST

    case _ =>
      val rowCost = costPerRow(plan, effectiveCardinalities.inputCardinality, semanticTable)
      val costForThisPlan = effectiveCardinalities.inputCardinality * rowCost
      costForThisPlan + lhsCost + rhsCost
  }
}

object CardinalityCostModel {
  val DEFAULT_COST_PER_ROW: CostPerRow = 0.1
  val PROBE_BUILD_COST: CostPerRow = 3.1
  val PROBE_SEARCH_COST: CostPerRow = 2.4
  // A property has at least 2 db hits, even though it could even have many more.
  val PROPERTY_ACCESS_DB_HITS = 2
  val LABEL_CHECK_DB_HITS = 1
  val EXPAND_INTO_COST: CostPerRow = 6.4

  val INDEX_SCAN_COST_PER_ROW = 1.0
  val INDEX_SEEK_COST_PER_ROW = 1.9
  // When reading from node store or relationship store
  val STORE_LOOKUP_COST_PER_ROW = 6.2

  /**
   * PartialSort always has to Sort whole buckets, even when under a Limit.
   * The work reduction from the Limit does therefore not propagate 100 % to the
   * child of the PartialSort.
   *
   * We have no way of determining the size of buckets currently, so we simply guess
   * that the work of the child is 10 % more than the work of the PartialSort.
   */
  val PARTIAL_SORT_WORK_INCREASE = 0.1

  /**
   * The cost of evaluating an expression, per row.
   */
  def costPerRowFor(expression: Expression, semanticTable: SemanticTable): CostPerRow = {
    val noOfStoreAccesses = expression.treeFold(0) {
      case x: Property if semanticTable.isNodeNoFail(x.map) || semanticTable.isRelationshipNoFail(x.map) => count => TraverseChildren(count + PROPERTY_ACCESS_DB_HITS)
      case cp: CachedProperty if cp.knownToAccessStore => count => TraverseChildren(count + PROPERTY_ACCESS_DB_HITS)
      case cp: CachedHasProperty if cp.knownToAccessStore => count => TraverseChildren(count + PROPERTY_ACCESS_DB_HITS)
      case _: HasLabels |
           _: HasTypes |
           _: HasLabelsOrTypes => count => TraverseChildren(count + LABEL_CHECK_DB_HITS)
      case _ => count => TraverseChildren(count)
    }
    if (noOfStoreAccesses > 0)
      CostPerRow(noOfStoreAccesses)
    else
      DEFAULT_COST_PER_ROW
  }

  /**
   * @param plan the plan
   * @param cardinality the outgoing cardinality of the plan ???
   * @param semanticTable the semantic table
   * @return the cost of the plan per outgoing row
   */
  private def costPerRow(plan: LogicalPlan, cardinality: Cardinality, semanticTable: SemanticTable): CostPerRow = plan match {
    /*
     * These constants are approximations derived from test runs,
     * see ActualCostCalculationTest
     */

    /*
     * Ties that may occur between leaf plans (e.g. label/type scans and index plans) are arbitrated by a SelectorHeuristic
     */

    case _: NodeByLabelScan |
         _: NodeIndexScan
    => INDEX_SCAN_COST_PER_ROW

    case _: ProjectEndpoints
    => STORE_LOOKUP_COST_PER_ROW

    case Selection(predicate, _)
    => costPerRowFor(predicate, semanticTable)

    case _: AllNodesScan
    => 1.2

    case e: Expand if e.mode == ExpandInto
    => EXPAND_INTO_COST

    case e: VarExpand if e.mode == ExpandInto
    => EXPAND_INTO_COST

    case _: Expand |
         _: VarExpand
    => 1.5

    case _: NodeUniqueIndexSeek |
         _: NodeIndexSeek |
         _: NodeIndexContainsScan |
         _: NodeIndexEndsWithScan
    => INDEX_SEEK_COST_PER_ROW

    case _: NodeByIdSeek |
         _: DirectedRelationshipByIdSeek
    => STORE_LOOKUP_COST_PER_ROW

    case _: UndirectedRelationshipByIdSeek
      // Only every second row needs to access the store
    => STORE_LOOKUP_COST_PER_ROW / 2

    case
      _: DirectedRelationshipTypeScan |
      _: DirectedRelationshipIndexScan
    => INDEX_SCAN_COST_PER_ROW + STORE_LOOKUP_COST_PER_ROW

    case
      _: UndirectedRelationshipTypeScan |
      _: UndirectedRelationshipIndexScan
      // Only every second row needs to access the index and the store
    => (INDEX_SCAN_COST_PER_ROW + STORE_LOOKUP_COST_PER_ROW) / 2

    case
      _: DirectedRelationshipIndexSeek |
      _: DirectedRelationshipIndexContainsScan |
      _: DirectedRelationshipIndexEndsWithScan
    => INDEX_SEEK_COST_PER_ROW + STORE_LOOKUP_COST_PER_ROW

    case
      _: UndirectedRelationshipIndexSeek |
      _: UndirectedRelationshipIndexContainsScan |
      _: UndirectedRelationshipIndexEndsWithScan
      // Only every second row needs to access the index and the store
    => (INDEX_SEEK_COST_PER_ROW + STORE_LOOKUP_COST_PER_ROW) / 2

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

  /**
   * The input cardinality if defined, otherwise the output cardinality
   */
  private def inputCardinality(plan: LogicalPlan, cardinalities: Cardinalities): Cardinality =
    plan.lhs.map(p => cardinalities.get(p.id)).getOrElse(outputCardinality(plan, cardinalities))

  private def outputCardinality(plan: LogicalPlan, cardinalities: Cardinalities): Cardinality =
    cardinalities.get(plan.id)

  final case class EffectiveCardinalities(
    outputCardinality: Cardinality,
    inputCardinality: Cardinality,
    lhs: Cardinality,
    rhs: Cardinality,
    lhsReduction: WorkReduction,
    rhsReduction: WorkReduction,
  )

  /**
   * The limit selectivity of a limiting plan.
   *
   * @param inputCardinality        the cardinality of the plan's parent
   * @param outputCardinality       the cardinality of plan
   * @param parentLimitSelectivity  the limit selectivity of the plan's parent
   */
  def limitingPlanSelectivity(inputCardinality: Cardinality, outputCardinality: Cardinality, parentLimitSelectivity: Selectivity): Selectivity =
    limitingPlanWorkReduction(inputCardinality, outputCardinality, WorkReduction(parentLimitSelectivity)).fraction

  /**
   * The work reduction of a limiting plan.
   *
   * @param inputCardinality        the cardinality of the plan's parent
   * @param outputCardinality       the cardinality of plan
   * @param parentWorkReduction     the work reduction of the plan's parent
   */
  def limitingPlanWorkReduction(inputCardinality: Cardinality, outputCardinality: Cardinality, parentWorkReduction: WorkReduction): WorkReduction = {
    val reducedOutput = parentWorkReduction.calculate(outputCardinality)
    val fraction = (reducedOutput / inputCardinality) getOrElse Selectivity.ONE
    parentWorkReduction.withFraction(fraction)
  }

  /**
   * Given an incoming WorkReduction, calculate how this reduction applies to the LHS and RHS of the plan.
   *
   */
  private[logical] def effectiveCardinalities(plan: LogicalPlan, workReduction: WorkReduction, batchSize: SelectedBatchSize, cardinalities: Cardinalities): EffectiveCardinalities = {
    val (lhsWorkReduction, rhsWorkReduction) = childrenWorkReduction(plan, workReduction, batchSize, cardinalities)

    // Make sure argument leaf plans under semiApply etc. get bounded to the same cardinality as the lhs
    val useMinimum = plan match {
      case _: Argument => true
      case _           => false
    }

    EffectiveCardinalities(
      outputCardinality = workReduction.calculate(outputCardinality(plan, cardinalities), useMinimum),
      inputCardinality = lhsWorkReduction.calculate(inputCardinality(plan, cardinalities), useMinimum),
      plan.lhs.map(p => lhsWorkReduction.calculate(cardinalities.get(p.id))) getOrElse Cardinality.EMPTY,
      plan.rhs.map(p => rhsWorkReduction.calculate(cardinalities.get(p.id))) getOrElse Cardinality.EMPTY,
      lhsWorkReduction,
      rhsWorkReduction,
    )
  }

  /**
   * Given an parent WorkReduction, calculate how this reduction applies to the LHS and RHS of the plan.
   */
  private[logical] def childrenWorkReduction(plan: LogicalPlan, parentWorkReduction: WorkReduction, batchSize: SelectedBatchSize, cardinalities: Cardinalities): (WorkReduction, WorkReduction) = plan match {

    //NOTE: we don't match on ExhaustiveLimit here since that doesn't affect the cardinality of earlier plans
    case p: LimitingLogicalPlan =>
      val inputCardinality = cardinalities.get(p.source.id)
      val outputCardinality = cardinalities.get(p.id)
      val reduction = limitingPlanWorkReduction(inputCardinality, outputCardinality, parentWorkReduction)
      (reduction, reduction)

    case p: SingleFromRightLogicalPlan =>
      val rhsCardinality = cardinalities.get(p.inner.id)
      // The RHS is reduced by 1/rhsCardinality.
      // Any existing work reduction does not propagate into the RHS, since that does not influence the amount of rows _per loop invocation_.
      // It will, however, reduce the amount of effective rows after `recordEffectiveOutputCardinality` has multiplied the RHS with the LHS cardinality.
      val rhsReduction = limitingPlanWorkReduction(rhsCardinality, Cardinality.SINGLE, WorkReduction.NoReduction).copy(minimum = Some(Cardinality.SINGLE))
      (parentWorkReduction, rhsReduction)

    // if there is no parentWorkReduction, all cases below are unnecessary, so let's skip doing unnecessary work
    case _ if parentWorkReduction == WorkReduction.NoReduction =>
      (parentWorkReduction, parentWorkReduction)


    case _: ForeachApply =>
      // ForeachApply is an ApplyPlan, but only yields LHS rows, therefore we match this before matching ApplyPlan
      (parentWorkReduction, WorkReduction.NoReduction)

    case a:ApplyPlan =>
      nestedLoopChildrenWorkReduction(a, parentWorkReduction, VolcanoBatchSize, cardinalities)

    case p: CartesianProduct =>
      nestedLoopChildrenWorkReduction(p, parentWorkReduction, batchSize, cardinalities)

    case _: AssertSameNode =>
      (parentWorkReduction, WorkReduction.NoReduction)

    case u:Union =>
      // number of rows available from lhs/rhs plan
      val lhsCardinality: Cardinality = cardinalities.get(u.left.id)
      val rhsCardinality: Cardinality = cardinalities.get(u.right.id)

      val parentEffectiveCardinality = cardinalities.get(u.id) * parentWorkReduction.fraction

      val (lhsEffectiveCardinality, rhsEffectiveCardinality) = if (lhsCardinality > parentEffectiveCardinality) {
        (parentEffectiveCardinality, Cardinality.EMPTY)
      } else {
        (lhsCardinality, parentEffectiveCardinality - lhsCardinality)
      }

      val lhsFraction = (lhsEffectiveCardinality / lhsCardinality).getOrElse(Selectivity.ONE)
      val rhsFraction = (rhsEffectiveCardinality / rhsCardinality).getOrElse(Selectivity.ONE)

      (parentWorkReduction.withFraction(lhsFraction), parentWorkReduction.withFraction(rhsFraction))

    case _: OrderedUnion =>
      (parentWorkReduction, parentWorkReduction)

    case HashJoin() =>
      (WorkReduction.NoReduction, parentWorkReduction)

    case _:PartialSort =>
      // Let's assume the child has to do "a little more" work.
      // This happens because PartialSort has to process at least a whole bucket of identical values.
      val parentFraction = parentWorkReduction.fraction.factor
      val childFraction = Math.min(1.0, parentFraction * (1.0 + PARTIAL_SORT_WORK_INCREASE))
      (parentWorkReduction.withFraction(Selectivity(childFraction)), WorkReduction.NoReduction)

    case _: ExhaustiveLogicalPlan =>
      (WorkReduction.NoReduction, WorkReduction.NoReduction)

    case _: LogicalUnaryPlan =>
      (parentWorkReduction, WorkReduction.NoReduction)

    case _:LogicalLeafPlan =>
      (parentWorkReduction, WorkReduction.NoReduction)

    case lp:LogicalBinaryPlan =>
      // Forces us to hopefully think about this when adding new binary plans
      AssertMacros.checkOnlyWhenAssertionsAreEnabled(false, s"childrenWorkReduction: No case for ${lp.getClass.getSimpleName} added.")
      (parentWorkReduction, WorkReduction.NoReduction)
  }

  /**
   * Given an parent WorkReduction, calculate how this reduction applies to the LHS and RHS of this plan that implements a nested loop.
   */
  private def nestedLoopChildrenWorkReduction(plan: LogicalBinaryPlan, parentWorkReduction: WorkReduction, batchSize: SelectedBatchSize, cardinalities: Cardinalities): (WorkReduction, WorkReduction) = {
    // number of rows available from lhs/rhs plan
    val lhsCardinality: Cardinality = cardinalities.get(plan.left.id)
    val rhsCardinality: Cardinality = cardinalities.get(plan.right.id)

    // smallest number of rows we can get from lhs/rhs
    val chunkSize: Cardinality = Cardinality.min(batchSize.size, lhsCardinality)
    val rhsRowsMinimum: Cardinality = Cardinality.min(batchSize.size, rhsCardinality)

    // number of rows we can produce per execution of rhs
    val outputRowsPerExecution: Cardinality = chunkSize * rhsCardinality

    // round required output to nearest multiple of the smallest output chunk
    val outputChunk = chunkSize * rhsRowsMinimum
    val requiredOutput: Cardinality = outputChunk * Multiplier.ofDivision(parentWorkReduction.calculate(cardinalities.get(plan.id)), outputChunk).getOrElse(Multiplier.ZERO).ceil

    // number of executions needed to produce the required output
    val rhsExecutions: Multiplier = Multiplier.ofDivision(requiredOutput, outputRowsPerExecution).getOrElse(Multiplier.ZERO)
    val lhsFullBatches: Cardinality = Cardinality(rhsExecutions.coefficient).ceil

    // total number of rows fetched from lhs/rhs
    val lhsProducedRows: Cardinality = Cardinality.min(lhsFullBatches * chunkSize, lhsCardinality)
    val rhsProducedRows: Cardinality = Cardinality.max(rhsExecutions * rhsCardinality, rhsRowsMinimum)

    val rhsProducedRowsPerExecution = Cardinality(Multiplier.ofDivision(rhsProducedRows * chunkSize, lhsProducedRows).getOrElse(Multiplier.ZERO).coefficient)

    val lhsFraction = (lhsProducedRows / lhsCardinality).getOrElse(Selectivity.ONE)
    val rhsFraction = (rhsProducedRowsPerExecution / rhsCardinality).getOrElse(Selectivity.ONE)

    (parentWorkReduction.withFraction(lhsFraction), parentWorkReduction.withFraction(rhsFraction))
  }

  private[logical] def getEffectiveBatchSize(batchSize: SelectedBatchSize, plan: LogicalPlan, providedOrders: ProvidedOrders): SelectedBatchSize = {
    plan match {
      // Ideally we would want to check leveragedOrders here. But that attribute will not be set properly at this point of planning,
      // since that only happens when the leveraging plan is planned.
      case cp:CartesianProduct if !providedOrders(cp.id).isEmpty => ExecutionModel.VolcanoBatchSize // A CartesianProduct that provides order is rewritten to execute in a row-by-row fashion
      case _ => batchSize
    }
  }

  private object HashJoin {
    def unapply(x: LogicalPlan): Boolean = x match {
      case _: NodeHashJoin |
           _: LeftOuterHashJoin |
           _: RightOuterHashJoin |
           _: ValueHashJoin => true
      case _ => false
    }
  }
}
