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
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.EffectiveCardinalities
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.HashJoin
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.PROBE_BUILD_COST
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.PROBE_SEARCH_COST
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.costPerRow
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.inputCardinality
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.limitingPlanWorkReduction
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.outputCardinality
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CostModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
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
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.ExhaustiveLimit
import org.neo4j.cypher.internal.logical.plans.ExhaustiveLogicalPlan
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.ExpandInto
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LimitingLogicalPlan
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
import org.neo4j.cypher.internal.logical.plans.SingleFromRightLogicalPlan
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.logical.plans.VarExpand
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
   * Calculate the combined cost of a plan, including the costs of its children.
   *
   * @param plan                   the plan
   * @param effectiveCardinalities the effective cardinalities for this plan, the LHS and the RHS
   * @param cardinalities          the cardinalities without applying any limit selectivities to them
   * @param rootPlan               the whole plan currently calculating cost for.
   */
  private def combinedCostForPlan(plan: LogicalPlan,
                                  effectiveCardinalities: EffectiveCardinalities,
                                  cardinalities: Cardinalities,
                                  providedOrders: ProvidedOrders,
                                  lhsCost: Cost,
                                  rhsCost: Cost,
                                  semanticTable: SemanticTable,
                                  rootPlan: LogicalPlan,
                                  batchSize: SelectedBatchSize): Cost = plan match {
    case cp: CartesianProduct =>
      val lhsCardinality = Cardinality.max(Cardinality.SINGLE, effectiveCardinalities.lhs)
      // Ideally we would want to check leveragedOrders here. But that attribute will not be set properly at this point of planning,
      // since that only happens when the leveraging plan is planned.
      val providesOrder = !providedOrders(cp.id).isEmpty
      // A CartesianProduct that provides order is rewritten to execute in a row-by-row fashion

      val effectiveBatchSize = if (providesOrder) ExecutionModel.VolcanoBatchSize else batchSize

      // Batched: The RHS is executed for each batch of LHS rows
      // Volcano: The RHS is executed for each LHS row
      val rhsExecutions = effectiveBatchSize.numBatchesFor(lhsCardinality)
      lhsCost + rhsExecutions * rhsCost

    case _: ApplyPlan =>
      // the rCost has already been multiplied by the lhs cardinality
      lhsCost + rhsCost

    case HashJoin() =>
      lhsCost + rhsCost +
        effectiveCardinalities.lhs * PROBE_BUILD_COST +
        effectiveCardinalities.rhs * PROBE_SEARCH_COST

    case _ =>
      val rowCost = costPerRow(plan, effectiveCardinalities.inputCardinality, semanticTable)
      val costForThisPlan = effectiveCardinalities.inputCardinality * rowCost
      costForThisPlan + lhsCost + rhsCost
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

    val effectiveCard = effectiveCardinalities(plan, workReduction, batchSize, cardinalities)

    val lhsCost = plan.lhs.map(p => calculateCost(p, effectiveCard.lhsReduction, cardinalities, providedOrders, semanticTable, rootPlan, batchSize, monitor)) getOrElse Cost.ZERO
    val rhsCost = plan.rhs.map(p => calculateCost(p, effectiveCard.rhsReduction, cardinalities, providedOrders, semanticTable, rootPlan, batchSize, monitor)) getOrElse Cost.ZERO

    val cost = combinedCostForPlan(plan, effectiveCard, cardinalities, providedOrders, lhsCost, rhsCost, semanticTable, rootPlan, batchSize)

    monitor.reportPlanCost(rootPlan, plan, cost)
    monitor.reportPlanEffectiveCardinality(rootPlan, plan, effectiveCard.outputCardinality)

    cost
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
   *
   */
  private[logical] def childrenWorkReduction(plan: LogicalPlan, parentWorkReduction: WorkReduction, batchSize: SelectedBatchSize, cardinalities: Cardinalities): (WorkReduction, WorkReduction) = plan match {

    case p: CartesianProduct =>
      // number of rows available from lhs/rhs plan
      val lhsCardinality: Cardinality = cardinalities.get(p.left.id)
      val rhsCardinality: Cardinality = cardinalities.get(p.right.id)

      // smallest number of rows we can get from lhs/rhs
      val chunkSize: Cardinality = Cardinality.min(batchSize.size, lhsCardinality)
      val rhsRowsMinimum: Cardinality = Cardinality.min(batchSize.size, rhsCardinality)

      // number of rows we can produce per execution of rhs
      val outputRowsPerExecution: Cardinality = chunkSize * rhsCardinality

      // round required output to nearest multiple of the smallest output chunk
      val outputChunk = chunkSize * rhsRowsMinimum
      val requiredOutput: Cardinality = Math.ceil((parentWorkReduction.calculate(cardinalities.get(p.id)) * outputChunk.inverse).amount) * outputChunk

      // number of executions needed to produce the required output
      val rhsExecutions: Multiplier = Multiplier.ofDivision(requiredOutput, outputRowsPerExecution)
      val lhsFullBatches: Cardinality = Cardinality(rhsExecutions.coefficient).ceil

      // total number of rows fetched from lhs/rhs
      val lhsProducedRows: Cardinality = Cardinality.min(lhsFullBatches * chunkSize, lhsCardinality)
      val rhsProducedRows: Cardinality = Cardinality.max(rhsExecutions * rhsCardinality, rhsRowsMinimum)

      val rhsProducedRowsPerExecution = rhsProducedRows * chunkSize * lhsProducedRows.inverse

      val lhsFraction = (lhsProducedRows / lhsCardinality).getOrElse(Selectivity.ONE)
      val rhsFraction = (rhsProducedRowsPerExecution / rhsCardinality).getOrElse(Selectivity.ONE)

      (parentWorkReduction.withFraction(lhsFraction), parentWorkReduction.withFraction(rhsFraction))

    //NOTE: we don't match on ExhaustiveLimit here since that doesn't affect the cardinality of earlier plans
    case p: LimitingLogicalPlan =>
      val inputCardinality = cardinalities.get(p.source.id)
      val outputCardinality = cardinalities.get(p.id)
      val reduction = limitingPlanWorkReduction(inputCardinality, outputCardinality, parentWorkReduction)
      (reduction, reduction)

    case p: SingleFromRightLogicalPlan =>
      val lhsCardinality = cardinalities.get(p.source.id)
      val rhsCardinality = cardinalities.get(p.inner.id)
      val effectiveLhsCardinality = parentWorkReduction.calculate(cardinalities.get(p.source.id))
      val rhsReduction = limitingPlanWorkReduction(rhsCardinality, lhsCardinality, parentWorkReduction).copy(minimum = Some(effectiveLhsCardinality))
      (parentWorkReduction, rhsReduction)

    case HashJoin() =>
      (WorkReduction.NoReduction, parentWorkReduction)

    case _: ExhaustiveLogicalPlan =>
      (WorkReduction.NoReduction, WorkReduction.NoReduction)

    case _ =>
      (parentWorkReduction, parentWorkReduction)
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

  /**
   * The cost of evaluating an expression, per row.
   */
  def costPerRowFor(expression: Expression, semanticTable: SemanticTable): CostPerRow = {
    val noOfStoreAccesses = expression.treeFold(0) {
      case x: Property if semanticTable.isNodeNoFail(x.map) || semanticTable.isRelationshipNoFail(x.map) => count => TraverseChildren(count + PROPERTY_ACCESS_DB_HITS)
      case cp: CachedProperty if cp.knownToAccessStore => count => TraverseChildren(count + PROPERTY_ACCESS_DB_HITS)
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

    case _: NodeByLabelScan |
         _: NodeIndexScan |
         _: ProjectEndpoints
    => 1.0

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
    => 1.9

    case _: NodeByIdSeek |
         _: DirectedRelationshipByIdSeek |
         _: UndirectedRelationshipByIdSeek |
         _: DirectedRelationshipTypeScan |
         _: UndirectedRelationshipTypeScan
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
