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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.ExecutionModel.SelectedBatchSize
import org.neo4j.cypher.internal.compiler.ExecutionModel.VolcanoBatchSize
import org.neo4j.cypher.internal.compiler.helpers.PropertyAccessHelper.PropertyAccess
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.ALL_SCAN_COST_PER_ROW
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.EffectiveCardinalities
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.HashJoin
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.PROBE_BUILD_COST
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.PROBE_SEARCH_COST
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.costPerRow
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.effectiveCardinalities
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.getEffectiveBatchSize
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.hackyRelTypeScanCost
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CostModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence.RepetitionCardinalityModel
import org.neo4j.cypher.internal.expressions.AndedPropertyInequalities
import org.neo4j.cypher.internal.expressions.CachedHasProperty
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.HasLabelsOrTypes
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.InequalityExpression
import org.neo4j.cypher.internal.expressions.LogicalProperty
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.logical.plans.AbstractLetSemiApply
import org.neo4j.cypher.internal.logical.plans.AbstractSemiApply
import org.neo4j.cypher.internal.logical.plans.AggregatingPlan
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.ApplyPlan
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.AssertSameRelationship
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.DirectedAllRelationshipsScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByElementIdSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.DirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.ExhaustiveLimit
import org.neo4j.cypher.internal.logical.plans.ExhaustiveLogicalPlan
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.IntersectionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LimitingLogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalBinaryPlan
import org.neo4j.cypher.internal.logical.plans.LogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlanExtension
import org.neo4j.cypher.internal.logical.plans.LogicalUnaryPlan
import org.neo4j.cypher.internal.logical.plans.NodeByElementIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.NodeIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.logical.plans.OrderedUnion
import org.neo4j.cypher.internal.logical.plans.PartialSort
import org.neo4j.cypher.internal.logical.plans.PartitionedScanPlan
import org.neo4j.cypher.internal.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.logical.plans.ProjectEndpoints
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.SingleFromRightLogicalPlan
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.logical.plans.SubtractionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.Trail
import org.neo4j.cypher.internal.logical.plans.UndirectedAllRelationshipsScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByElementIdSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.UndirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.UnionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Cost
import org.neo4j.cypher.internal.util.CostPerRow
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.Multiplier
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.cypher.internal.util.WorkReduction
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship

case class CardinalityCostModel(executionModel: ExecutionModel, cancellationChecker: CancellationChecker)
    extends CostModel {

  override def costFor(
    plan: LogicalPlan,
    input: QueryGraphSolverInput,
    semanticTable: SemanticTable,
    cardinalities: Cardinalities,
    providedOrders: ProvidedOrders,
    propertyAccess: Set[PropertyAccess],
    statistics: GraphStatistics,
    monitor: CostModelMonitor
  ): Cost = {
    // The plan we use here to select the batch size will obviously not be the final plan for the whole query.
    // So it could very well be that we select the small chunk size here for cost estimation purposes, but the cardinalities increase
    // in the later course of the plan and it will actually be executed with the big chunk size.
    val batchSize = executionModel.selectBatchSize(plan, cardinalities, cancellationChecker)
    calculateCost(
      plan,
      WorkReduction(input.limitSelectivity),
      cardinalities,
      providedOrders,
      semanticTable,
      plan,
      batchSize,
      propertyAccess,
      statistics,
      monitor
    )
  }

  /**
   * Recursively calculate the cost of a plan
   *
   * @param plan          the plan
   * @param workReduction expected work reduction due to limits and laziness
   * @param rootPlan      the whole plan currently calculating cost for.
   */
  private def calculateCost(
    plan: LogicalPlan,
    workReduction: WorkReduction,
    cardinalities: Cardinalities,
    providedOrders: ProvidedOrders,
    semanticTable: SemanticTable,
    rootPlan: LogicalPlan,
    batchSize: SelectedBatchSize,
    propertyAccess: Set[PropertyAccess],
    statistics: GraphStatistics,
    monitor: CostModelMonitor
  ): Cost = {

    val effectiveBatchSize = getEffectiveBatchSize(batchSize, plan, providedOrders)

    val effectiveCard = effectiveCardinalities(plan, workReduction, effectiveBatchSize, cardinalities)

    val lhsCost = plan.lhs.map(p =>
      calculateCost(
        p,
        effectiveCard.lhsReduction,
        cardinalities,
        providedOrders,
        semanticTable,
        rootPlan,
        batchSize,
        propertyAccess,
        statistics,
        monitor
      )
    ) getOrElse Cost.ZERO
    val rhsCost = plan.rhs.map(p =>
      calculateCost(
        p,
        effectiveCard.rhsReduction,
        cardinalities,
        providedOrders,
        semanticTable,
        rootPlan,
        batchSize,
        propertyAccess,
        statistics,
        monitor
      )
    ) getOrElse Cost.ZERO

    val cost =
      combinedCostForPlan(
        plan,
        effectiveCard,
        cardinalities,
        lhsCost,
        rhsCost,
        semanticTable,
        effectiveBatchSize,
        propertyAccess,
        statistics
      )

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
  private def combinedCostForPlan(
    plan: LogicalPlan,
    effectiveCardinalities: EffectiveCardinalities,
    cardinalities: Cardinalities,
    lhsCost: Cost,
    rhsCost: Cost,
    semanticTable: SemanticTable,
    batchSize: SelectedBatchSize,
    propertyAccess: Set[PropertyAccess],
    statistics: GraphStatistics
  ): Cost = {

    /**
     * For the cost comparisons of AllRelationshipScan/RelationshipTypeScan vs AllNodesScan + Expand
     * we apply special logic the match below. This logic is even valid if there are any number of
     * cardinality preserving unary plans between the AllNodesScan and the Expand, thus this extractor object.
     */
    object AllNodesScanIsh {
      def unapply(v: LogicalPlan): Boolean = v match {
        case _: AllNodesScan                                                                                   => true
        case lup @ LogicalUnaryPlan(ans @ AllNodesScanIsh()) if cardinalities(lup.id) == cardinalities(ans.id) => true
        case _                                                                                                 => false
      }
    }

    plan match {
      case _: CartesianProduct =>
        val lhsCardinality = Cardinality.max(Cardinality.SINGLE, effectiveCardinalities.lhs)

        // Batched: The RHS is executed for each batch of LHS rows
        // Volcano: The RHS is executed for each LHS row
        val rhsExecutions = batchSize.numBatchesFor(lhsCardinality)
        lhsCost + rhsExecutions * rhsCost

      case t: Trail =>
        val lhsCardinality = effectiveCardinalities.lhs
        val rhsCardinality = effectiveCardinalities.rhs

        val qppRange = RepetitionCardinalityModel.quantifiedPathPatternRepetitionAsRange(t.repetition)

        // For iteration 1 the RHS executes with LHS cardinality.
        val iteration1Cost =
          if (qppRange.start == 0 && qppRange.end == 0) Cost(0)
          else lhsCardinality * rhsCost

        // Starting from iteration 2 the RHS executes with the cardinality of the previous RHS iteration.
        // We don't have separate estimations for the RHS iterations, so we simply use
        // lhsCardinality * rhsCardinality, assuming each iteration returns the same amount of rows.
        // In the future, if there are more alternatives that can solve a QPP, we should improve this.
        // In NodeConnectionMultiplierCalculator we estimate the separate iterations, so we could extract that logic
        // to be used here as well.

        // We also disregard a supplied min, since we need to execute iterations up to min, even if their result
        // is not yielded.
        val iterationsNCost = {
          val from2Range = 2 to qppRange.end
          val rhsInvocations = from2Range.length * lhsCardinality * rhsCardinality
          rhsInvocations * rhsCost
        }

        lhsCost + iteration1Cost + iterationsNCost

      case _: ApplyPlan =>
        val lhsCardinality = effectiveCardinalities.lhs
        // The RHS is executed for each LHS row
        lhsCost + lhsCardinality * rhsCost

      case HashJoin() =>
        lhsCost + rhsCost +
          effectiveCardinalities.lhs * PROBE_BUILD_COST +
          effectiveCardinalities.rhs * PROBE_SEARCH_COST

      case _: Union | _: OrderedUnion =>
        val inCardinality = effectiveCardinalities.lhs + effectiveCardinalities.rhs
        val rowCost = costPerRow(plan, inCardinality, semanticTable, propertyAccess)
        val costForThisPlan = inCardinality * rowCost
        costForThisPlan + lhsCost + rhsCost

      // NOTE: Expand generally gets underestimated since they are treated as a middle operator
      // like Selection which doesn't reflect that for each row it creates it will read data from
      // the relationship store. This particular special case is just for making it more likely to plan
      // AllRelationshipsScan since we know they are always faster than doing AllNodes + Expand
      case Expand(AllNodesScanIsh(), _, _, types, _, _, ExpandAll) if types.isEmpty =>
        // AllNodes + Expand is more expensive than scanning the relationship directly
        val rowCost = CostPerRow(ALL_SCAN_COST_PER_ROW * 1.1)
        // Note: we use the outputCardinality to compute the cost
        val costForThisPlan = effectiveCardinalities.outputCardinality * rowCost
        costForThisPlan + lhsCost + rhsCost

      // Always consider AllNodesScan + Expand more expensive than RelationshipTypeScan
      case exp @ Expand(AllNodesScanIsh(), _, _, types, _, _, ExpandAll) if types.size == 1 =>
        val rowCost =
          CostPerRow(1.1 * hackyRelTypeScanCost(propertyAccess, exp.relName, exp.dir != SemanticDirection.BOTH))
        // Note: we use the outputCardinality to compute the cost
        val costForThisPlan = effectiveCardinalities.outputCardinality * rowCost
        costForThisPlan + lhsCost + rhsCost

      case UnionNodeByLabelsScan(_, labels, _, _) =>
        val rowCost = costPerRow(plan, effectiveCardinalities.inputCardinality, semanticTable, propertyAccess)
        val nextCallsForCursor = labels.map(l => statistics.nodesWithLabelCardinality(semanticTable.id(l)).amount).sum
        Cardinality(nextCallsForCursor) * rowCost

      case IntersectionNodeByLabelsScan(_, labels, _, _) =>
        // We don't use the outgoing cardinality to compute the cost here since for doing the intersection
        // scan we will need to exhaust at least the label with the smallest cardinality to find all intersecting nodes.
        // For example, given (a:A&B) where we have 10 nodes with label A, 100 nodes with label B but only 1 node with A&B.
        // Using a cardinality of 1 to compute the cost would underestimate the work that is needed since it will at least
        // need to visit all 10 A nodes to find all the intersecting nodes.
        val nextCallsForCursor = labels.map(l => statistics.nodesWithLabelCardinality(semanticTable.id(l))).min
        val rowCost = costPerRow(plan, effectiveCardinalities.inputCardinality, semanticTable, propertyAccess)
        nextCallsForCursor * rowCost

      case SubtractionNodeByLabelsScan(_, positiveLabels, negativeLabels, _, _) =>
        val rowCost = costPerRow(plan, effectiveCardinalities.inputCardinality, semanticTable, propertyAccess)
        // Need to go over the intersection of the nodes with the positive labels once
        // And over the union of the nodes with the negative labels once
        // MATCH (n:A&B&!C&!D)
        // Consider all the following nodes: (n1:A), (n2:B), (n3:C), (n4:D), (n5:A&B), (n6:A&B&C), (n7:A&B&C&D)
        // Nodes with all positive labels: n5, n6, n7
        // Nodes with any negative label:  n3, n4, n6, n7
        // The set difference gives all resulting nodes of the subtraction scan: n5
        val nextCallsForPositiveCursor =
          positiveLabels.map(l => statistics.nodesWithLabelCardinality(semanticTable.id(l)).amount).min
        // Take the worst case where the sets are disjoint (i.e. the sum of the set cardinalities)
        val negativeLabelsCardinalitySum = negativeLabels.map(l =>
          statistics.nodesWithLabelCardinality(semanticTable.id(l)).amount
        ).sum
        val nextCallsForNegativeCursor = math.min(nextCallsForPositiveCursor, negativeLabelsCardinalitySum)

        Cardinality(nextCallsForPositiveCursor + nextCallsForNegativeCursor) * rowCost

      case _ =>
        val rowCost = costPerRow(plan, effectiveCardinalities.inputCardinality, semanticTable, propertyAccess)
        // Note: By default we use the inputCardinality to compute the cost
        val costForThisPlan = effectiveCardinalities.inputCardinality * rowCost
        costForThisPlan + lhsCost + rhsCost
    }
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
  val EXPAND_ALL_COST: CostPerRow = 1.5
  val ALL_SCAN_COST_PER_ROW = 1.2

  val SHORTEST_INTO_COST = 12.0
  val SHORTEST_PRODUCT_GRAPH_COST = 18.0

  val INDEX_SCAN_COST_PER_ROW = 1.0
  val INDEX_SEEK_COST_PER_ROW = 1.9
  // When reading from node store or relationship store
  val STORE_LOOKUP_COST_PER_ROW = 6.2

  val DIRECTED_RELATIONSHIP_INDEX_SCAN_COST_PER_ROW = INDEX_SCAN_COST_PER_ROW + STORE_LOOKUP_COST_PER_ROW

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
    val noOfStoreAccesses = calculateNumberOfStoreAccesses(expression, semanticTable)
    if (noOfStoreAccesses > 0)
      CostPerRow(noOfStoreAccesses)
    else
      DEFAULT_COST_PER_ROW
  }

  def calculateNumberOfStoreAccesses(expression: Expression, semanticTable: SemanticTable): Int =
    expression.folder.treeFold(0) {
      case AndedPropertyInequalities(_: LogicalVariable, _: LogicalProperty, _: NonEmptyList[InequalityExpression]) =>
        count =>
          TraverseChildren(count - PROPERTY_ACCESS_DB_HITS) // Ignore the `property` grouping key in `AndedPropertyInequalities`, only count properties inside `properties`.
      case x: Property if semanticTable.typeFor(x.map).isAnyOf(CTNode, CTRelationship) =>
        count => TraverseChildren(count + PROPERTY_ACCESS_DB_HITS)
      case cp: CachedProperty if cp.knownToAccessStore    => count => TraverseChildren(count + PROPERTY_ACCESS_DB_HITS)
      case cp: CachedHasProperty if cp.knownToAccessStore => count => TraverseChildren(count + PROPERTY_ACCESS_DB_HITS)
      case _: HasLabels |
        _: HasTypes |
        _: HasLabelsOrTypes => count => TraverseChildren(count + LABEL_CHECK_DB_HITS)
      case _ => count => TraverseChildren(count)
    }

  def hackyRelTypeScanCost(
    propertyAccess: Set[PropertyAccess],
    relVariable: LogicalVariable,
    directed: Boolean
  ): Double = {
    // A workaround for cases where we might get value from an index scan instead. Using the same cost means we will use leaf plan heuristic to decide.
    if (propertyAccess.exists(_.variable == relVariable)) {
      // If undirected only every second row needs to access the index and the store
      val multiplier = if (directed) 1.0 else 0.5
      DIRECTED_RELATIONSHIP_INDEX_SCAN_COST_PER_ROW * multiplier
    } else {
      val allNodeScanCostMultiplier = if (directed) 2.2 else 1.3
      ALL_SCAN_COST_PER_ROW * allNodeScanCostMultiplier
    }
  }

  /**
   * @param plan the plan
   * @param cardinality the input cardinality of the plan
   * @param semanticTable the semantic table
   * @return the cost of the plan per incoming row, if defined.
   *         For leaf plans, the cost of the plan per outgoing row.
   */
  private def costPerRow(
    plan: LogicalPlan,
    cardinality: Cardinality,
    semanticTable: SemanticTable,
    propertyAccess: Set[PropertyAccess]
  ): CostPerRow =
    plan match {
      /*
       * These constants are approximations derived from test runs.
       */
      /*
       * Ties that may occur between leaf plans (e.g. label/type scans and index plans) are arbitrated by a SelectorHeuristic
       */

      case _: NodeByLabelScan |
        _: UnionNodeByLabelsScan |
        _: NodeIndexScan => INDEX_SCAN_COST_PER_ROW

      case plan: IntersectionNodeByLabelsScan =>
        // A workaround for cases where we might get value from an index scan instead. Using the same cost means we will use leaf plan heuristic to decide.
        if (propertyAccess.exists(_.variable == plan.idName)) {
          INDEX_SCAN_COST_PER_ROW + STORE_LOOKUP_COST_PER_ROW
        } else {
          INDEX_SCAN_COST_PER_ROW
        }

      case _: ProjectEndpoints => STORE_LOOKUP_COST_PER_ROW

      case Selection(predicate, _) => costPerRowFor(predicate, semanticTable)

      case _: AllNodesScan => ALL_SCAN_COST_PER_ROW

      case e: OptionalExpand if e.mode == ExpandInto => EXPAND_INTO_COST

      case e: Expand if e.mode == ExpandInto => EXPAND_INTO_COST

      case e: VarExpand if e.mode == ExpandInto => EXPAND_INTO_COST

      case _: Expand |
        _: VarExpand |
        _: OptionalExpand => EXPAND_ALL_COST

      case _: NodeUniqueIndexSeek |
        _: NodeIndexSeek |
        _: NodeIndexContainsScan |
        _: NodeIndexEndsWithScan => INDEX_SEEK_COST_PER_ROW

      case _: NodeByIdSeek |
        _: NodeByElementIdSeek |
        _: DirectedRelationshipByIdSeek |
        _: DirectedRelationshipByElementIdSeek => STORE_LOOKUP_COST_PER_ROW

      case _: UndirectedRelationshipByIdSeek | _: UndirectedRelationshipByElementIdSeek
        // Only every second row needs to access the store
        => STORE_LOOKUP_COST_PER_ROW / 2

      case _: DirectedAllRelationshipsScan => ALL_SCAN_COST_PER_ROW

      case _: UndirectedAllRelationshipsScan => ALL_SCAN_COST_PER_ROW / 2

      case plan: DirectedRelationshipTypeScan =>
        hackyRelTypeScanCost(propertyAccess, plan.idName, directed = true)

      case plan: UndirectedRelationshipTypeScan =>
        hackyRelTypeScanCost(propertyAccess, plan.idName, directed = false)

      case plan: DirectedUnionRelationshipTypesScan =>
        hackyRelTypeScanCost(propertyAccess, plan.idName, directed = true)

      case plan: UndirectedUnionRelationshipTypesScan =>
        hackyRelTypeScanCost(propertyAccess, plan.idName, directed = false)

      case _: DirectedRelationshipIndexScan => DIRECTED_RELATIONSHIP_INDEX_SCAN_COST_PER_ROW

      case _: UndirectedRelationshipIndexScan
        // Only every second row needs to access the index and the store
        => DIRECTED_RELATIONSHIP_INDEX_SCAN_COST_PER_ROW / 2

      case _: DirectedRelationshipIndexSeek |
        _: DirectedRelationshipIndexContainsScan |
        _: DirectedRelationshipIndexEndsWithScan => INDEX_SEEK_COST_PER_ROW + STORE_LOOKUP_COST_PER_ROW

      case _: UndirectedRelationshipIndexSeek |
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
        _: ProcedureCall => DEFAULT_COST_PER_ROW

      case _: Sort =>
        // Sorting is O(n * log(n)). Therefore the cost per row is O(log(n))
        // This means:
        // Sorting 1 row has cost 0.03 per row.
        // Sorting 9 rows has cost 0.1 per row (DEFAULT_COST_PER_ROW).
        // Sorting 99 rows has cost 0.2 per row.
        DEFAULT_COST_PER_ROW * Math.log(cardinality.amount + 1)

      case _: FindShortestPaths =>
        SHORTEST_INTO_COST

      case _: StatefulShortestPath =>
        SHORTEST_PRODUCT_GRAPH_COST

      case _: PartitionedScanPlan =>
        throw new IllegalStateException("partitioned scans should only be planned at physical planning")

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
    rhsReduction: WorkReduction
  )

  /**
   * The limit selectivity of a limiting plan.
   *
   * @param inputCardinality        the cardinality of the plan's parent
   * @param outputCardinality       the cardinality of plan
   * @param parentLimitSelectivity  the limit selectivity of the plan's parent
   */
  def limitingPlanSelectivity(
    inputCardinality: Cardinality,
    outputCardinality: Cardinality,
    parentLimitSelectivity: Selectivity
  ): Selectivity =
    limitingPlanWorkReduction(inputCardinality, outputCardinality, WorkReduction(parentLimitSelectivity)).fraction

  /**
   * The work reduction of a limiting plan.
   *
   * @param inputCardinality        the cardinality of the plan's parent
   * @param outputCardinality       the cardinality of plan
   * @param parentWorkReduction     the work reduction of the plan's parent
   */
  def limitingPlanWorkReduction(
    inputCardinality: Cardinality,
    outputCardinality: Cardinality,
    parentWorkReduction: WorkReduction
  ): WorkReduction = {
    val reducedOutput = parentWorkReduction.calculate(outputCardinality)
    val fraction = (reducedOutput / inputCardinality) getOrElse Selectivity.ONE
    parentWorkReduction.withFraction(fraction)
  }

  /**
   * Given an incoming WorkReduction, calculate how this reduction applies to the LHS and RHS of the plan.
   *
   */
  private[logical] def effectiveCardinalities(
    plan: LogicalPlan,
    workReduction: WorkReduction,
    batchSize: SelectedBatchSize,
    cardinalities: Cardinalities
  ): EffectiveCardinalities = {
    val (lhsWorkReduction, rhsWorkReduction) = childrenWorkReduction(plan, workReduction, batchSize, cardinalities)

    // Make sure argument leaf plans under semiApply etc. get bounded to the same cardinality as the lhs...
    val outputCardinalityUseMinimum = plan match {
      case _: Argument => true
      case _           => false
    }

    // ...and make sure that plans on top of argument lef plans under semiApply etc. get bounded to that same cardinality.
    val inputCardinalityUseMinimum = outputCardinalityUseMinimum || plan.lhs.exists {
      case _: Argument => true
      case _           => false
    }

    EffectiveCardinalities(
      outputCardinality = workReduction.calculate(outputCardinality(plan, cardinalities), outputCardinalityUseMinimum),
      inputCardinality = lhsWorkReduction.calculate(inputCardinality(plan, cardinalities), inputCardinalityUseMinimum),
      plan.lhs.map(p => lhsWorkReduction.calculate(cardinalities.get(p.id))) getOrElse Cardinality.EMPTY,
      plan.rhs.map(p => rhsWorkReduction.calculate(cardinalities.get(p.id))) getOrElse Cardinality.EMPTY,
      lhsWorkReduction,
      rhsWorkReduction
    )
  }

  /**
   * Given an parent WorkReduction, calculate how this reduction applies to the LHS and RHS of the plan.
   */
  private[logical] def childrenWorkReduction(
    plan: LogicalPlan,
    parentWorkReduction: WorkReduction,
    batchSize: SelectedBatchSize,
    cardinalities: Cardinalities
  ): (WorkReduction, WorkReduction) = plan match {

    // NOTE: we don't match on ExhaustiveLimit here since that doesn't affect the cardinality of earlier plans
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
      val rhsReduction =
        limitingPlanWorkReduction(rhsCardinality, Cardinality.SINGLE, WorkReduction.NoReduction)
          .copy(minimum = Some(Cardinality.SINGLE))
      (parentWorkReduction, rhsReduction)

    // if there is no parentWorkReduction, all cases below are unnecessary, so let's skip doing unnecessary work
    case _ if parentWorkReduction == WorkReduction.NoReduction =>
      (parentWorkReduction, parentWorkReduction)

    case _: ForeachApply =>
      // ForeachApply is an ApplyPlan, but only yields LHS rows, therefore we match this before matching ApplyPlan
      (parentWorkReduction, WorkReduction.NoReduction)

    case t: Trail =>
      // Trail is an ApplyPlan, but nestedLoopChildrenWorkReduction makes some assumptions that don't hold for Trail
      trailChildrenWorkReduction(t, parentWorkReduction, cardinalities)

    case a: ApplyPlan =>
      nestedLoopChildrenWorkReduction(a, parentWorkReduction, VolcanoBatchSize, cardinalities)

    case p: CartesianProduct =>
      nestedLoopChildrenWorkReduction(p, parentWorkReduction, batchSize, cardinalities)

    case _: AssertSameNode =>
      (parentWorkReduction, WorkReduction.NoReduction)

    case _: AssertSameRelationship =>
      (parentWorkReduction, WorkReduction.NoReduction)

    case u: Union =>
      // number of rows available from lhs/rhs plan
      val lhsCardinality: Cardinality = cardinalities.get(u.left.id)
      val rhsCardinality: Cardinality = cardinalities.get(u.right.id)

      val parentEffectiveCardinality = cardinalities.get(u.id) * parentWorkReduction.fraction

      val (lhsEffectiveCardinality, rhsEffectiveCardinality) =
        if (lhsCardinality > parentEffectiveCardinality) {
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

    case _: PartialSort =>
      // Let's assume the child has to do "a little more" work.
      // This happens because PartialSort has to process at least a whole bucket of identical values.
      val parentFraction = parentWorkReduction.fraction.factor
      val childFraction = Math.min(1.0, parentFraction * (1.0 + PARTIAL_SORT_WORK_INCREASE))
      (parentWorkReduction.withFraction(Selectivity(childFraction)), WorkReduction.NoReduction)

    case _: ExhaustiveLogicalPlan =>
      (WorkReduction.NoReduction, WorkReduction.NoReduction)

    case _: LogicalUnaryPlan =>
      (parentWorkReduction, WorkReduction.NoReduction)

    case _: LogicalLeafPlan =>
      (parentWorkReduction, WorkReduction.NoReduction)

    case lp: LogicalBinaryPlan =>
      // Forces us to hopefully think about this when adding new binary plans
      AssertMacros.checkOnlyWhenAssertionsAreEnabled(
        false,
        s"childrenWorkReduction: No case for ${lp.getClass.getSimpleName} added."
      )
      (parentWorkReduction, WorkReduction.NoReduction)

    case p: LogicalPlanExtension =>
      throw new IllegalArgumentException(s"Did not expect this plan here: $p")
  }

  /**
   * Given an parent WorkReduction, calculate how this reduction applies to the LHS and RHS of this plan that implements a nested loop.
   */
  private def nestedLoopChildrenWorkReduction(
    plan: LogicalBinaryPlan,
    parentWorkReduction: WorkReduction,
    batchSize: SelectedBatchSize,
    cardinalities: Cardinalities
  ): (WorkReduction, WorkReduction) = {
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
    val requiredOutput: Cardinality = outputChunk * Multiplier.ofDivision(
      parentWorkReduction.calculate(cardinalities.get(plan.id)),
      outputChunk
    ).getOrElse(Multiplier.ZERO).ceil

    // number of executions needed to produce the required output
    val rhsExecutions: Multiplier =
      Multiplier.ofDivision(requiredOutput, outputRowsPerExecution).getOrElse(Multiplier.ZERO)
    val lhsFullBatches: Cardinality = Cardinality(rhsExecutions.coefficient).ceil

    // total number of rows fetched from lhs/rhs
    val lhsProducedRows: Cardinality = Cardinality.min(lhsFullBatches * chunkSize, lhsCardinality)
    val rhsProducedRows: Cardinality = Cardinality.max(rhsExecutions * rhsCardinality, rhsRowsMinimum)

    val rhsProducedRowsPerExecution = Cardinality(Multiplier.ofDivision(
      rhsProducedRows * chunkSize,
      lhsProducedRows
    ).getOrElse(Multiplier.ZERO).coefficient)

    val lhsFraction = (lhsProducedRows / lhsCardinality).getOrElse(Selectivity.ONE)
    val rhsFraction = (rhsProducedRowsPerExecution / rhsCardinality).getOrElse(Selectivity.ONE)

    (parentWorkReduction.withFraction(lhsFraction), parentWorkReduction.withFraction(rhsFraction))
  }

  /**
   * Given an parent WorkReduction, calculate how this reduction applies to the LHS and RHS.
   * 
   * This is essentially a much simplified version of [[nestedLoopChildrenWorkReduction]] with [[VolcanoBatchSize]].
   * It differs from [[nestedLoopChildrenWorkReduction]] in the fact that here
   * `lhsReduction * rhsReduction = parentReduction` always holds,
   * which is not true in [[nestedLoopChildrenWorkReduction]] if the RHS cardinality is < 1.0.
   * 
   * [[nestedLoopChildrenWorkReduction]] also assumes that `lhsCardinality * rhsCardinality = parentCardinality`, 
   * which is not true for Trail.
   */
  private def trailChildrenWorkReduction(
    plan: Trail,
    parentWorkReduction: WorkReduction,
    cardinalities: Cardinalities
  ): (WorkReduction, WorkReduction) = {
    // number of rows available from lhs plan
    val lhsCardinality: Cardinality = cardinalities.get(plan.left.id)

    // apply reduction to LHS
    val rhsNeededExecutions: Cardinality = parentWorkReduction.calculate(lhsCardinality)

    // round up
    val lhsProducedRows: Cardinality = rhsNeededExecutions.ceil

    // Calculate LHS reduction with rounding taken into account
    val lhsFraction = (lhsProducedRows / lhsCardinality).getOrElse(Selectivity.ONE)

    // Apply remaining reduction to RHS
    val rhsFraction =
      Selectivity.of(parentWorkReduction.fraction.factor / lhsFraction.factor).getOrElse(Selectivity.ONE)

    (parentWorkReduction.withFraction(lhsFraction), parentWorkReduction.withFraction(rhsFraction))
  }

  private[logical] def getEffectiveBatchSize(
    batchSize: SelectedBatchSize,
    plan: LogicalPlan,
    providedOrders: ProvidedOrders
  ): SelectedBatchSize = {
    plan match {
      // Ideally we would want to check leveragedOrders here. But that attribute will not be set properly at this point of planning,
      // since that only happens when the leveraging plan is planned.
      case cp: CartesianProduct if !providedOrders(cp.id).isEmpty =>
        ExecutionModel.VolcanoBatchSize // A CartesianProduct that provides order is rewritten to execute in a row-by-row fashion
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
