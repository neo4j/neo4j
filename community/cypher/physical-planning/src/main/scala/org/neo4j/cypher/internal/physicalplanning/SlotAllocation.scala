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

import org.neo4j.cypher.internal
import org.neo4j.cypher.internal.ast.ProcedureResultItem
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorFail
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.CachedHasProperty
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.Literal
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Null
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.frontend.phases.ResolvedCall
import org.neo4j.cypher.internal.ir.CreatePattern
import org.neo4j.cypher.internal.ir.HasHeaders
import org.neo4j.cypher.internal.ir.NoHeaders
import org.neo4j.cypher.internal.logical.plans.AbstractSelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.AbstractSemiApply
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.Anti
import org.neo4j.cypher.internal.logical.plans.AntiConditionalApply
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.ApplyPlan
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.ArgumentTracker
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.AssertSameRelationship
import org.neo4j.cypher.internal.logical.plans.AssertingMultiNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.AssertingMultiRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.BFSPruningVarExpand
import org.neo4j.cypher.internal.logical.plans.CacheProperties
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.CommandLogicalPlan
import org.neo4j.cypher.internal.logical.plans.ConditionalApply
import org.neo4j.cypher.internal.logical.plans.Create
import org.neo4j.cypher.internal.logical.plans.DeleteExpression
import org.neo4j.cypher.internal.logical.plans.DeleteNode
import org.neo4j.cypher.internal.logical.plans.DeletePath
import org.neo4j.cypher.internal.logical.plans.DeleteRelationship
import org.neo4j.cypher.internal.logical.plans.DetachDeleteExpression
import org.neo4j.cypher.internal.logical.plans.DetachDeleteNode
import org.neo4j.cypher.internal.logical.plans.DetachDeletePath
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.EmptyResult
import org.neo4j.cypher.internal.logical.plans.ErrorPlan
import org.neo4j.cypher.internal.logical.plans.ExhaustiveLimit
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths
import org.neo4j.cypher.internal.logical.plans.Foreach
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.InjectCompilationError
import org.neo4j.cypher.internal.logical.plans.Input
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.LetAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSelectOrAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSemiApply
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LoadCSV
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Merge
import org.neo4j.cypher.internal.logical.plans.MultiNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NestedPlanCollectExpression
import org.neo4j.cypher.internal.logical.plans.NestedPlanExpression
import org.neo4j.cypher.internal.logical.plans.NodeCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.NodeIndexLeafPlan
import org.neo4j.cypher.internal.logical.plans.NodeLogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.NonFuseable
import org.neo4j.cypher.internal.logical.plans.NonPipelined
import org.neo4j.cypher.internal.logical.plans.NonPipelinedStreaming
import org.neo4j.cypher.internal.logical.plans.NullifyMetadata
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.logical.plans.OrderedAggregation
import org.neo4j.cypher.internal.logical.plans.OrderedUnion
import org.neo4j.cypher.internal.logical.plans.PartialSort
import org.neo4j.cypher.internal.logical.plans.PartialTop
import org.neo4j.cypher.internal.logical.plans.PartitionedUnwindCollection
import org.neo4j.cypher.internal.logical.plans.PreserveOrder
import org.neo4j.cypher.internal.logical.plans.Prober
import org.neo4j.cypher.internal.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.ProjectEndpoints
import org.neo4j.cypher.internal.logical.plans.ProjectingPlan
import org.neo4j.cypher.internal.logical.plans.PruningVarExpand
import org.neo4j.cypher.internal.logical.plans.RelationshipCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.RelationshipIndexLeafPlan
import org.neo4j.cypher.internal.logical.plans.RelationshipLogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.RemoveLabels
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.RollUpApply
import org.neo4j.cypher.internal.logical.plans.RunQueryAt
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.SetLabels
import org.neo4j.cypher.internal.logical.plans.SetNodeProperties
import org.neo4j.cypher.internal.logical.plans.SetNodePropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetNodeProperty
import org.neo4j.cypher.internal.logical.plans.SetProperties
import org.neo4j.cypher.internal.logical.plans.SetPropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetProperty
import org.neo4j.cypher.internal.logical.plans.SetRelationshipProperties
import org.neo4j.cypher.internal.logical.plans.SetRelationshipPropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetRelationshipProperty
import org.neo4j.cypher.internal.logical.plans.SimulatedExpand
import org.neo4j.cypher.internal.logical.plans.SimulatedSelection
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.logical.plans.SubqueryForeach
import org.neo4j.cypher.internal.logical.plans.Top
import org.neo4j.cypher.internal.logical.plans.Top1WithTies
import org.neo4j.cypher.internal.logical.plans.Trail
import org.neo4j.cypher.internal.logical.plans.TransactionApply
import org.neo4j.cypher.internal.logical.plans.TransactionForeach
import org.neo4j.cypher.internal.logical.plans.TriadicBuild
import org.neo4j.cypher.internal.logical.plans.TriadicFilter
import org.neo4j.cypher.internal.logical.plans.TriadicSelection
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.ApplyPlans
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.ArgumentSizes
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.LiveVariables
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.NestedPlanArgumentConfigurations
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.SlotConfigurations
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.TrailPlans
import org.neo4j.cypher.internal.physicalplanning.PipelineBreakingPolicy.DiscardFromLhs
import org.neo4j.cypher.internal.physicalplanning.PipelineBreakingPolicy.DiscardFromRhs
import org.neo4j.cypher.internal.physicalplanning.PipelineBreakingPolicy.DoNotDiscard
import org.neo4j.cypher.internal.physicalplanning.SlotAllocation.LOAD_CSV_METADATA_KEY
import org.neo4j.cypher.internal.physicalplanning.SlotAllocation.NO_ARGUMENT
import org.neo4j.cypher.internal.physicalplanning.SlotAllocation.SlotMetaData
import org.neo4j.cypher.internal.physicalplanning.SlotAllocation.SlotsAndArgument
import org.neo4j.cypher.internal.physicalplanning.SlotAllocation.TRAIL_STATE_METADATA_KEY
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.ApplyPlanSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.CachedPropertySlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.DuplicatedSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.MetaDataSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.OuterNestedApplyPlanSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.Size
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.SlotWithKeyAndAliases
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.VariableSlotKey
import org.neo4j.cypher.internal.runtime.CypherRuntimeConfiguration
import org.neo4j.cypher.internal.runtime.expressionVariableAllocation.AvailableExpressionVariables
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Foldable
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTPath
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.exceptions.InternalException

import java.util

/**
 * This object knows how to configure slots for a logical plan tree.
 *
 * The structure of the code is built this maybe weird way instead of being recursive to avoid the JVM execution stack
 * and instead handle the stacks manually here. Some queries we have seen are deep enough to crash the VM if not
 * configured carefully.
 *
 * The knowledge about how to actually allocate slots for each logical plan lives in the three `allocate` methods,
 * whereas the knowledge of how to traverse the plan tree is store in the while loops and stacks in the `populate`
 * method.
 **/
object SlotAllocation {

  /**
   * Case class containing information about the argument at a particular point during slot allocation.
   *
   * @param slotConfiguration the slot configuration of the argument. Might contain more slot than the argument.
   * @param argumentSize the prefix size of `slotConfiguration` that holds the argument.
   * @param argumentPlan the plan which introduced this argument
   * @param conditionalApplyPlan the nearest outer [Anti]ConditionalApply plan
   * @param trailPlan the nearest outer Trail plan
   */
  case class SlotsAndArgument(
    slotConfiguration: SlotConfiguration,
    argumentSize: Size,
    argumentPlan: Id,
    conditionalApplyPlan: Id,
    trailPlan: Id
  ) {

    def isArgument(field: String): Boolean =
      slotConfiguration.get(field).fold(false)(isArgument)

    def isArgument(slot: Slot): Boolean =
      argumentSize.contains(slot)
  }

  case class SlotMetaData(
    slotConfigurations: SlotConfigurations,
    argumentSizes: ArgumentSizes,
    applyPlans: ApplyPlans,
    trailPlans: TrailPlans,
    nestedPlanArgumentConfigurations: NestedPlanArgumentConfigurations
  )

  private[physicalplanning] def NO_ARGUMENT(allocateArgumentSlots: Boolean): SlotsAndArgument = {
    val slots = SlotConfiguration.empty
    if (allocateArgumentSlots) {
      slots.newArgument(Id.INVALID_ID)
    }
    SlotsAndArgument(slots, Size.zero, Id.INVALID_ID, Id.INVALID_ID, Id.INVALID_ID)
  }

  final val INITIAL_SLOT_CONFIGURATION: SlotConfiguration = NO_ARGUMENT(true).slotConfiguration

  def allocateSlots(
    lp: LogicalPlan,
    semanticTable: SemanticTable,
    breakingPolicy: PipelineBreakingPolicy,
    availableExpressionVariables: AvailableExpressionVariables,
    config: CypherRuntimeConfiguration,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    liveVariables: LiveVariables,
    allocatePipelinedSlots: Boolean = false
  ): SlotMetaData =
    new SingleQuerySlotAllocator(
      allocatePipelinedSlots,
      breakingPolicy,
      availableExpressionVariables,
      config,
      anonymousVariableNameGenerator,
      liveVariables = liveVariables
    ).allocateSlots(lp, semanticTable, None)

  final val LOAD_CSV_METADATA_KEY: String = "csv"

  final val TRAIL_STATE_METADATA_KEY: String = "trailState"
}

/**
 * Single shot slot allocator. Will break if used on two logical plans.
 */
//noinspection NameBooleanParameters,RedundantDefaultArgument
class SingleQuerySlotAllocator private[physicalplanning] (
  allocateArgumentSlots: Boolean,
  breakingPolicy: PipelineBreakingPolicy,
  availableExpressionVariables: AvailableExpressionVariables,
  config: CypherRuntimeConfiguration,
  anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
  private val allocations: SlotConfigurations = new SlotConfigurations,
  private val argumentSizes: ArgumentSizes = new ArgumentSizes,
  private val applyPlans: ApplyPlans = new ApplyPlans,
  private val trailPlans: TrailPlans = new TrailPlans,
  private val liveVariables: LiveVariables = new LiveVariables,
  private val nestedPlanArgumentConfigurations: NestedPlanArgumentConfigurations = new NestedPlanArgumentConfigurations
) {

  /**
   * We need argument row id slots for cartesian product in the pipelines runtime
   */
  private def argumentRowIdSlotForCartesianProductNeeded(plan: LogicalPlan): Boolean =
    allocateArgumentSlots && plan.isInstanceOf[CartesianProduct]

  /**
   * Allocate slot for every operator in the logical plan tree `lp`.
   *
   * @param lp the logical plan to process.
   * @return the slot configurations of every operator.
   */
  def allocateSlots(
    lp: LogicalPlan,
    semanticTable: SemanticTable,
    initialSlotsAndArgument: Option[SlotsAndArgument]
  ): SlotMetaData = {

    val planStack = new util.ArrayDeque[(Boolean, LogicalPlan)]()
    val resultStack = new util.ArrayDeque[SlotConfiguration]()
    val argumentStack = new util.ArrayDeque[SlotsAndArgument]()
    initialSlotsAndArgument.foreach(argumentStack.push)
    var comingFrom = lp

    def recordArgument(plan: LogicalPlan, argument: SlotsAndArgument): Unit = {
      argumentSizes.set(plan.id, argument.argumentSize)
    }

    def getArgument(): SlotsAndArgument = {
      if (argumentStack.isEmpty) {
        NO_ARGUMENT(allocateArgumentSlots)
      } else {
        argumentStack.getFirst
      }
    }

    /**
     * Eagerly populate the stack using all the lhs children.
     */
    def populate(plan: LogicalPlan, nullIn: Boolean): Unit = {
      var nullable = nullIn
      var current = plan
      while (!current.isLeaf) {
        if (current.isInstanceOf[Optional]) {
          nullable = true
        }
        planStack.push((nullable, current))

        current = current.lhs.get // this should not fail unless we are on a leaf
      }
      comingFrom = current
      planStack.push((nullable, current))
    }

    populate(lp, nullIn = false)

    while (!planStack.isEmpty) {
      val (nullable, current) = planStack.pop()

      val (outerApplyPlan, outerTrailPlan) = if (argumentStack.isEmpty) (Id.INVALID_ID, Id.INVALID_ID)
      else (argumentStack.getFirst.argumentPlan, argumentStack.getFirst.trailPlan)

      applyPlans.set(current.id, outerApplyPlan)
      trailPlans.set(current.id, outerTrailPlan)

      (current.lhs, current.rhs) match {
        case (None, None) =>
          val argument = getArgument()
          recordArgument(current, argument)

          val slots = breakingPolicy.invoke(current, argument.slotConfiguration, argument.slotConfiguration, applyPlans)

          allocateExpressionsOneChild(current, nullable, slots, semanticTable)
          allocateLeaf(current, nullable, slots)
          allocations.set(current.id, slots)
          resultStack.push(slots)

        case (Some(_), None) =>
          val sourceSlots = resultStack.pop()
          val argument = getArgument()
          allocateExpressionsOneChildOnInput(current, nullable, sourceSlots, semanticTable)

          val slots = breakingPolicy.invoke(current, sourceSlots, argument.slotConfiguration, applyPlans)
          allocateOneChild(
            argument,
            current,
            nullable,
            sourceSlots,
            slots,
            recordArgument(_, argument),
            semanticTable
          )
          allocateExpressionsOneChildOnOutput(current, nullable, slots, semanticTable)
          allocations.set(current.id, slots)
          resultStack.push(slots)

        case (Some(left), Some(right)) if (comingFrom eq left) && current.isInstanceOf[ApplyPlan] =>
          planStack.push((nullable, current))
          val argumentSlots = resultStack.getFirst
          val argument = getArgument()

          def isConditionalApplyPlan: Boolean = current match {
            case _: ConditionalApply | _: AntiConditionalApply => true
            case _                                             => false
          }
          val conditionalApplyPlan = if (isConditionalApplyPlan) current.id else argument.conditionalApplyPlan
          val trailPlan = if (current.isInstanceOf[Trail]) current.id else argument.trailPlan
          if (allocateArgumentSlots) {
            current match {
              case _: Trail =>
                // Trail requires 2 arguments: one per incoming LHS row (regular ApplyPlan case), and one per RHS invocation (QPP repetition)
                argumentSlots.newNestedArgument(current.id)
                argumentSlots.newArgument(current.id)
              case _ =>
                argumentSlots.newArgument(current.id)
            }
          }
          allocateLhsOfApply(current, nullable, argumentSlots, semanticTable)
          val lhsSlots = allocations.get(left.id)
          allocateExpressionsTwoChild(current, lhsSlots, semanticTable, comingFromLeft = true)
          argumentStack.push(SlotsAndArgument(
            argumentSlots,
            argumentSlots.size(),
            current.id,
            conditionalApplyPlan,
            trailPlan
          ))
          populate(right, nullable)

        case (Some(left), Some(right)) if comingFrom eq left =>
          planStack.push((nullable, current))
          val lhsSlots = allocations.get(left.id)
          if (argumentRowIdSlotForCartesianProductNeeded(current)) {
            val previousArgument = getArgument()
            // We put a new argument on the argument stack, by creating a copy of the incoming LHS slot configuration,
            // because although the RHS does not need any slots from the LHS, an argument slot needs to be present.
            val newArgument = lhsSlots.newArgument(current.id)
            argumentStack.push(SlotsAndArgument(
              newArgument,
              newArgument.size(),
              current.id,
              previousArgument.conditionalApplyPlan,
              previousArgument.trailPlan
            ))
          }
          allocateExpressionsTwoChild(current, lhsSlots, semanticTable, comingFromLeft = true)

          populate(right, nullable)

        case (Some(_), Some(right)) if comingFrom eq right =>
          val rhsSlots = resultStack.pop()
          val lhsSlots = resultStack.pop()
          val argument = getArgument()
          // NOTE: If we introduce a two sourced logical plan with an expression that needs to be evaluated in a
          //       particular scope (lhs or rhs) we need to add handling of it to allocateExpressionsTwoChild.
          allocateExpressionsTwoChild(current, rhsSlots, semanticTable, comingFromLeft = false)

          val result = allocateTwoChild(current, nullable, lhsSlots, rhsSlots, recordArgument(_, argument), argument)
          allocations.set(current.id, result)
          if (current.isInstanceOf[ApplyPlan] || argumentRowIdSlotForCartesianProductNeeded(current)) {
            argumentStack.pop()
          }
          resultStack.push(result)
      }

      comingFrom = current
    }

    SlotMetaData(allocations, argumentSizes, applyPlans, trailPlans, nestedPlanArgumentConfigurations)
  }

  case class Accumulator(doNotTraverseExpression: Option[Expression])

  private def allocateExpressionsOneChildOnInput(
    plan: LogicalPlan,
    nullable: Boolean,
    slots: SlotConfiguration,
    semanticTable: SemanticTable
  ): Unit = plan match {
    case ssp: StatefulShortestPath =>
      allocateExpressionsInternal(ssp.nfa, slots, semanticTable, plan.id)
    case _: OptionalExpand                                               =>
    case FindShortestPaths(_, _, nodePredicates, relPredicates, _, _, _) =>
      // Node & Relationship predicates may contain NestPlanExpressions.
      // In those cases the nested plan must have the same slot configuration as input rows,
      // otherwise argument copying breaks with index out of bounds.
      nodePredicates.foreach(allocateExpressionsInternal(_, slots, semanticTable, plan.id))
      relPredicates.foreach(allocateExpressionsInternal(_, slots, semanticTable, plan.id))
    case _ => allocateExpressionsOneChild(plan, nullable, slots, semanticTable)
  }

  private def allocateExpressionsOneChildOnOutput(
    plan: LogicalPlan,
    nullable: Boolean,
    slots: SlotConfiguration,
    semanticTable: SemanticTable
  ): Unit = plan match {
    case ssp: StatefulShortestPath =>
      allocateExpressionsInternal(ssp.nonInlinedPreFilters, slots, semanticTable, plan.id)
    case _: OptionalExpand =>
      allocateExpressionsOneChild(plan, nullable, slots, semanticTable)
    case FindShortestPaths(_, pattern, _, _, pathPredicates, _, _) =>
      // Path predicates must be allocated after 'rels' and 'path' slots have been allocated.
      allocateExpressionsInternal(pattern, slots, semanticTable, plan.id)
      allocateExpressionsInternal(pathPredicates, slots, semanticTable, plan.id)
    case _ =>
  }

  private def allocateExpressionsOneChild(
    plan: LogicalPlan,
    nullable: Boolean,
    slots: SlotConfiguration,
    semanticTable: SemanticTable
  ): Unit = {

    plan.folder.treeFold[Accumulator](Accumulator(doNotTraverseExpression = None)) {
      case otherPlan: LogicalPlan if otherPlan.id != plan.id =>
        (acc: Accumulator) =>
          SkipChildren(acc) // Do not traverse the logical plan tree! We are only looking at the given lp

      case ProjectEndpoints(_, _, start, startInScope, end, endInScope, _, _, _) =>
        (acc: Accumulator) => {
          if (!startInScope) {
            slots.newLong(start, nullable, CTNode)
          }
          if (!endInScope) {
            slots.newLong(end, nullable, CTNode)
          }
          TraverseChildren(acc)
        }

      case e: Expression =>
        allocateExpressionsInternal(e, slots, semanticTable, plan.id)
        (acc: Accumulator) =>
          SkipChildren(acc)
    }
  }

  private def allocateExpressionsTwoChild(
    plan: LogicalPlan,
    slots: SlotConfiguration,
    semanticTable: SemanticTable,
    comingFromLeft: Boolean
  ): Unit = {
    plan.folder.treeFold[Accumulator](Accumulator(doNotTraverseExpression = None)) {
      case otherPlan: LogicalPlan if otherPlan.id != plan.id =>
        (acc: Accumulator) =>
          SkipChildren(acc) // Do not traverse the logical plan tree! We are only looking at the given lp

      case ValueHashJoin(_, _, Equals(_, rhsExpression)) if comingFromLeft =>
        (_: Accumulator) =>
          TraverseChildren(Accumulator(doNotTraverseExpression = Some(rhsExpression))) // Only look at lhsExpression

      case ValueHashJoin(_, _, Equals(lhsExpression, _)) if !comingFromLeft =>
        (_: Accumulator) =>
          TraverseChildren(Accumulator(doNotTraverseExpression = Some(lhsExpression))) // Only look at rhsExpression

      // Only allocate expression on the LHS for these other two-child plans (which have expressions)
      case _: ApplyPlan if !comingFromLeft =>
        (acc: Accumulator) => SkipChildren(acc)

      case e: Expression =>
        (acc: Accumulator) =>
          allocateExpressionsInternal(e, slots, semanticTable, plan.id, acc)
          SkipChildren(acc)
    }
  }

  private def allocateExpressionsInternal(
    expression: Foldable,
    slots: SlotConfiguration,
    semanticTable: SemanticTable,
    planId: Id,
    acc: Accumulator = Accumulator(doNotTraverseExpression = None)
  ): Unit = {
    expression.folder.treeFold[Accumulator](acc) {
      case otherPlan: LogicalPlan if otherPlan.id != planId =>
        (acc: Accumulator) =>
          SkipChildren(acc) // Do not traverse the logical plan tree! We are only looking at the given lp

      case e: NestedPlanExpression =>
        (acc: Accumulator) => {
          if (acc.doNotTraverseExpression.contains(e)) {
            SkipChildren(acc)
          } else {
            val argumentSlotConfiguration = slots.copy()
            availableExpressionVariables(e.plan.id).foreach { expVar =>
              argumentSlotConfiguration.newReference(expVar.name, nullable = true, CTAny)
            }
            nestedPlanArgumentConfigurations.set(e.plan.id, argumentSlotConfiguration)

            /*
             * TODO:
             *
             * The correct value of `argumentPlanId` is `applyPlans(planId)`
             * but in the current implementation that causes a failure in
             * Unchangable because we're not done writing that value if we're
             * evaluating the expression on an ApplyPlan, e.g. SelectOrSemiApply.
             *
             * Although not strictly correct, this workaround is currently
             * harmless. We always plan nested plan expressions with slotted
             * breaking policy and the apply plan id is currently only used
             * during slot allocation within PipelinedPipelineBreakingPolicy.
             *
             */
            val conditionalApplyPlan = Id.INVALID_ID
            val argumentPlan = Id.INVALID_ID
            /*
             * We don't think we need to propagate the Trail plan id since
             * nested plans are only ever run with slotted pipes.
             */
            val trailPlanId = Id.INVALID_ID
            val slotsAndArgument =
              SlotsAndArgument(
                argumentSlotConfiguration.copy(),
                argumentSlotConfiguration.size(),
                argumentPlan,
                conditionalApplyPlan,
                trailPlanId
              )

            // Allocate slots for nested plan
            // Pass in mutable attributes to be modified by recursive call
            // disable argument allocation since this will always be solved by slotted
            val nestedPhysicalPlan =
              withoutArgumentAllocationAndWithBreakingPolicy(breakingPolicy.nestedPlanBreakingPolicy).allocateSlots(
                e.plan,
                semanticTable,
                Some(slotsAndArgument)
              )

            // Allocate slots for the projection expression, based on the resulting slot configuration
            // from the inner plan
            val nestedSlots = nestedPhysicalPlan.slotConfigurations(e.plan.id)
            e match {
              case NestedPlanCollectExpression(_, projection, _) =>
                allocateExpressionsInternal(projection, nestedSlots, semanticTable, planId)
              case _ => // do nothing
            }

            // Since we did allocation for nested plan and projection explicitly we do not need to traverse into children
            // The inner slot configuration does not need to affect the accumulated result of the outer plan
            SkipChildren(acc)
          }
        }

      case e: Expression =>
        (acc: Accumulator) => {
          if (acc.doNotTraverseExpression.contains(e)) {
            SkipChildren(acc)
          } else {
            e match {
              case c: CachedProperty =>
                slots.newCachedProperty(c.runtimeKey)
              case c: CachedHasProperty =>
                slots.newCachedProperty(c.runtimeKey)
              case _ => // Do nothing
            }
            TraverseChildren(acc)
          }
        }
    }
  }

  private def withoutArgumentAllocationAndWithBreakingPolicy(newBreakingPolicy: PipelineBreakingPolicy) =
    new SingleQuerySlotAllocator(
      allocateArgumentSlots = false,
      newBreakingPolicy,
      availableExpressionVariables,
      config,
      anonymousVariableNameGenerator,
      allocations,
      argumentSizes,
      applyPlans,
      trailPlans,
      nestedPlanArgumentConfigurations = nestedPlanArgumentConfigurations
    )

  /**
   * Compute the slot configuration of a leaf logical plan operator `lp`.
   *
   * @param lp the operator to compute slots for.
   * @param nullable true if new slots are nullable
   * @param slots the slot configuration of lp
   */
  private def allocateLeaf(lp: LogicalPlan, nullable: Boolean, slots: SlotConfiguration): Unit =
    lp match {
      case MultiNodeIndexSeek(leafPlans) =>
        leafPlans.foreach { p =>
          allocateLeaf(p, nullable, slots)
          allocations.set(p.id, slots)
        }

      case AssertingMultiNodeIndexSeek(_, leafPlans) =>
        leafPlans.foreach { p =>
          allocateLeaf(p, nullable, slots)
          allocations.set(p.id, slots)
        }

      case AssertingMultiRelationshipIndexSeek(_, _, _, _, leafPlans) =>
        leafPlans.foreach { p =>
          allocateLeaf(p, nullable, slots)
          allocations.set(p.id, slots)
        }

      case leaf: NodeIndexLeafPlan =>
        slots.newLong(leaf.idName, nullable, CTNode)
        leaf.cachedProperties.foreach(cp => slots.newCachedProperty(cp.runtimeKey))

      case leaf: RelationshipIndexLeafPlan =>
        slots.newLong(leaf.idName, nullable, CTRelationship)
        slots.newLong(leaf.leftNode, nullable, CTNode)
        slots.newLong(leaf.rightNode, nullable, CTNode)
        leaf.cachedProperties.foreach(cp => slots.newCachedProperty(cp.runtimeKey))

      case leaf: NodeLogicalLeafPlan =>
        slots.newLong(leaf.idName, nullable, CTNode)

      case leaf: RelationshipLogicalLeafPlan =>
        slots.newLong(leaf.idName, nullable, CTRelationship)
        slots.newLong(leaf.leftNode, nullable, CTNode)
        slots.newLong(leaf.rightNode, nullable, CTNode)

      case _: Argument =>

      case leaf: NodeCountFromCountStore =>
        slots.newReference(leaf.idName, nullable, CTInteger)

      case leaf: RelationshipCountFromCountStore =>
        slots.newReference(leaf.idName, nullable, CTInteger)

      case leaf: CommandLogicalPlan =>
        for (v <- leaf.availableSymbols.map(_.name) ++ leaf.defaultColumns.map(_.name))
          slots.newReference(v, nullable, CTAny)

      case Input(nodes, relationships, variables, nullableInput) =>
        for (v <- nodes)
          slots.newLong(v, nullableInput || nullable, CTNode)
        for (v <- relationships)
          slots.newLong(v, nullableInput || nullable, CTRelationship)
        for (v <- variables)
          slots.newReference(v, nullableInput || nullable, CTAny)

      case p => throw new SlotAllocationFailed(s"Don't know how to handle $p")
    }

  /**
   * Compute the slot configuration of a single source logical plan operator `lp`.
   *
   * @param lp the operator to compute slots for.
   * @param nullable true if new slots are nullable
   * @param source the slot configuration of the source operator.
   * @param slots the slot configuration of lp.
   * @param recordArgument function which records the argument size for the given operator
   */
  private def allocateOneChild(
    slotsAndArgument: SlotsAndArgument,
    lp: LogicalPlan,
    nullable: Boolean,
    source: SlotConfiguration,
    slots: SlotConfiguration,
    recordArgument: LogicalPlan => Unit,
    semanticTable: SemanticTable
  ): Unit = {
    lp match {

      case Aggregation(_, groupingExpressions, aggregationExpressions) =>
        recordArgument(lp)
        addGroupingSlots(groupingExpressions, source, slots)
        aggregationExpressions foreach {
          case (key, _) =>
            slots.newReference(key, nullable = true, CTAny)
        }

      case OrderedAggregation(_, groupingExpressions, aggregationExpressions, _) =>
        addGroupingSlots(groupingExpressions, source, slots)
        aggregationExpressions foreach {
          case (key, _) =>
            slots.newReference(key, nullable = true, CTAny)
        }
        recordArgument(lp)

      case Expand(_, _, _, _, to, relName, ExpandAll) =>
        slots.newLong(relName, nullable, CTRelationship)
        slots.newLong(to, nullable, CTNode)

      case Expand(_, _, _, _, _, relName, ExpandInto) =>
        slots.newLong(relName, nullable, CTRelationship)

      case SimulatedExpand(_, _, rel, to, _) =>
        slots.newLong(rel, nullable, CTRelationship)
        slots.newLong(to, nullable, CTNode)

      case Optional(_, _) =>
        recordArgument(lp)

      case Anti(_) =>
        recordArgument(lp)

      case _: ProduceResult |
        _: Selection |
        _: SimulatedSelection |
        _: Limit |
        _: ExhaustiveLimit |
        _: Skip |
        _: Sort |
        _: PartialSort |
        _: Top |
        _: Top1WithTies |
        _: PartialTop |
        _: CacheProperties |
        _: NonFuseable |
        _: InjectCompilationError |
        _: NonPipelined |
        _: NonPipelinedStreaming |
        _: Prober |
        _: TriadicBuild |
        _: TriadicFilter |
        _: PreserveOrder |
        _: EmptyResult |
        _: ArgumentTracker =>

      case p: ProjectingPlan =>
        /**
         * This check is necessary to avoid variables on RHS of ConditionalApply from aliasing variables that were
         * introduced on the LHS. Doing so is incorrect because the RHS may never be executed, in which case all RHS
         * variables should be set to null.
         *
         * Note that this is conservative, it means that variables on RHS of ConditionalApply will _always_ introduce
         * a new slot. In theory we could create an alias if all aliased variables were introduced in the same scope,
         * but scopes where variables are introduced are not tracked at this time.
         *
         * It is also necessary to check if the alias would reference an argument under Optional ([[nullable]]),
         * in which case it must be disallowed so that the empty-input case can correctly return different values.
         */
        def mayAlias(ident: String): Boolean =
          (!nullable || !slotsAndArgument.isArgument(ident)) &&
            slotsAndArgument.conditionalApplyPlan == Id.INVALID_ID

        p.projectExpressions foreach {
          case (key, internal.expressions.Variable(ident)) if key.name == ident =>
          // it's already there. no need to add a new slot for it

          case (newKey, internal.expressions.Variable(ident)) if newKey.name != ident && mayAlias(ident) =>
            slots.addAlias(newKey, ident)

          case (key, _: PathExpression) =>
            slots.newReference(key, nullable = true, CTPath)

          case (key, _) =>
            slots.newReference(key, nullable = true, CTAny)
        }

      case OptionalExpand(_, _, _, _, to, rel, ExpandAll, _) =>
        // Note that OptionalExpand only is optional on the expand and not on incoming rows, so
        // we do not need to record the argument here.
        slots.newLong(rel, nullable = true, CTRelationship)
        slots.newLong(to, nullable = true, CTNode)

      case OptionalExpand(_, _, _, _, _, rel, ExpandInto, _) =>
        // Note that OptionalExpand only is optional on the expand and not on incoming rows, so
        // we do not need to record the argument here.
        slots.newLong(rel, nullable = true, CTRelationship)

      case VarExpand(_, _, _, _, _, to, relationship, _, expansionMode, _, _) =>
        if (expansionMode == ExpandAll) {
          slots.newLong(to, nullable, CTNode)
        }
        slots.newReference(relationship, nullable, CTList(CTRelationship))

      case PruningVarExpand(_, _, _, _, to, _, _, _, _) =>
        slots.newLong(to, nullable, CTNode)

      case expand: BFSPruningVarExpand =>
        slots.newLong(expand.to, nullable, CTNode)
        expand.depthName.foreach(name => slots.newReference(name, nullable, CTInteger))

      case c: Create =>
        c.nodes.foreach(n => slots.newLong(n.variable, nullable = false, CTNode))
        c.relationships.foreach(r =>
          slots.newLong(r.variable, nullable = config.lenientCreateRelationship, CTRelationship)
        )

      case _: EmptyResult |
        _: ErrorPlan |
        _: Eager =>

      case UnwindCollection(_, variable, expression) => allocateUnwind(variable, nullable, expression, slots)

      case PartitionedUnwindCollection(_, variable, expression) => allocateUnwind(variable, nullable, expression, slots)

      case _: DeleteNode |
        _: DeleteRelationship |
        _: DeletePath |
        _: DeleteExpression |
        _: DetachDeleteNode |
        _: DetachDeletePath |
        _: DetachDeleteExpression =>

      case _: SetLabels |
        _: SetNodeProperty |
        _: SetNodeProperties |
        _: SetNodePropertiesFromMap |
        _: SetRelationshipProperty |
        _: SetRelationshipProperties |
        _: SetRelationshipPropertiesFromMap |
        _: SetProperties |
        _: SetProperty |
        _: SetPropertiesFromMap |
        _: RemoveLabels =>

      case ProjectEndpoints(_, _, _, _, _, _, _, _, _) =>
      // Because of the way the interpreted pipe works, we already have to do the necessary allocations in allocateExpressions(), before the pipeline breaking.
      // Legacy interpreted pipes write directly to the incoming context, so to support pipeline breaking, the slots have to be allocated
      // on the source slot configuration.

      case LoadCSV(_, _, variableName, NoHeaders, _, _, _) =>
        slots.newReference(variableName, nullable, CTList(CTAny))
        slots.newMetaData(LOAD_CSV_METADATA_KEY)

      case LoadCSV(_, _, variableName, HasHeaders, _, _, _) =>
        slots.newReference(variableName, nullable, CTMap)
        slots.newMetaData(LOAD_CSV_METADATA_KEY)

      case ProcedureCall(_, ResolvedCall(_, _, callResults, _, _, _)) =>
        callResults.foreach {
          case ProcedureResultItem(_, variable) =>
            slots.newReference(variable.name, true, CTAny)
        }

      case _: Merge =>
        recordArgument(lp)

      case sp: FindShortestPaths =>
        val rel = sp.pattern.expr.element match {
          case internal.expressions.RelationshipChain(_, relationshipPattern, _) =>
            relationshipPattern
          case _ =>
            throw new IllegalStateException("This should be caught during semantic checking")
        }

        val pathName = sp.pattern.maybePathVar.get // Should always be given anonymous name
        val relsName = rel.variable.get.name // Should always be given anonymous name

        slots.newReference(pathName, nullable, CTPath)
        slots.newReference(relsName, nullable, CTList(CTRelationship))

      case p: StatefulShortestPath =>
        val nodeStateVars = p.singletonNodeVariables.map(_.rowVar.name)
        val relStateVars = p.singletonRelationshipVariables.map(_.rowVar.name)

        p.nodeVariableGroupings.foreach { n =>
          slots.newReference(n.group.name, nullable, CTList(CTNode))
        }
        p.relationshipVariableGroupings.foreach { n =>
          slots.newReference(n.group.name, nullable, CTList(CTRelationship))
        }
        nodeStateVars.foreach {
          slots.newLong(_, nullable, CTNode)
        }
        relStateVars.foreach {
          slots.newLong(_, nullable, CTRelationship)
        }

      case Foreach(_, variableName, listExpression, mutations) =>
        mutations.foreach {
          case c: CreatePattern =>
            c.nodes.foreach(n => slots.newLong(n.variable, false, CTNode))
            c.relationships.foreach(r => slots.newLong(r.variable, false, CTRelationship))
          case _ =>
        }
        val typeGetter = semanticTable.typeFor(listExpression)
        val listOfNodes = typeGetter.is(CTList(CTNode))
        val listOfRels = typeGetter.is(CTList(CTRelationship))

        (listOfNodes, listOfRels) match {
          case (true, false) => slots.newLong(variableName, true, CTNode)
          case (false, true) => slots.newLong(variableName, true, CTRelationship)
          case _             => slots.newReference(variableName, true, CTAny)
        }

      case p: NullifyMetadata =>
        if (slots.getMetaDataSlot(p.key, Id(p.planId)).isEmpty) {
          throw new IllegalStateException(s"Expected MetaDataSlot for (${p.key}, ${p.planId}) to already exist.")
        }

      case p: RunQueryAt =>
        p.columns.foreach(v => slots.newReference(v.name, true, CTAny))

      case p =>
        throw new SlotAllocationFailed(s"Don't know how to handle $p")
    }
    discardUnusedSlots(lp, slots, source, None)
  }

  /**
   * Compute the slot configuration of a branching logical plan operator `lp`.
   *
   * @param lp the operator to compute slots for.
   * @param nullable true if new slots are nullable
   * @param lhs the slot configuration of the left hand side operator.
   * @param rhs the slot configuration of the right hand side operator.
   * @return the slot configuration of lp
   */
  private def allocateTwoChild(
    lp: LogicalPlan,
    nullable: Boolean,
    lhs: SlotConfiguration,
    rhs: SlotConfiguration,
    recordArgument: LogicalPlan => Unit,
    argument: SlotsAndArgument
  ): SlotConfiguration = {
    val result = lp match {
      case _: Apply =>
        rhs

      case _: TriadicSelection =>
        // TriadicSelection is essentially a special Apply which performs filtering.
        // All the slots are allocated by it's left and right children
        rhs

      case _: AbstractSemiApply =>
        lhs

      case _: AntiConditionalApply |
        _: ConditionalApply |
        _: AbstractSelectOrSemiApply =>
        // A new pipeline is not strictly needed here unless we have batching/vectorization
        recordArgument(lp)
        val result = breakingPolicy.invoke(lp, rhs, argument.slotConfiguration, applyPlans)
        rhs.foreachSlotAndAliases {
          case SlotWithKeyAndAliases(VariableSlotKey(key), slot, _) if slot.offset >= lhs.numberOfLongs =>
            result.add(key, slot.asNullable)
          case _ => // do nothing
        }
        result

      case LetSemiApply(_, _, name) =>
        lhs.newReference(name, nullable, CTBoolean)
        lhs

      case LetAntiSemiApply(_, _, name) =>
        lhs.newReference(name, nullable, CTBoolean)
        lhs

      case LetSelectOrSemiApply(_, _, name, _) =>
        lhs.newReference(name, true, CTBoolean)
        lhs

      case LetSelectOrAntiSemiApply(_, _, name, _) =>
        lhs.newReference(name, true, CTBoolean)
        lhs

      case _: CartesianProduct =>
        // A new pipeline is not strictly needed here unless we have batching/vectorization
        recordArgument(lp)
        val result = breakingPolicy.invoke(lp, lhs, argument.slotConfiguration, applyPlans)
        // For the implementation of the slotted pipe to use array copy
        // it is very important that we add the slots in the same order
        // Note, we can potentially carry discarded slots from rhs here to save memory
        rhs.addAllSlotsInOrderTo(result, argument.argumentSize)
        rhs.addArgumentAliasesTo(result, argument.argumentSize)
        result

      case RightOuterHashJoin(nodes, _, _) =>
        // A new pipeline is not strictly needed here unless we have batching/vectorization
        recordArgument(lp)
        val result = breakingPolicy.invoke(lp, rhs, argument.slotConfiguration, applyPlans)

        // Note, we can potentially carry discarded slots from lhs here to save memory
        lhs.foreachSlotAndAliasesOrdered {
          case SlotWithKeyAndAliases(VariableSlotKey(key), slot, aliases) =>
            // If the column is one of the join columns there is no need to add it again
            if (!nodes.contains(varFor(key))) {
              result.add(key, slot.asNullable)
            }
            aliases.foreach(alias => result.addAlias(alias, key))

          case SlotWithKeyAndAliases(CachedPropertySlotKey(key), _, _) =>
            result.newCachedProperty(key)

          case SlotWithKeyAndAliases(MetaDataSlotKey(key, planId), _, _) =>
            result.newMetaData(key, planId)

          case SlotWithKeyAndAliases(_: ApplyPlanSlotKey, _, _) =>
          // apply plan slots are already in the argument, and don't have to be added here

          case SlotWithKeyAndAliases(_: OuterNestedApplyPlanSlotKey, _, _) =>
          // apply plan slots are already in the argument, and don't have to be added here

          case SlotWithKeyAndAliases(DuplicatedSlotKey(key, _), slot, _) =>
            if (result.get(key).isEmpty) {
              if (slot.isLongSlot) result.newDuplicatedLongSlot(key)
              else result.newDuplicatedRefSlot(key)
            }
        }
        result

      case LeftOuterHashJoin(nodes, _, _) =>
        // A new pipeline is not strictly needed here unless we have batching/vectorization
        recordArgument(lp)
        val result = breakingPolicy.invoke(lp, lhs, argument.slotConfiguration, applyPlans)

        // Note, we can potentially carry discarded slots from rhs here to save memory
        rhs.foreachSlotAndAliasesOrdered {
          case SlotWithKeyAndAliases(VariableSlotKey(key), slot, aliases) =>
            // If the column is one of the join columns there is no need to add it again
            if (!nodes(varFor(key))) {
              result.add(key, slot.asNullable)
            }
            aliases.foreach(alias => result.addAlias(alias, key))

          case SlotWithKeyAndAliases(CachedPropertySlotKey(key), _, _) =>
            result.newCachedProperty(key)

          case SlotWithKeyAndAliases(MetaDataSlotKey(key, planId), _, _) =>
            result.newMetaData(key, planId)

          case SlotWithKeyAndAliases(_: ApplyPlanSlotKey, _, _) =>
          // apply plan slots are already in the argument, and don't have to be added here

          case SlotWithKeyAndAliases(_: OuterNestedApplyPlanSlotKey, _, _) =>
          // apply plan slots are already in the argument, and don't have to be added here

          case SlotWithKeyAndAliases(DuplicatedSlotKey(key, _), slot, _) =>
            if (result.get(key).isEmpty) {
              if (slot.isLongSlot) result.newDuplicatedLongSlot(key)
              else result.newDuplicatedRefSlot(key)
            }
        }
        result

      case NodeHashJoin(nodes, _, _) =>
        // A new pipeline is not strictly needed here unless we have batching/vectorization
        recordArgument(lp)
        val result = breakingPolicy.invoke(lp, lhs, argument.slotConfiguration, applyPlans)

        // Note, we can potentially carry discaded slots from rhs here to save memory
        rhs.foreachSlotAndAliasesOrdered {
          case SlotWithKeyAndAliases(VariableSlotKey(key), slot, aliases) =>
            // If the column is one of the join columns there is no need to add it again
            if (!nodes(varFor(key))) {
              result.add(key, slot)
            }
            aliases.foreach(alias => result.addAlias(alias, key))

          case SlotWithKeyAndAliases(CachedPropertySlotKey(key), _, _) =>
            result.newCachedProperty(key)

          case SlotWithKeyAndAliases(MetaDataSlotKey(key, planId), _, _) =>
            result.newMetaData(key, planId)

          case SlotWithKeyAndAliases(_: ApplyPlanSlotKey, _, _) =>
          // apply plan slots are already in the argument, and don't have to be added here

          case SlotWithKeyAndAliases(_: OuterNestedApplyPlanSlotKey, _, _) =>
          // nested apply plan slots are already in the argument, and don't have to be added here
          case SlotWithKeyAndAliases(DuplicatedSlotKey(key, _), slot, _) =>
            if (result.get(key).isEmpty) {
              if (slot.isLongSlot) result.newDuplicatedLongSlot(key)
              else result.newDuplicatedRefSlot(key)
            }
        }
        result

      case _: ValueHashJoin =>
        // A new pipeline is not strictly needed here unless we have batching/vectorization
        recordArgument(lp)
        rhs.addArgumentAliasesTo(lhs, argument.argumentSize)
        val result = breakingPolicy.invoke(lp, lhs, argument.slotConfiguration, applyPlans)
        // For the implementation of the slotted pipe to use array copy
        // it is very important that we add the slots in the same order
        // Note, we can potentially carry discaded slots from rhs here to save memory
        rhs.foreachSlotAndAliasesOrdered(
          {
            case SlotWithKeyAndAliases(VariableSlotKey(key), slot, aliases) =>
              result.add(key, slot)
              aliases.foreach(alias => result.addAlias(alias, key))
            case SlotWithKeyAndAliases(CachedPropertySlotKey(key), _, _) =>
              result.newCachedProperty(key, shouldDuplicate = true)
            case SlotWithKeyAndAliases(MetaDataSlotKey(key, planId), _, _) =>
              result.newMetaData(key, planId)
            case SlotWithKeyAndAliases(_: ApplyPlanSlotKey, _, _) =>
            // apply plan slots are already in the argument, and don't have to be added here
            case SlotWithKeyAndAliases(_: OuterNestedApplyPlanSlotKey, _, _) =>
            // apply plan slots are already in the argument, and don't have to be added here
            case SlotWithKeyAndAliases(DuplicatedSlotKey(key, _), slot, _) =>
              if (result.get(key).isEmpty) {
                if (slot.isLongSlot) result.newDuplicatedLongSlot(key)
                else result.newDuplicatedRefSlot(key)
              }
          },
          skipFirst = argument.argumentSize
        )
        result

      case RollUpApply(_, _, collectionName, _) =>
        lhs.newReference(collectionName, nullable, CTList(CTAny))
        lhs

      case _: ForeachApply =>
        lhs

      case _: Union |
        _: OrderedUnion =>
        // The result slot configuration should only contain the variables we join on.
        // If both lhs and rhs has a long slot with the same type the result should
        // also use a long slot, otherwise we use a ref slot.
        val result = SlotConfiguration.empty
        def addVariableToResult(key: String, slot: Slot): Unit = slot match {
          case lhsSlot: LongSlot =>
            // find all shared variables and look for other long slots with same type
            rhs.get(key).foreach {
              case LongSlot(_, rhsNullable, typ) if typ == lhsSlot.typ =>
                result.newLong(key, lhsSlot.nullable || rhsNullable, typ)
              case rhsSlot =>
                val newType = if (lhsSlot.typ == rhsSlot.typ) lhsSlot.typ else CTAny
                result.newReference(key, lhsSlot.nullable || rhsSlot.nullable, newType)
            }
          case lhsSlot =>
            // We know lhs uses a ref slot so just look for shared variables.
            rhs.get(key).foreach {
              rhsSlot =>
                val newType = if (lhsSlot.typ == rhsSlot.typ) lhsSlot.typ else CTAny
                result.newReference(key, lhsSlot.nullable || rhsSlot.nullable, newType)
            }
        }
        // Note, we can potentially carry discaded slots from lhs/rhs here to save memory
        // First, add original variable names, cached properties and apply plan slots in order
        lhs.foreachSlotAndAliasesOrdered({
          case SlotWithKeyAndAliases(VariableSlotKey(key), slot, _) => addVariableToResult(key, slot)
          case SlotWithKeyAndAliases(CachedPropertySlotKey(key), _, _) =>
            if (rhs.hasCachedPropertySlot(key)) {
              result.newCachedProperty(key)
            }
          case SlotWithKeyAndAliases(MetaDataSlotKey(key, planId), _, _) =>
            if (rhs.hasMetaDataSlot(key, planId)) {
              result.newMetaData(key, planId)
            }
          case SlotWithKeyAndAliases(ApplyPlanSlotKey(id), _, _) =>
            // apply plan slots need to be copied if both sides have them,
            // i.e. if the union sits _under_ the apply with this id
            if (rhs.hasArgumentSlot(id)) {
              result.newArgument(id)
            }
          case SlotWithKeyAndAliases(OuterNestedApplyPlanSlotKey(id), _, _) =>
            // nested apply plan slots need to be copied if both sides have them,
            // i.e. if the union sits _under_ the trail with this id
            if (rhs.hasNestedArgumentSlot(id)) {
              result.newNestedArgument(id)
            }
        })

        // Second, add aliases in order. Aliases get their own slots after a union.
        lhs.foreachSlotAndAliasesOrdered({
          case SlotWithKeyAndAliases(VariableSlotKey(key), slot, aliases) =>
            if (argument.isArgument(slot)) {
              aliases.foreach(alias =>
                if (rhs.get(alias).contains(slot)) {
                  result.addAlias(alias, key)
                } else {
                  addVariableToResult(alias, slot)
                }
              )
            } else {
              aliases.foreach(addVariableToResult(_, slot))
            }
          case _ =>
        })
        result

      case _: AssertSameNode | _: AssertSameRelationship =>
        lhs

      case _: SubqueryForeach =>
        breakingPolicy.invoke(lp, lhs, argument.slotConfiguration, applyPlans)

      case t: TransactionForeach =>
        t.maybeReportAs.foreach { statusVar =>
          lhs.newReference(statusVar, nullable, CTMap)
        }

        breakingPolicy.invoke(lp, lhs, argument.slotConfiguration, applyPlans)

      case t: TransactionApply =>
        // We need to declare the slot for the status variable
        t.maybeReportAs.foreach { statusVar =>
          rhs.newReference(statusVar, nullable, CTMap)
        }

        if (t.onErrorBehaviour != OnErrorFail) {
          // We need to make slots for variables inside the CALL {...} nullable,
          // e.g. in CALL { CREATE p: Person(age : ...) RETURN p }, that's p,
          // because we want to see a NULL if one of the transactions failed
          val nullableVars = t.right.availableSymbols.map(_.name) -- t.left.availableSymbols.map(_.name)
          nullableVars.foreach { name =>
            rhs.get(name).foreach { slot =>
              rhs.replaceExistingSlot(name, slot, slot.asNullable)
            }
          }
        }

        breakingPolicy.invoke(lp, rhs, argument.slotConfiguration, applyPlans)

      case _: Trail =>
        recordArgument(lp)
        breakingPolicy.invoke(lp, rhs, argument.slotConfiguration, applyPlans)

      case p =>
        throw new SlotAllocationFailed(s"Don't know how to handle $p")
    }
    discardUnusedSlots(lp, result, lhs, Some(rhs))
    result
  }

  private def allocateLhsOfApply(
    plan: LogicalPlan,
    nullable: Boolean,
    lhs: SlotConfiguration,
    semanticTable: SemanticTable
  ): Unit =
    plan match {
      case ForeachApply(_, _, variableName, listExpression) =>
        // The slot for the iteration variable of foreach needs to be available as an argument on the rhs of the apply
        // so we allocate it on the lhs (even though its value will not be needed after the foreach is done)
        val typeGetter = semanticTable.typeFor(listExpression)
        val listOfNodes = typeGetter.is(CTList(CTNode))
        val listOfRels = typeGetter.is(CTList(CTRelationship))

        (listOfNodes, listOfRels) match {
          case (true, false) => lhs.newLong(variableName, true, CTNode)
          case (false, true) => lhs.newLong(variableName, true, CTRelationship)
          case _             => lhs.newReference(variableName, true, CTAny)
        }

      case Trail(_, _, _, _, end, innerStart, _, groupNodes, groupRelationships, _, _, _, _) =>
        // The slot for the per-repetition inner node variable of Trail needs to be available as an argument on the RHS of the Trail
        // so we allocate it on the LHS (even though its value will not be needed after the Trail is done).
        // Additionally, to avoid copying rows emitted by Trail, all Trail slots are allocated on the LHS.

        lhs.newLong(innerStart, nullable, CTNode)
        lhs.newLong(end, nullable, CTNode)
        groupNodes.foreach(n => lhs.newReference(n.group, nullable, CTList(CTNode)))
        groupRelationships.foreach(r => lhs.newReference(r.group, nullable, CTList(CTRelationship)))
        lhs.newMetaData(TRAIL_STATE_METADATA_KEY, plan.id)

      case _ =>
    }

  private def addGroupingSlots(
    groupingExpressions: Map[LogicalVariable, Expression],
    incoming: SlotConfiguration,
    outgoing: SlotConfiguration
  ): Unit = {
    groupingExpressions foreach {
      case (key, internal.expressions.Variable(ident)) =>
        val slotInfo = incoming(ident)
        slotInfo.typ match {
          case CTNode | CTRelationship =>
            outgoing.newLong(key, slotInfo.nullable, slotInfo.typ)
          case _ =>
            outgoing.newReference(key, slotInfo.nullable, slotInfo.typ)
        }
      case (key, _) =>
        outgoing.newReference(key, nullable = true, CTAny)
    }
  }

  private def discardUnusedSlots(
    lp: LogicalPlan,
    slots: SlotConfiguration,
    lhs: SlotConfiguration,
    rhs: Option[SlotConfiguration]
  ): Unit = {
    def doDiscard(source: SlotConfiguration): Unit = {
      // Operators for these plans will 'compact' the morsel, setting discarded ref slots to null,
      // before putting it in the eager buffer. This frees memory of those slots for the remainder
      // of the query execution.
      liveVariables.getOption(lp.id) match {
        case Some(live) if slots ne source =>
          val unusedVars = lp.availableSymbols.map(_.name).diff(live)
          unusedVars.foreach(source.markDiscarded)
        case _ =>
        /*
         * We could discard even when this plan is not breaking slot config,
         * by only variables that was discarded before the last break.
         *
         * An example in slotted,
         * AllNodeScan (n:A) -> Projection n.p * 2 AS unusedVar -> Expand (n)-->(m) -> Sort (m.p) -> Produce Result (m.p)
         * Breaks slots                                            Breaks slots
         *
         * Sort do not break slots in slotted runtime, but would still be safe to discard `unusedVar`, because it has
         * no usage after the previous slot break (Expand).
         */
      }
    }

    breakingPolicy.discardPolicy(lp, applyPlans) match {
      case DoNotDiscard   => // Do nothing
      case DiscardFromLhs => doDiscard(lhs)
      case DiscardFromRhs => doDiscard(rhs.getOrElse(throw new IllegalStateException(s"Expected plan to ha $lp")))
    }
  }

  private def allocateUnwind(
    variable: LogicalVariable,
    nullable: Boolean,
    expression: Expression,
    slots: SlotConfiguration
  ): Unit = {
    val nullableExpression = expression match {
      case ListLiteral(expressions) => expressions.exists {
          case Null()             => true
          case _: Literal         => false
          case v: LogicalVariable => slots.get(v.name).exists(_.nullable)
          case _                  => true
        }
      case v: LogicalVariable => slots.get(v.name).forall(_.nullable)
      case _                  => true
    }

    slots.newReference(variable, nullableExpression || nullable, CTAny)
  }
}

class SlotAllocationFailed(str: String) extends InternalException(str)
