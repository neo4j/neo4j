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

import org.neo4j.cypher.internal.compiler.phases.CompilationContains
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.phases.ValidateAvailableSymbols
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.LogicalPlanContainsIDReferences
import org.neo4j.cypher.internal.compiler.planner.logical.steps.CompressPlanIDs
import org.neo4j.cypher.internal.compiler.planner.logical.steps.SortPredicatesBySelectivity
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.LOGICAL_PLANNING
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerConfig
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.DatabaseMode
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.EffectiveCardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.LabelAndRelTypeInfos
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.rewriting.rewriters.ElementUniquenessRewriter
import org.neo4j.cypher.internal.rewriting.rewriters.VarLengthBoundPredicateRewriter
import org.neo4j.cypher.internal.rewriting.rewriters.combineHasLabels
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.Rewriter.BottomUpMergeableRewriter
import org.neo4j.cypher.internal.util.Rewriter.TopDownMergeableRewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.helpers.fixedPoint
import org.neo4j.cypher.internal.util.inSequence

case object LogicalPlanRewritten extends StepSequencer.Condition
case object AndedPropertyInequalitiesRemoved extends StepSequencer.Condition

/**
 * Optimize logical plans using heuristic rewriting.
 *
 * Rewriters that live here are required to adhere to the contract of
 * receiving a valid plan and producing a valid plan. It should be possible
 * to disable any and all of these rewriters, and still produce correct behavior.
 */
case object PlanRewriter extends LogicalPlanRewriter with StepSequencer.Step with PlanPipelineTransformerFactory {

  override def instance(
    context: PlannerContext,
    solveds: Solveds,
    cardinalities: Cardinalities,
    effectiveCardinalities: EffectiveCardinalities,
    providedOrders: ProvidedOrders,
    labelAndRelTypeInfos: LabelAndRelTypeInfos,
    otherAttributes: Attributes[LogicalPlan],
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    readOnly: Boolean
  ): Rewriter = {
    val isShardedDatabase = context.planContext.databaseMode == DatabaseMode.SHARDED
    val trailToVarExpandRewriter = RepeatToVarExpandRewriter.forGeneralCase(
      labelAndRelTypeInfos,
      otherAttributes.withAlso(solveds, cardinalities, effectiveCardinalities, providedOrders),
      anonymousVariableNameGenerator,
      isShardedDatabase,
      context
    )
    val pruningVarExpanderRewriter = pruningVarExpander(anonymousVariableNameGenerator, VarExpandRewritePolicy.default)

    val trailWithTwoFiltersToPruningVarExpandRewriter = trailWithTwoFiltersToPruningVarExpand(
      trailRewriter = RepeatToVarExpandRewriter.forEnablingPruningVarExpand(trailToVarExpandRewriter),
      pruningRewriter = pruningVarExpanderRewriter
    )

    // not fusing these allows UnnestApply to do more work
    val dontFuseRewriters: Seq[Rewriter] = Seq(
      Some(ForAllRepetitionsPredicateRewriter(
        anonymousVariableNameGenerator,
        solveds,
        cardinalities,
        providedOrders,
        context.logicalPlanIdGen
      )),
      Option.when(isShardedDatabase)(RemoveUnusedVariablesFromOption),
      Some(RemoveUnusedGroupVariablesRewriter),
      Option.when(context.config.gpmShortestToLegacyShortestEnabled())(
        StatefulShortestToFindShortestRewriter(
          solveds,
          anonymousVariableNameGenerator,
          isShardedDatabase
        )
      ),
      Some(trailToVarExpandRewriter),
      Some(fuseSelections),
      Some(UnnestApply(
        solveds,
        cardinalities,
        providedOrders,
        otherAttributes.withAlso(effectiveCardinalities, labelAndRelTypeInfos),
        context.cancellationChecker
      ))
    ).flatten

    val rewritersAfterUnnestApply: Seq[Rewriter] = Seq(
      Some(unnestCartesianProduct),
      Some(simplifyPredicates),
      Some(unnestOptional),
      Some(predicateRemovalThroughJoins(
        solveds,
        cardinalities,
        otherAttributes.withAlso(effectiveCardinalities, labelAndRelTypeInfos, providedOrders)
      )),
      Some(pushSemiApplyAboveAggregation(
        context.planContext,
        solveds,
        cardinalities,
        providedOrders,
        otherAttributes.withAlso(effectiveCardinalities, labelAndRelTypeInfos)
      )),
      Some(removeIdenticalPlans(otherAttributes.withAlso(
        cardinalities,
        effectiveCardinalities,
        labelAndRelTypeInfos,
        solveds,
        providedOrders
      ))),
      Some(inSequence(
        pruningVarExpanderRewriter,
        trailWithTwoFiltersToPruningVarExpandRewriter
      )),
      Some(collectDistinctRewriter),
      // Only used on read-only queries, until rewriter is tested to work with cleanUpEager
      Option.when(readOnly)(bfsAggregationRemover),
      // Only used on read-only queries, until rewriter is tested to work with cleanUpEager
      // To support PartialSort/PartialTop, which is introduced in bfsDepthOrderer the runtime needs
      // to be able to preserve order
      Option.when(context.executionModel.providedOrderPreserving && readOnly)(bfsDepthOrderer),
      Some(useTop),
      Some(skipInPartialSort),
      Some(simplifySelections),
      Some(limitNestedPlanExpressions(
        cardinalities,
        otherAttributes.withAlso(effectiveCardinalities, labelAndRelTypeInfos, solveds, providedOrders)
      )),
      Some(combineHasLabels),
      Some(truncateDatabaseDeeagerizer),
      Some(ElementUniquenessRewriter(anonymousVariableNameGenerator)),
      Some(VarLengthBoundPredicateRewriter),
      Some(extractRuntimeConstants(
        anonymousVariableNameGenerator,
        context.cancellationChecker
      )),
      Some(groupPercentileFunctions(
        anonymousVariableNameGenerator,
        otherAttributes.withAlso(solveds, cardinalities, effectiveCardinalities, providedOrders)
      )),
      Some(AllReduceSingletonRewriter(
        anonymousVariableNameGenerator,
        solveds,
        cardinalities,
        providedOrders,
        context.logicalPlanIdGen
      )),
      Some(AllReduceFallback(anonymousVariableNameGenerator)),
      Option.when(
        context.config.mergeOptimizationEnabled()
      )(mergeRewriter(context.planContext.storageSupportsFastExpandInto))
    ).flatten

    val (bottomUps, topDowns, others) = rewritersAfterUnnestApply.foldLeft((
      Vector.empty[BottomUpMergeableRewriter],
      Vector.empty[TopDownMergeableRewriter],
      Vector.empty[Rewriter]
    )) {
      case ((bottomUps, topDowns, others), r: BottomUpMergeableRewriter) =>
        (bottomUps :+ r, topDowns, others)
      case ((bottomUps, topDowns, others), r: TopDownMergeableRewriter) =>
        (bottomUps, topDowns :+ r, others)
      case ((bottomUps, topDowns, others), r) =>
        (bottomUps, topDowns, others :+ r)
    }

    val allRewritersMerged = dontFuseRewriters ++ Seq(
      Rewriter.mergeBottomUp(bottomUps: _*),
      Rewriter.mergeTopDown(topDowns: _*)
    ) ++ others

    fixedPoint(context.cancellationChecker)(
      inSequence(context.cancellationChecker)(
        allRewritersMerged: _*
      )
    )
  }

  override def preConditions: Set[StepSequencer.Condition] = Set(
    // The rewriters operate on the LogicalPlan
    CompilationContains[LogicalPlan](),
    // Rewriters mess with IDs so let's rather have this run before Eagerness analysis.
    !LogicalPlanContainsIDReferences
  )

  override def postConditions: Set[StepSequencer.Condition] = Set(
    LogicalPlanRewritten,
    // This belongs to simplifyPredicates
    AndedPropertyInequalitiesRemoved,
    ValidateAvailableSymbols,
    NoAllReducePredicatesLeft
  )

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set(
    // Rewriting logical plans introduces new IDs
    CompressPlanIDs.completed,
    // fuseSelections and simplifySelections can invalidate this condition
    SortPredicatesBySelectivity.completed
  )

  override def getTransformer(planPipelineConfig: PlanPipelineTransformerConfig): LogicalPlanRewriter = this

  /**
   * Special case for rewriting Trails with two Filters:
   *
   * .trail(...)
   * .|.filter(..., isRepeatTrailUnique(r))
   * .|.expandAll(...)
   * .|.filter(...)
   * .|.argument(...)
   * .lhs
   *
   * But _only_ if the resulting VarExpand is in turn rewritable by [[pruningVarExpander]].
   */
  private def trailWithTwoFiltersToPruningVarExpand(
    trailRewriter: RepeatToVarExpandRewriter,
    pruningRewriter: Rewriter
  ): Rewriter = {
    new Rewriter {
      override def apply(start: AnyRef): AnyRef = {
        val intermediate = trailRewriter(start)
        val result = pruningRewriter(intermediate)

        if (intermediate != result) {
          // pruningRewriter did some work, accept rewrite
          result
        } else {
          // undo trailRewriter work otherwise
          start
        }
      }
    }
  }
}

trait LogicalPlanRewriter extends Phase[PlannerContext, LogicalPlanState, LogicalPlanState] {
  self: Product =>

  override def phase: CompilationPhase = LOGICAL_PLANNING

  def instance(
    context: PlannerContext,
    solveds: Solveds,
    cardinalities: Cardinalities,
    effectiveCardinalities: EffectiveCardinalities,
    providedOrders: ProvidedOrders,
    labelAndRelTypeInfos: LabelAndRelTypeInfos,
    otherAttributes: Attributes[LogicalPlan],
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    readOnly: Boolean
  ): Rewriter

  override def process(from: LogicalPlanState, context: PlannerContext): LogicalPlanState = {
    val idGen = context.logicalPlanIdGen
    val otherAttributes = Attributes[LogicalPlan](
      idGen,
      from.planningAttributes.leveragedOrders
    )
    val rewritten = from.logicalPlan.endoRewrite(
      instance(
        context,
        from.planningAttributes.solveds,
        from.planningAttributes.cardinalities,
        from.planningAttributes.effectiveCardinalities,
        from.planningAttributes.providedOrders,
        from.planningAttributes.labelAndRelTypeInfos,
        otherAttributes,
        from.anonymousVariableNameGenerator,
        from.query.readOnly
      )
    )
    from.copy(maybeLogicalPlan = Some(rewritten))
  }
}
