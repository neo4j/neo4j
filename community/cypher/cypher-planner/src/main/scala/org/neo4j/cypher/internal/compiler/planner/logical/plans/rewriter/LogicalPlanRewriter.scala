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

import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.compiler.phases.CompilationContains
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanCondition
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.phases.ValidateAvailableSymbols
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.LogicalPlanContainsIDReferences
import org.neo4j.cypher.internal.compiler.planner.logical.steps.CompressPlanIDs
import org.neo4j.cypher.internal.compiler.planner.logical.steps.SortPredicatesBySelectivity
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.LOGICAL_PLANNING
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.options.CypherEagerAnalyzerOption
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.EffectiveCardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.LabelAndRelTypeInfos
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.rewriting.rewriters.UniquenessRewriter
import org.neo4j.cypher.internal.rewriting.rewriters.VarLengthRewriter
import org.neo4j.cypher.internal.rewriting.rewriters.combineHasLabels
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Rewriter
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
    fixedPoint(context.cancellationChecker)(
      inSequence(context.cancellationChecker)(
        ForAllRepetitionsPredicateRewriter(anonymousVariableNameGenerator),
        RemoveUnusedGroupVariablesRewriter,
        TrailToVarExpandRewriter(
          labelAndRelTypeInfos,
          otherAttributes.withAlso(solveds, cardinalities, effectiveCardinalities, providedOrders)
        ),
        fuseSelections,
        unnestApply(
          solveds,
          cardinalities,
          providedOrders,
          otherAttributes.withAlso(effectiveCardinalities, labelAndRelTypeInfos)
        ),
        unnestCartesianProduct,
        if (context.eagerAnalyzer == CypherEagerAnalyzerOption.lp) identity
        else cleanUpEager(
          cardinalities,
          otherAttributes.withAlso(solveds, effectiveCardinalities, labelAndRelTypeInfos, providedOrders)
        ),
        simplifyPredicates,
        unnestOptional,
        predicateRemovalThroughJoins(
          solveds,
          cardinalities,
          otherAttributes.withAlso(effectiveCardinalities, labelAndRelTypeInfos, providedOrders)
        ),
        removeIdenticalPlans(otherAttributes.withAlso(
          cardinalities,
          effectiveCardinalities,
          labelAndRelTypeInfos,
          solveds,
          providedOrders
        )),
        pruningVarExpander(anonymousVariableNameGenerator, VarExpandRewritePolicy.default),
        // Only used on read-only queries, until rewriter is tested to work with cleanUpEager
        if (readOnly) bfsAggregationRemover else identity,
        // Only used on read-only queries, until rewriter is tested to work with cleanUpEager
        if (context.executionModel.providedOrderPreserving && readOnly) bfsDepthOrderer else identity,
        useTop,
        skipInPartialSort,
        simplifySelections,
        limitNestedPlanExpressions(
          cardinalities,
          otherAttributes.withAlso(effectiveCardinalities, labelAndRelTypeInfos, solveds, providedOrders)
        ),
        combineHasLabels,
        truncateDatabaseDeeagerizer,
        UniquenessRewriter(anonymousVariableNameGenerator),
        VarLengthRewriter,
        extractRuntimeConstants(anonymousVariableNameGenerator),
        groupPercentileFunctions(
          anonymousVariableNameGenerator,
          otherAttributes.withAlso(solveds, cardinalities, effectiveCardinalities, providedOrders)
        )
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
    LogicalPlanCondition.wrap(ValidateAvailableSymbols)
  )

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set(
    // Rewriting logical plans introduces new IDs
    CompressPlanIDs.completed,
    // fuseSelections and simplifySelections can invalidate this condition
    SortPredicatesBySelectivity.completed
  )

  override def getTransformer(
    pushdownPropertyReads: Boolean,
    semanticFeatures: Seq[SemanticFeature]
  ): LogicalPlanRewriter = this
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
