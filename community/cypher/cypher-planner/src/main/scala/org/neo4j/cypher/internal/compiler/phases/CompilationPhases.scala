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
package org.neo4j.cypher.internal.compiler.phases

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.compiler.AdministrationCommandPlanBuilder
import org.neo4j.cypher.internal.compiler.SchemaCommandPlanBuilder
import org.neo4j.cypher.internal.compiler.UnsupportedSystemCommand
import org.neo4j.cypher.internal.compiler.planner.CheckForUnresolvedTokens
import org.neo4j.cypher.internal.compiler.planner.ResolveTokens
import org.neo4j.cypher.internal.compiler.planner.VerifyGraphTarget
import org.neo4j.cypher.internal.compiler.planner.logical.DeriveEagerAnalyzerOption
import org.neo4j.cypher.internal.compiler.planner.logical.EmptyRelationshipListEndpointProjection
import org.neo4j.cypher.internal.compiler.planner.logical.GetDegreeRewriterStep
import org.neo4j.cypher.internal.compiler.planner.logical.InlineRelationshipTypePredicates
import org.neo4j.cypher.internal.compiler.planner.logical.MoveQuantifiedPathPatternPredicates
import org.neo4j.cypher.internal.compiler.planner.logical.OptionalMatchRemover
import org.neo4j.cypher.internal.compiler.planner.logical.QueryPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.StatefulShortestPlanningHintsInserter
import org.neo4j.cypher.internal.compiler.planner.logical.UnfulfillableQueryRewriter
import org.neo4j.cypher.internal.compiler.planner.logical.VarLengthQuantifierMerger
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.CardinalityRewriter
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.ParameterToDefaultRewriter
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.PlanRewriter
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.EagerRewriter
import org.neo4j.cypher.internal.compiler.planner.logical.steps.CompressPlanIDs
import org.neo4j.cypher.internal.compiler.planner.logical.steps.InsertCachedProperties
import org.neo4j.cypher.internal.compiler.planner.logical.steps.SortPredicatesBySelectivity
import org.neo4j.cypher.internal.frontend.phases.AmbiguousAggregationAnalysis
import org.neo4j.cypher.internal.frontend.phases.AstRewriting
import org.neo4j.cypher.internal.frontend.phases.BaseContains
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CopyQuantifiedPathPatternPredicatesToJuxtaposedNodes
import org.neo4j.cypher.internal.frontend.phases.FrontEndCompilationPhases
import org.neo4j.cypher.internal.frontend.phases.If
import org.neo4j.cypher.internal.frontend.phases.MoveBoundaryNodePredicates
import org.neo4j.cypher.internal.frontend.phases.Namespacer
import org.neo4j.cypher.internal.frontend.phases.ObfuscationMetadataCollection
import org.neo4j.cypher.internal.frontend.phases.PreparatoryRewriting
import org.neo4j.cypher.internal.frontend.phases.ProcedureAndFunctionDeprecationWarnings
import org.neo4j.cypher.internal.frontend.phases.ProcedureWarnings
import org.neo4j.cypher.internal.frontend.phases.ProjectNamedPathsRewriter
import org.neo4j.cypher.internal.frontend.phases.SemanticAnalysis
import org.neo4j.cypher.internal.frontend.phases.ShortestPathVariableDeduplicator
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.collapseMultipleInPredicates
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.frontend.phases.isolateAggregation
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.CNFNormalizer
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.rewriteEqualityToInPredicate
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.simplifyPredicates
import org.neo4j.cypher.internal.frontend.phases.transitiveEqualities
import org.neo4j.cypher.internal.rewriting.rewriters.computeDependenciesForExpressions.ExpressionsHaveComputedDependencies
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.AccumulatedSteps

object CompilationPhases extends FrontEndCompilationPhases {

  // these steps work on LogicalPlanState.maybeStatement, up until LogicalPlanState.maybeQuery is created
  private val AccumulatedSteps(astPlanPipelineSteps, astPlanPipelinePostConditions) =
    StepSequencer[StepSequencer.Step with PlanPipelineTransformerFactory]()
      .orderSteps(
        Set(
          SemanticAnalysis,
          Namespacer,
          ProjectNamedPathsRewriter,
          isolateAggregation,
          transitiveEqualities,
          rewriteEqualityToInPredicate,
          collapseMultipleInPredicates,
          ResolveTokens,
          VerifyGraphTarget,
          CopyQuantifiedPathPatternPredicatesToJuxtaposedNodes,
          MoveBoundaryNodePredicates,
          ShortestPathVariableDeduplicator
        ) ++ CNFNormalizer.steps,
        initialConditions =
          Set(BaseContains[Statement]())
            ++ PreparatoryRewriting.postConditions
            ++ AstRewriting.postConditions
            // ExpressionsHaveComputedDependencies is introduced by SemanticAnalysis.
            // It is currently not allowed to then also have it as an initial condition
            - ExpressionsHaveComputedDependencies
      )

  // these steps work on LogicalPlanState.maybeQuery, up until LogicalPlanState.maybeLogicalPlan is created
  private val AccumulatedSteps(irPlanPipelineSteps, irPlanPipelinePostConditions) =
    StepSequencer[StepSequencer.Step with PlanPipelineTransformerFactory]()
      .orderSteps(
        Set(
          CreatePlannerQuery,
          OptionalMatchRemover,
          EmptyRelationshipListEndpointProjection,
          GetDegreeRewriterStep,
          InlineRelationshipTypePredicates,
          UnfulfillableQueryRewriter,
          VarLengthQuantifierMerger,
          CheckForUnresolvedTokens,
          MoveQuantifiedPathPatternPredicates,
          StatefulShortestPlanningHintsInserter
        ),
        initialConditions = astPlanPipelinePostConditions
      )

  // these steps work on LogicalPlanState.maybeLogicalPlan
  private val AccumulatedSteps(lpPlanPipelineSteps, _) =
    StepSequencer[StepSequencer.Step with PlanPipelineTransformerFactory]()
      .orderSteps(
        Set(
          DeriveEagerAnalyzerOption,
          QueryPlanner,
          PlanRewriter,
          InsertCachedProperties,
          CardinalityRewriter,
          CompressPlanIDs,
          EagerRewriter,
          SortPredicatesBySelectivity,
          ParameterToDefaultRewriter
        ),
        initialConditions = irPlanPipelinePostConditions
      )

  private val orderedPlanPipelineSteps = astPlanPipelineSteps ++ irPlanPipelineSteps ++ lpPlanPipelineSteps

  // Phase 2
  val prepareForCaching: Transformer[PlannerContext, BaseState, BaseState] =
    RewriteProcedureCalls andThen
      AmbiguousAggregationAnalysis() andThen
      ProcedureAndFunctionDeprecationWarnings andThen
      ProcedureWarnings andThen
      ObfuscationMetadataCollection

  // Phase 3
  def planPipeLine(
    pushdownPropertyReads: Boolean = true,
    semanticFeatures: Seq[SemanticFeature] = defaultSemanticFeatures
  ): Transformer[PlannerContext, BaseState, LogicalPlanState] =
    SchemaCommandPlanBuilder andThen
      If((s: LogicalPlanState) => s.maybeLogicalPlan.isEmpty)(
        Chainer.chainTransformers(
          orderedPlanPipelineSteps.map(_.getTransformer(pushdownPropertyReads, semanticFeatures))
        ).asInstanceOf[Transformer[PlannerContext, BaseState, LogicalPlanState]]
      )

  // Alternative Phase 3
  def systemPipeLine: Transformer[PlannerContext, BaseState, LogicalPlanState] =
    RewriteProcedureCalls andThen
      simplifyPredicates andThen
      AdministrationCommandPlanBuilder andThen
      If((s: LogicalPlanState) => s.maybeLogicalPlan.isEmpty)(
        UnsupportedSystemCommand
      ) andThen
      If((s: LogicalPlanState) => s.maybeLogicalPlan.isDefined)(
        CompressPlanIDs
      )
}
