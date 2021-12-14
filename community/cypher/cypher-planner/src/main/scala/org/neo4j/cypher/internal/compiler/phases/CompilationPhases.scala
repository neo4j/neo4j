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
package org.neo4j.cypher.internal.compiler.phases

import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.CorrelatedSubQueries
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.MultipleDatabases
import org.neo4j.cypher.internal.compiler.AdministrationCommandPlanBuilder
import org.neo4j.cypher.internal.compiler.SchemaCommandPlanBuilder
import org.neo4j.cypher.internal.compiler.UnsupportedSystemCommand
import org.neo4j.cypher.internal.compiler.planner.CheckForUnresolvedTokens
import org.neo4j.cypher.internal.compiler.planner.ResolveTokens
import org.neo4j.cypher.internal.compiler.planner.logical.OptionalMatchRemover
import org.neo4j.cypher.internal.compiler.planner.logical.QueryPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.CardinalityRewriter
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.PlanRewriter
import org.neo4j.cypher.internal.compiler.planner.logical.steps.CompressPlanIDs
import org.neo4j.cypher.internal.compiler.planner.logical.steps.InsertCachedProperties
import org.neo4j.cypher.internal.frontend.phases.AstRewriting
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.ExpandStarRewriter
import org.neo4j.cypher.internal.frontend.phases.If
import org.neo4j.cypher.internal.frontend.phases.LiteralExtraction
import org.neo4j.cypher.internal.frontend.phases.Namespacer
import org.neo4j.cypher.internal.frontend.phases.ObfuscationMetadataCollection
import org.neo4j.cypher.internal.frontend.phases.Parsing
import org.neo4j.cypher.internal.frontend.phases.PreparatoryRewriting
import org.neo4j.cypher.internal.frontend.phases.ProjectNamedPathsRewriter
import org.neo4j.cypher.internal.frontend.phases.SemanticAnalysis
import org.neo4j.cypher.internal.frontend.phases.StatementCondition
import org.neo4j.cypher.internal.frontend.phases.SyntaxAdditionsErrors
import org.neo4j.cypher.internal.frontend.phases.SyntaxDeprecationWarningsAndReplacements
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.collapseMultipleInPredicates
import org.neo4j.cypher.internal.frontend.phases.extractSensitiveLiterals
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.frontend.phases.isolateAggregation
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.CNFNormalizer
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.rewriteEqualityToInPredicate
import org.neo4j.cypher.internal.frontend.phases.transitiveClosure
import org.neo4j.cypher.internal.planner.spi.ProcedureSignatureResolver
import org.neo4j.cypher.internal.rewriting.Additions
import org.neo4j.cypher.internal.rewriting.Deprecations
import org.neo4j.cypher.internal.rewriting.ListStepAccumulator
import org.neo4j.cypher.internal.rewriting.conditions.containsNoReturnAll
import org.neo4j.cypher.internal.rewriting.rewriters.IfNoParameter
import org.neo4j.cypher.internal.rewriting.rewriters.LiteralExtractionStrategy
import org.neo4j.cypher.internal.rewriting.rewriters.NoNamedPathsInPatternComprehensions
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.AccumulatedSteps
import org.neo4j.cypher.internal.util.symbols.CypherType

object CompilationPhases {

  val defaultSemanticFeatures = Seq(
    MultipleDatabases,
    CorrelatedSubQueries,
  )

  private val AccumulatedSteps(orderedPlanPipelineSteps, _) = StepSequencer(ListStepAccumulator[StepSequencer.Step with PlanPipelineTransformerFactory]())
    .orderSteps(
      Set(
        SemanticAnalysis,
        Namespacer,
        ProjectNamedPathsRewriter,
        isolateAggregation,
        transitiveClosure,
        rewriteEqualityToInPredicate,
        collapseMultipleInPredicates,
        ResolveTokens,
        CreatePlannerQuery,
        OptionalMatchRemover,
        QueryPlanner,
        PlanRewriter,
        InsertCachedProperties,
        CardinalityRewriter,
        CompressPlanIDs,
        CheckForUnresolvedTokens,
      ) ++ CNFNormalizer.steps,
      initialConditions = Set(StatementCondition(containsNoReturnAll), NoNamedPathsInPatternComprehensions)
    )

  case class ParsingConfig(compatibilityMode: CypherCompatibilityVersion = Compatibility4_3,
                           literalExtractionStrategy: LiteralExtractionStrategy = IfNoParameter,
                           parameterTypeMapping: Map[String, CypherType] = Map.empty,
                           semanticFeatures: Seq[SemanticFeature] = defaultSemanticFeatures,
                           useJavaCCParser: Boolean = false,
                           obfuscateLiterals: Boolean = false
  )

  private def parsingBase(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] = {
    val parse = (if (config.useJavaCCParser) JavaccParsing else Parsing)
    val compatibilityCheck: Transformer[BaseContext, BaseState, BaseState] =
      config.compatibilityMode match {
        case Compatibility3_5 =>
            SyntaxAdditionsErrors(Additions.addedFeaturesIn4_x) andThen
            SyntaxDeprecationWarningsAndReplacements(Deprecations.removedFeaturesIn4_0) andThen
            SyntaxAdditionsErrors(Additions.addedFeaturesIn4_3)
        case Compatibility4_2 =>
            SyntaxAdditionsErrors(Additions.addedFeaturesIn4_3)
        case Compatibility4_3 =>
          Transformer.identity
      }

    parse andThen
      compatibilityCheck andThen
      SyntaxDeprecationWarningsAndReplacements(Deprecations.syntacticallyDeprecatedFeaturesIn4_X) andThen
      PreparatoryRewriting andThen
      If( (_: BaseState) => config.obfuscateLiterals) (
        extractSensitiveLiterals
      ) andThen
      SemanticAnalysis(warn = true, config.semanticFeatures: _*) andThen
      SyntaxDeprecationWarningsAndReplacements(Deprecations.semanticallyDeprecatedFeaturesIn4_X)
  }

  // Phase 1
  def parsing(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] = {
    parsingBase(config) andThen
      AstRewriting(parameterTypeMapping = config.parameterTypeMapping) andThen
      LiteralExtraction(config.literalExtractionStrategy)
  }

  // Phase 1 (Fabric)
  def fabricParsing(config: ParsingConfig, resolver: ProcedureSignatureResolver): Transformer[BaseContext, BaseState, BaseState] = {
    parsingBase(config) andThen
      ExpandStarRewriter andThen
      TryRewriteProcedureCalls(resolver) andThen
      ObfuscationMetadataCollection andThen
      SemanticAnalysis(warn = true, config.semanticFeatures: _*)
  }

  // Phase 1.1 (Fabric)
  def fabricFinalize(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] = {
    SemanticAnalysis(warn = true, config.semanticFeatures: _*) andThen
      AstRewriting(parameterTypeMapping = config.parameterTypeMapping) andThen
      LiteralExtraction(config.literalExtractionStrategy) andThen
      SemanticAnalysis(warn = true, config.semanticFeatures: _*)
  }

  // Phase 2
  val prepareForCaching: Transformer[PlannerContext, BaseState, BaseState] =
    RewriteProcedureCalls andThen
      ProcedureDeprecationWarnings andThen
      ProcedureWarnings andThen
      ObfuscationMetadataCollection

  // Phase 3
  def planPipeLine(
    pushdownPropertyReads: Boolean = true,
    semanticFeatures: Seq[SemanticFeature] = defaultSemanticFeatures,
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
      AdministrationCommandPlanBuilder andThen
      If((s: LogicalPlanState) => s.maybeLogicalPlan.isEmpty)(
        UnsupportedSystemCommand
      )
}

sealed trait CypherCompatibilityVersion
case object Compatibility3_5 extends CypherCompatibilityVersion
case object Compatibility4_2 extends CypherCompatibilityVersion
case object Compatibility4_3 extends CypherCompatibilityVersion

