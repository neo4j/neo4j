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
package org.neo4j.cypher.internal.compiler.phases

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.CorrelatedSubQueries
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.MultipleDatabases
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.compiler.AdministrationCommandPlanBuilder
import org.neo4j.cypher.internal.compiler.SchemaCommandPlanBuilder
import org.neo4j.cypher.internal.compiler.UnsupportedSystemCommand
import org.neo4j.cypher.internal.compiler.planner.CheckForUnresolvedTokens
import org.neo4j.cypher.internal.compiler.planner.ResolveTokens
import org.neo4j.cypher.internal.compiler.planner.logical.OptionalMatchRemover
import org.neo4j.cypher.internal.compiler.planner.logical.QueryPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.PlanRewriter
import org.neo4j.cypher.internal.compiler.planner.logical.steps.InsertCachedProperties
import org.neo4j.cypher.internal.frontend.phases.AstRewriting
import org.neo4j.cypher.internal.frontend.phases.BaseContains
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CNFNormalizer
import org.neo4j.cypher.internal.frontend.phases.ExpandStarRewriter
import org.neo4j.cypher.internal.frontend.phases.If
import org.neo4j.cypher.internal.frontend.phases.LateAstRewriting
import org.neo4j.cypher.internal.frontend.phases.Namespacer
import org.neo4j.cypher.internal.frontend.phases.ObfuscationMetadataCollection
import org.neo4j.cypher.internal.frontend.phases.Parsing
import org.neo4j.cypher.internal.frontend.phases.PreparatoryRewriting
import org.neo4j.cypher.internal.frontend.phases.SemanticAnalysis
import org.neo4j.cypher.internal.frontend.phases.SyntaxAdditionsErrors
import org.neo4j.cypher.internal.frontend.phases.SyntaxDeprecationWarnings
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.isolateAggregation
import org.neo4j.cypher.internal.frontend.phases.rewriteEqualityToInPredicate
import org.neo4j.cypher.internal.frontend.phases.transitiveClosure
import org.neo4j.cypher.internal.ir.UnionQuery
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.ProcedureSignatureResolver
import org.neo4j.cypher.internal.rewriting.Additions
import org.neo4j.cypher.internal.rewriting.Deprecations
import org.neo4j.cypher.internal.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.rewriting.rewriters.IfNoParameter
import org.neo4j.cypher.internal.rewriting.rewriters.InnerVariableNamer
import org.neo4j.cypher.internal.rewriting.rewriters.LiteralExtraction
import org.neo4j.cypher.internal.util.symbols.CypherType

object CompilationPhases {

  val defaultSemanticFeatures = Seq(
    MultipleDatabases,
    CorrelatedSubQueries,
  )

  case class ParsingConfig(
    sequencer: String => RewriterStepSequencer,
    innerVariableNamer: InnerVariableNamer,
    compatibilityMode: CypherCompatibilityVersion = Compatibility4_1,
    literalExtraction: LiteralExtraction = IfNoParameter,
    parameterTypeMapping: Map[String, CypherType] = Map.empty,
    semanticFeatures: Seq[SemanticFeature] = defaultSemanticFeatures,
  )

  private def parsingBase(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] = {
    val parse = Parsing.adds(BaseContains[Statement])
    val parseAndCompatibilityCheck: Transformer[BaseContext, BaseState, BaseState] =
      config.compatibilityMode match {
        case Compatibility3_5 =>
          parse andThen
            SyntaxAdditionsErrors(Additions.addedFeaturesIn4_0) andThen
            SyntaxDeprecationWarnings(Deprecations.removedFeaturesIn4_0) andThen
            PreparatoryRewriting(Deprecations.removedFeaturesIn4_0) andThen
            SyntaxAdditionsErrors(Additions.addedFeaturesIn4_1) andThen
            SyntaxDeprecationWarnings(Deprecations.removedFeaturesIn4_1) andThen
            PreparatoryRewriting(Deprecations.removedFeaturesIn4_1)
        case Compatibility4_0 =>
          parse andThen
            SyntaxAdditionsErrors(Additions.addedFeaturesIn4_1) andThen
            SyntaxDeprecationWarnings(Deprecations.removedFeaturesIn4_1) andThen
            PreparatoryRewriting(Deprecations.removedFeaturesIn4_1)
        case Compatibility4_1 =>
          parse
      }

    parseAndCompatibilityCheck andThen
      SyntaxDeprecationWarnings(Deprecations.V2) andThen
      PreparatoryRewriting(Deprecations.V2) andThen
      SemanticAnalysis(warn = true, config.semanticFeatures: _*).adds(BaseContains[SemanticState])
  }

  // Phase 1
  def parsing(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] = {
    parsingBase(config) andThen
      AstRewriting(config.sequencer, config.literalExtraction, innerVariableNamer = config.innerVariableNamer, parameterTypeMapping = config.parameterTypeMapping)
  }

  // Phase 1 (Fabric)
  def fabricParsing(config: ParsingConfig, resolver: ProcedureSignatureResolver): Transformer[BaseContext, BaseState, BaseState] = {
    parsingBase(config) andThen
      ExpandStarRewriter andThen
      TryRewriteProcedureCalls(resolver) andThen
      ObfuscationMetadataCollection andThen
      SemanticAnalysis(warn = true, config.semanticFeatures: _*).adds(BaseContains[SemanticState])
  }

  // Phase 1.1 (Fabric)
  def fabricFinalize(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] = {
    SemanticAnalysis(warn = true, config.semanticFeatures: _*).adds(BaseContains[SemanticState]) andThen
      AstRewriting(config.sequencer, config.literalExtraction, innerVariableNamer = config.innerVariableNamer, parameterTypeMapping = config.parameterTypeMapping) andThen
      SemanticAnalysis(warn = true, config.semanticFeatures: _*).adds(BaseContains[SemanticState])
  }

  // Phase 2
  val prepareForCaching: Transformer[PlannerContext, BaseState, BaseState] =
    RewriteProcedureCalls andThen
      ProcedureDeprecationWarnings andThen
      ProcedureWarnings andThen
      ObfuscationMetadataCollection

  // Phase 3
  def planPipeLine(
    sequencer: String => RewriterStepSequencer,
    pushdownPropertyReads: Boolean = true,
    semanticFeatures: Seq[SemanticFeature] = defaultSemanticFeatures,
    readPropertiesFromCursor: Boolean = false,
  ): Transformer[PlannerContext, BaseState, LogicalPlanState] =
    SchemaCommandPlanBuilder andThen
      If((s: LogicalPlanState) => s.maybeLogicalPlan.isEmpty)(
        SemanticAnalysis(warn = false, semanticFeatures: _*) andThen
          Namespacer andThen
          isolateAggregation andThen
          SemanticAnalysis(warn = false, semanticFeatures: _*) andThen
          Namespacer andThen
          transitiveClosure andThen
          rewriteEqualityToInPredicate andThen
          CNFNormalizer andThen
          LateAstRewriting andThen
          SemanticAnalysis(warn = false, semanticFeatures: _*) andThen
          ResolveTokens andThen
          CreatePlannerQuery.adds(CompilationContains[UnionQuery]) andThen
          OptionalMatchRemover andThen
          QueryPlanner.adds(CompilationContains[LogicalPlan]) andThen
          PlanRewriter(sequencer) andThen
          InsertCachedProperties(pushdownPropertyReads, readPropertiesFromCursor) andThen
          If((s: LogicalPlanState) => s.query.readOnly)(
            CheckForUnresolvedTokens
          )
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
case object Compatibility4_0 extends CypherCompatibilityVersion
case object Compatibility4_1 extends CypherCompatibilityVersion

