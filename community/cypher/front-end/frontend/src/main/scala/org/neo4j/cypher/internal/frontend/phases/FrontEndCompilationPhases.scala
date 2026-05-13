/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseInternalSettings.ExtractLiteral
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.AttributeBasedAccessControl
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.ComposableCommands
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.DisableTypeCheckingInSemanticAnalysis
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.EnableParsingOfObfuscatedLiterals
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.EnableWorkingScopeNamespacer
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.ExperimentalCypherVersions
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.GraphTypes
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.GroupByClause
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.LocalCallables
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.MultipleDatabases
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.OidcCredentialForwarding
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.RelationshipPropertyValueAccessRules
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.ScopeQueries
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.ShowSetting
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.UUIDType
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.UserTags
import org.neo4j.cypher.internal.frontend.phases.factories.ParsePipelineTransformerFactory
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.AmbiguousAggregationAnalysis
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.AstRewriting
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.CollectSyntaxUsageMetrics
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.ExpandClauses
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.ExtractLocalDefinitions
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.ExtractSensitiveLiterals
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.IsolateSubqueriesInMutatingPatterns
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.LiteralExtraction
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.Parse
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.PreparatoryRewriting
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.ProcedureRelocator
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.RemoveDuplicateUseClauses
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.ReplacePatternComprehensionWithCollectSubqueryRewriter
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.ResolveLocalFunctions
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.ResolveSimpleDynamicExpressions
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.SemanticAnalysis
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.SemanticTypeCheck
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.SyntaxDeprecationWarningsAndReplacements
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.UnresolveShadowedFunctions
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.WrapAndExpandProcedureCall
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.ScopeSurveyor
import org.neo4j.cypher.internal.rewriting.Deprecations
import org.neo4j.cypher.internal.rewriting.rewriters.Forced
import org.neo4j.cypher.internal.rewriting.rewriters.IfNoParameter
import org.neo4j.cypher.internal.rewriting.rewriters.LiteralExtractionStrategy
import org.neo4j.cypher.internal.rewriting.rewriters.Never
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.AccumulatedSteps
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo
import org.neo4j.graphdb.config.Setting
import org.neo4j.values.virtual.MapValue

trait FrontEndCompilationPhases {

  // This needs to be lazy to avoid that TeaVM tries to cross-compile the configuration module to JavaScript
  lazy val settingToFeatureMapping: Seq[(Setting[java.lang.Boolean], String)] = Seq(
    GraphDatabaseInternalSettings.show_setting -> ShowSetting.productPrefix,
    GraphDatabaseInternalSettings.oidc_credential_forwarding_enabled -> OidcCredentialForwarding.productPrefix,
    GraphDatabaseInternalSettings.composable_commands -> ComposableCommands.productPrefix,
    GraphDatabaseInternalSettings.graph_type_enabled -> GraphTypes.productPrefix,
    GraphDatabaseInternalSettings.enable_experimental_cypher_versions -> ExperimentalCypherVersions.productPrefix,
    GraphDatabaseInternalSettings.relationship_property_value_access_rules -> RelationshipPropertyValueAccessRules.productPrefix,
    GraphDatabaseInternalSettings.cypher_uuid_type_enabled -> UUIDType.productPrefix,
    GraphDatabaseInternalSettings.cypher_group_by_clause_enabled -> GroupByClause.productPrefix,
    GraphDatabaseInternalSettings.cypher_enable_local_callables -> LocalCallables.productPrefix,
    GraphDatabaseInternalSettings.cypher_enable_scope_queries -> ScopeQueries.productPrefix,
    GraphDatabaseInternalSettings.cypher_enable_working_scope_namespacer -> EnableWorkingScopeNamespacer.productPrefix,
    GraphDatabaseInternalSettings.cypher_enable_parsing_of_obfuscated_literals -> EnableParsingOfObfuscatedLiterals.productPrefix,
    GraphDatabaseInternalSettings.cypher_disable_type_checking -> DisableTypeCheckingInSemanticAnalysis.productPrefix,
    GraphDatabaseInternalSettings.attribute_based_access_control -> AttributeBasedAccessControl.productPrefix,
    GraphDatabaseInternalSettings.user_tags -> UserTags.productPrefix
  )

  val defaultSemanticFeatures: Seq[String] = Seq(
    MultipleDatabases.productPrefix,
    ShowSetting.productPrefix,
    OidcCredentialForwarding.productPrefix,
    GraphTypes.productPrefix,
    RelationshipPropertyValueAccessRules.productPrefix,
    AttributeBasedAccessControl.productPrefix,
    ComposableCommands.productPrefix,
    EnableWorkingScopeNamespacer.productPrefix
  )

  def enabledSemanticFeatures(features: Set[String]): Seq[SemanticFeature] =
    features.map(SemanticFeature.fromString).toSeq

  case class ParsingConfig(
    extractLiterals: ExtractLiteral = ExtractLiteral.ALWAYS,
    /* TODO: This is not part of configuration - Move to BaseState */
    parameterTypeMapping: Map[String, ParameterTypeInfo] = Map.empty,
    obfuscateLiterals: Boolean = false,
    resolveSimpleDynamicExpressions: Boolean = false,
    enabledVirtualGraph: Boolean = false
  ) {

    def literalExtractionStrategy: LiteralExtractionStrategy = extractLiterals match {
      case ExtractLiteral.ALWAYS          => Forced
      case ExtractLiteral.NEVER           => Never
      case ExtractLiteral.IF_NO_PARAMETER => IfNoParameter
      case _ => throw new IllegalStateException(s"$extractLiterals is not a known strategy")
    }
  }

  val AccumulatedSteps(orderedSteps, _postConditions) =
    StepSequencer[StepSequencer.Step with ParsePipelineTransformerFactory]().orderSteps(
      Set(
        CollectSyntaxUsageMetrics,
        ExpandClauses,
        IsolateSubqueriesInMutatingPatterns,
        PreparatoryRewriting,
        RemoveDuplicateUseClauses,
        ReplacePatternComprehensionWithCollectSubqueryRewriter,
        SemanticAnalysis,
        SemanticTypeCheck,
        SyntaxDeprecationWarningsAndReplacements(Deprecations.SemanticallyDeprecatedFeatures),
        SyntaxDeprecationWarningsAndReplacements(Deprecations.SyntacticallyDeprecatedFeatures),
        ResolveLocalFunctions,
        UnresolveShadowedFunctions,
        WrapAndExpandProcedureCall,
        ScopeSurveyor
      ),
      initialConditions = Set(BaseContains[Statement]())
    )

  def postParsingBase(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] =
    Chainer.chainTransformers(orderedSteps.map(_.getCheckedTransformer(
      literalExtractionStrategy = config.literalExtractionStrategy,
      parameterTypeMapping = config.parameterTypeMapping,
      obfuscateLiterals = config.obfuscateLiterals
    ))).asInstanceOf[Transformer[BaseContext, BaseState, BaseState]]

  private def parsingBase(
    config: ParsingConfig,
    parameters: MapValue
  ): Transformer[BaseContext, BaseState, BaseState] = {
    Parse andThen ScopeSurveyor andThen
      If((_: BaseState) => config.obfuscateLiterals)(
        // Needs to be done before any other rewrites to not miss literals
        ExtractSensitiveLiterals.andThen(ObfuscationMetadataCollection)
      ) andThen
      postParsingBase(config) andThen
      If((_: BaseState) => config.resolveSimpleDynamicExpressions)(
        IfChangedSetSemantics.using(ResolveSimpleDynamicExpressions(parameters))
      ) andThen
      SemanticAnalysis.ifSemanticsNotUpToDate(warn = Some(false)) andThen
      ExtractLocalDefinitions
  }

  /**
   * Resolves procedure/function calls and runs related checks.
   * The rewriteCalls transformer controls resolution behavior:
   * - parsing() passes StrictRewriteProcedureCalls (throws on unresolved)
   * - fabricParsing() passes TryRewriteProcedureCalls (leaves unresolved if not found locally)
   */
  def resolveAndCheck(
    rewriteCalls: Transformer[BaseContext, BaseState, BaseState],
    conf: ParsingConfig
  ): Transformer[BaseContext, BaseState, BaseState] = {
    ProcedureRelocator.transformer(conf.enabledVirtualGraph) andThen
      ExtractLocalDefinitions andThen
      rewriteCalls andThen
      ScopeSurveyor andThen
      AmbiguousAggregationAnalysis andThen
      ProcedureAndFunctionDeprecationWarnings andThen
      ProcedureWarnings
  }

  // Phase 1
  def parsing(
    config: ParsingConfig,
    resolver: ScopedProcedureSignatureResolver,
    parameters: MapValue = MapValue.EMPTY
  ): Transformer[BaseContext, BaseState, BaseState] = {
    parsingBase(config, parameters) andThen
      AstRewriting(parameterTypeMapping = config.parameterTypeMapping) andThen
      resolveAndCheck(StrictRewriteProcedureCalls(resolver), config) andThen
      LiteralExtraction(config.literalExtractionStrategy) andThen
      ObfuscationMetadataCollection
  }

  // Phase 1 (Fabric)
  def fabricParsing(
    config: ParsingConfig,
    resolver: ScopedProcedureSignatureResolver,
    parameters: MapValue
  ): Transformer[BaseContext, BaseState, BaseState] = {
    parsingBase(config, parameters) andThen
      AstRewriting(parameterTypeMapping = config.parameterTypeMapping) andThen
      resolveAndCheck(TryRewriteProcedureCalls(resolver), config) andThen
      SemanticAnalysis(warn = Some(true)) andThen
      ObfuscationMetadataCollection
  }

  // Phase 1.1 (Fabric)
  def fabricFinalize(
    config: ParsingConfig,
    resolver: ScopedProcedureSignatureResolver
  ): Transformer[BaseContext, BaseState, BaseState] = {
    ScopeSurveyor andThen
      ExtractLocalDefinitions andThen
      StrictRewriteProcedureCalls(resolver) andThen
      LiteralExtraction(config.literalExtractionStrategy) andThen
      SemanticAnalysis(warn = Some(true)) andThen
      ObfuscationMetadataCollection
  }
}

object FrontEndCompilationPhases extends FrontEndCompilationPhases
