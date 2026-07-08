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
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.SecretsManager
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.ShowSetting
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.UUIDType
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.UserTags
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.ValueInListProperty
import org.neo4j.cypher.internal.frontend.phases.factories.ParsingConfig
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.AstRewriting
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.ExtractLocalDefinitions
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.LiteralExtraction
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.Parse
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.ParsePipelineTransformer
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.ResolveSimpleDynamicExpressions
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.SemanticAnalysis
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.ScopeSurveyor
import org.neo4j.graphdb.config.Setting
import org.neo4j.values.virtual.MapValue

trait FrontEndCompilationPhases {

  val defaultSemanticFeatures: Seq[String] = Seq(
    MultipleDatabases.productPrefix,
    ShowSetting.productPrefix,
    OidcCredentialForwarding.productPrefix,
    GraphTypes.productPrefix,
    RelationshipPropertyValueAccessRules.productPrefix,
    AttributeBasedAccessControl.productPrefix,
    ComposableCommands.productPrefix,
    EnableWorkingScopeNamespacer.productPrefix,
    UserTags.productPrefix,
    GroupByClause.productPrefix
  )

  def enabledSemanticFeatures(features: Set[String]): Seq[SemanticFeature] =
    features.map(SemanticFeature.fromString).toSeq

  /**
   * Steps in the parse pipeline that run before the obfuscator is ready. The obfuscator is
   * wired onto the ExecutingQuery only after the post steps (including semantic analysis) complete,
   * so on failure anywhere before that not carry the `query` field in the logs.
   */
  private def parsingBasePre(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] =
    Parse andThen ScopeSurveyor andThen
      // Needs to be done before any other rewrites to not miss literals
      ObfuscationMetadataCollection andThen
      ParsePipelineTransformer.getPreObfuscatorTransformer(config)

  /**
   * Steps in the parse pipeline that include semantic analysis, semantics-dependent rewriting, and
   * obfuscation metadata collection. These run before the obfuscator is wired onto the
   * ExecutingQuery, so a failure here (e.g. a semantic error) leaves the query text withheld from
   * the debug log under obfuscation.
   */
  private def parsingBasePost(
    config: ParsingConfig,
    parameters: MapValue
  ): Transformer[BaseContext, BaseState, BaseState] = {
    ParsePipelineTransformer.getPostObfuscatorTransformer(config) andThen
      If((_: BaseState) => config.resolveSimpleDynamicExpressions)(
        IfChangedSetSemantics.using(ResolveSimpleDynamicExpressions(parameters))
      ) andThen
      SemanticAnalysis.ifSemanticsNotUpToDate(warn = Some(false))
  }

  // Phase 1 — pre-obfuscator portion.
  def parsingPre(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] = parsingBasePre(config)

  // Phase 1 — post-obfuscator portion.
  def parsingPost(
    config: ParsingConfig,
    parameters: MapValue = MapValue.EMPTY
  ): Transformer[BaseContext, BaseState, BaseState] =
    parsingBasePost(config, parameters) andThen
      AstRewriting(parameterTypeMapping = config.parameterTypeMapping) andThen
      LiteralExtraction(config.literalExtractionStrategy) andThen
      SetReturnColumns

  // Phase 1 — full chain. Convenience for callers that don't need to interpose at the boundary.
  def parsing(
    config: ParsingConfig,
    parameters: MapValue = MapValue.EMPTY
  ): Transformer[BaseContext, BaseState, BaseState] =
    parsingPre(config) andThen parsingPost(config, parameters)

  // Phase 1 (Fabric)
  def fabricParsing(
    config: ParsingConfig,
    parameters: MapValue
  ): Transformer[BaseContext, BaseState, BaseState] = {
    parsingBasePre(config) andThen
      parsingBasePost(config, parameters) andThen
      SemanticAnalysis(warn = Some(true)) andThen
      SetReturnColumns
  }

  // Phase 1.1 (Fabric)
  def fabricFinalize(
    config: ParsingConfig,
    resolver: ScopedProcedureSignatureResolver
  ): Transformer[BaseContext, BaseState, BaseState] = {
    ScopeSurveyor andThen
      StrictResolveCallables(resolver) andThen
      LiteralExtraction(config.literalExtractionStrategy) andThen
      SemanticAnalysis(warn = Some(true)) andThen
      AstRewriting(parameterTypeMapping = config.parameterTypeMapping) andThen
      SemanticAnalysis(warn = Some(true)) andThen
      ObfuscationMetadataCollection andThen
      ExtractLocalDefinitions
  }
}

object FrontEndCompilationPhases extends FrontEndCompilationPhases {

  def settingToFeatureMapping: Seq[(Setting[java.lang.Boolean], String)] = {
    Seq(
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
      GraphDatabaseInternalSettings.user_tags -> UserTags.productPrefix,
      GraphDatabaseInternalSettings.value_in_list_property -> ValueInListProperty.productPrefix,
      GraphDatabaseInternalSettings.secrets_manager_enabled -> SecretsManager.productPrefix
    )
  }
}
