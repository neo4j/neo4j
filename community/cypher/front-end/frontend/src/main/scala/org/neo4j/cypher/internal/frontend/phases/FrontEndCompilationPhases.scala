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

import org.neo4j.configuration.GraphDatabaseInternalSettings.ExtractLiteral
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.MultipleDatabases
import org.neo4j.cypher.internal.rewriting.Deprecations
import org.neo4j.cypher.internal.rewriting.rewriters.Forced
import org.neo4j.cypher.internal.rewriting.rewriters.IfNoParameter
import org.neo4j.cypher.internal.rewriting.rewriters.LiteralExtractionStrategy
import org.neo4j.cypher.internal.rewriting.rewriters.Never
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo

trait FrontEndCompilationPhases {

  val defaultSemanticFeatures: Seq[SemanticFeature.MultipleDatabases.type] = Seq(
    MultipleDatabases
  )

  def enabledSemanticFeatures(extra: Set[String]): Seq[SemanticFeature] =
    defaultSemanticFeatures ++ extra.map(SemanticFeature.fromString)

  case class ParsingConfig(
    extractLiterals: ExtractLiteral = ExtractLiteral.ALWAYS,
    /* TODO: This is not part of configuration - Move to BaseState */
    parameterTypeMapping: Map[String, ParameterTypeInfo] = Map.empty,
    semanticFeatures: Seq[SemanticFeature] = defaultSemanticFeatures,
    obfuscateLiterals: Boolean = false
  ) {

    def literalExtractionStrategy: LiteralExtractionStrategy = extractLiterals match {
      case ExtractLiteral.ALWAYS          => Forced
      case ExtractLiteral.NEVER           => Never
      case ExtractLiteral.IF_NO_PARAMETER => IfNoParameter
      case _ => throw new IllegalStateException(s"$extractLiterals is not a known strategy")
    }
  }

  def parsingBase(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] = {
    Parse andThen
      SyntaxDeprecationWarningsAndReplacements(Deprecations.syntacticallyDeprecatedFeatures) andThen
      PreparatoryRewriting andThen
      If((_: BaseState) => config.obfuscateLiterals)(
        extractSensitiveLiterals
      ) andThen
      SemanticAnalysis(warn = true, config.semanticFeatures: _*) andThen
      SemanticTypeCheck andThen
      SyntaxDeprecationWarningsAndReplacements(Deprecations.semanticallyDeprecatedFeatures) andThen
      IsolateSubqueriesInMutatingPatterns andThen
      SemanticAnalysis(warn = false, config.semanticFeatures: _*)
  }

  // Phase 1
  def parsing(
    config: ParsingConfig,
    resolver: Option[ProcedureSignatureResolver] = None
  ): Transformer[BaseContext, BaseState, BaseState] = {
    parsingBase(config) andThen
      AstRewriting(parameterTypeMapping = config.parameterTypeMapping) andThen
      LiteralExtraction(config.literalExtractionStrategy) andThen
      /*
       * With query router we log the query early and therefore need to resolve
       * procedure calls early in order to obfuscate sensitive procedure params
       * in the query log.
       */
      If((_: BaseState) => resolver.isDefined)(TryRewriteProcedureCalls(resolver.orNull)) andThen
      ObfuscationMetadataCollection
  }

  // Phase 1 (Fabric)
  def fabricParsing(
    config: ParsingConfig,
    resolver: ProcedureSignatureResolver
  ): Transformer[BaseContext, BaseState, BaseState] = {
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
      SemanticAnalysis(warn = false, config.semanticFeatures: _*)
  }
}

object FrontEndCompilationPhases extends FrontEndCompilationPhases
