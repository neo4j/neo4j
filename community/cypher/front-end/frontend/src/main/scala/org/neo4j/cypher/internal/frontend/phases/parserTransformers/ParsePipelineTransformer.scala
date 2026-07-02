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
package org.neo4j.cypher.internal.frontend.phases.parserTransformers

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.frontend.phases.BaseContains
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.Chainer
import org.neo4j.cypher.internal.frontend.phases.ObfuscationMetadataCollection
import org.neo4j.cypher.internal.frontend.phases.ProcedureAndFunctionDeprecationWarnings
import org.neo4j.cypher.internal.frontend.phases.ProcedureWarnings
import org.neo4j.cypher.internal.frontend.phases.ResolveCallables
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.ParsePipelineTransformerFactory
import org.neo4j.cypher.internal.frontend.phases.factories.ParsingConfig
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.ScopeSurveyor
import org.neo4j.cypher.internal.rewriting.Deprecations
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.AccumulatedSteps

case object ParsePipelineTransformer extends StepSequencer.Step {

  /**
   * Steps that run before obfuscation metadata is collected. These produce the AST shape
   * that ObfuscationMetadataCollection depends on. Failures here are *not* logged with
   * the query text in the debug log, because the obfuscator is not yet wired onto the ExecutingQuery.
   */
  private val preObfuscatorSet: Set[StepSequencer.Step & ParsePipelineTransformerFactory] = Set(
    CollectSyntaxUsageMetrics,
    PreparatoryRewriting,
    RemoveDuplicateUseClauses,
    WrapAndExpandProcedureCall,
    ScopeSurveyor,
    SyntaxDeprecationWarningsAndReplacements(Deprecations.SyntacticallyDeprecatedFeatures),
    ResolveLocalFunctions,
    ResolveLocalProceduresStep1,
    UnresolveShadowedFunctions,
    ProcedureRelocator,
    ResolveCallables,
    ResolveLocalProceduresStep2,
    ExtractLocalDefinitions
  )

  /**
   * Semantic analysis, semantics-dependent rewriting, and obfuscation metadata collection. These
   * run before the obfuscator is wired onto the ExecutingQuery (it is wired only once this whole
   * group has completed), so a failure here — e.g. a semantic error — leaves the query text
   * withheld from the debug log under obfuscation. ObfuscationMetadataCollection runs here so the
   * collected metadata reflects the fully-resolved, semantically-analysed statement.
   */
  private val postObfuscatorSet: Set[StepSequencer.Step & ParsePipelineTransformerFactory] = Set(
    ScopeSurveyor,
    SemanticAnalysis,
    SemanticTypeCheck,
    SyntaxDeprecationWarningsAndReplacements(Deprecations.SemanticallyDeprecatedFeatures),
    AggregationAnalysis,
    ProcedureAndFunctionDeprecationWarnings,
    ProcedureWarnings,
    IsolateSubqueriesInMutatingPatterns,
    ReplacePatternComprehensionWithCollectSubqueryRewriter,
    ExpandClauses,
    ExpandSubclauses,
    ObfuscationMetadataCollection
  )

  val AccumulatedSteps(preObfuscatorSteps, preObfuscatorPostConditions) =
    StepSequencer[StepSequencer.Step & ParsePipelineTransformerFactory]().orderSteps(
      preObfuscatorSet,
      initialConditions = Set(BaseContains[Statement]())
    )

  private val postObfuscatorEstablishes: Set[StepSequencer.Condition] =
    postObfuscatorSet.flatMap(_.postConditions)

  val AccumulatedSteps(postObfuscatorSteps, postObfuscatorPostConditions) =
    StepSequencer[StepSequencer.Step & ParsePipelineTransformerFactory]().orderSteps(
      postObfuscatorSet,
      initialConditions = preObfuscatorPostConditions -- postObfuscatorEstablishes
    )

  override def preConditions: Set[StepSequencer.Condition] = Set(BaseContains[Statement]())

  override def postConditions: Set[StepSequencer.Condition] = postObfuscatorPostConditions

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  /** Steps that must run before ExecutingQuery.onObfuscatorReady can be invoked. */
  def getPreObfuscatorTransformer(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] =
    Chainer.chainTransformers(preObfuscatorSteps.map(_.getCheckedTransformer(config)))
      .asInstanceOf[Transformer[BaseContext, BaseState, BaseState]]

  /** Steps that run after the obfuscator is wired up. Their throws carry query text into the debug log. */
  def getPostObfuscatorTransformer(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] =
    Chainer.chainTransformers(postObfuscatorSteps.map(_.getCheckedTransformer(config)))
      .asInstanceOf[Transformer[BaseContext, BaseState, BaseState]]

  /** Convenience for callers that don't need to interpose at the obfuscator boundary. */
  def getTransformer(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] =
    getPreObfuscatorTransformer(config) andThen getPostObfuscatorTransformer(config)

}
