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

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.expressions.PatternPart.SelectiveSelector
import org.neo4j.cypher.internal.expressions.ShortestPathsPatternPart
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.METADATA_COLLECTION
import org.neo4j.cypher.internal.frontend.phases.IsolateSubqueriesInMutatingPatterns.SubqueriesInMutatingPatternsIsolated
import org.neo4j.cypher.internal.frontend.phases.factories.ParsePipelineTransformerFactory
import org.neo4j.cypher.internal.rewriting.conditions.SensitiveLiteralsExtracted
import org.neo4j.cypher.internal.rewriting.rewriters.LiteralExtractionStrategy
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Condition
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo

/**
 * Collects usage statistics about several syntactical CYPHER features
 * and reports them as a metric.
 */
case object CollectSyntaxUsageMetrics
    extends VisitorPhase[BaseContext, BaseState]
    with StepSequencer.Step
    with ParsePipelineTransformerFactory
    with DefaultPostCondition {

  override def visit(state: BaseState, context: BaseContext): Unit = {
    def increaseMetric(key: SyntaxUsageMetricKey): Unit = {
      context.internalSyntaxUsageStats.incrementSyntaxUsageCount(key)
    }

    state.statement().folder.treeForeach {
      case _: SelectiveSelector =>
        increaseMetric(SyntaxUsageMetricKey.GPM_SHORTEST)
      case _: ShortestPathsPatternPart =>
        increaseMetric(SyntaxUsageMetricKey.LEGACY_SHORTEST)
    }
  }

  override def postConditions: Set[Condition] = super[DefaultPostCondition].postConditions

  override def preConditions: Set[Condition] = Set(
    BaseContains[Statement](),
    // No rewriting should have happened. These are all postConditions from rewriting steps in
    // FrontEndCompilationPhases.parsingBase
    !DeprecatedSyntaxReplaced,
    !DeprecatedSemanticsReplaced,
    !SensitiveLiteralsExtracted,
    !SubqueriesInMutatingPatternsIsolated
  ) ++ PreparatoryRewriting.postConditions.map(!_)

  override def invalidatedConditions: Set[Condition] = Set.empty

  override def getTransformer(
    literalExtractionStrategy: LiteralExtractionStrategy,
    parameterTypeMapping: Map[String, ParameterTypeInfo],
    semanticFeatures: Seq[SemanticFeature],
    obfuscateLiterals: Boolean = false
  ): Transformer[BaseContext, BaseState, BaseState] = this

  override def phase = METADATA_COLLECTION
}
