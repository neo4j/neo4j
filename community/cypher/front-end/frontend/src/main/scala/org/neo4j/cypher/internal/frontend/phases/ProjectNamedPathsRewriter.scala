/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.conditions.containsNamedPathOnlyForShortestPath
import org.neo4j.cypher.internal.rewriting.conditions.containsNoReturnAll
import org.neo4j.cypher.internal.rewriting.rewriters.NoNamedPathsInPatternComprehensions
import org.neo4j.cypher.internal.rewriting.rewriters.projectNamedPaths
import org.neo4j.cypher.internal.util.StepSequencer

/**
 * Expands `WITH *` or `RETURN *` by enumerating all columns instead of the `*`.
 */
case object ProjectNamedPathsRewriter extends Phase[BaseContext, BaseState, BaseState] with StepSequencer.Step with PlanPipelineTransformerFactory {

  def phase: CompilationPhaseTracer.CompilationPhase = AST_REWRITE

  def process(from: BaseState, context: BaseContext): BaseState =
    from.withStatement(from.statement().endoRewrite(projectNamedPaths))

  override def preConditions: Set[StepSequencer.Condition] = Set(
    StatementCondition(containsNoReturnAll),
    NoNamedPathsInPatternComprehensions,
    AmbiguousNamesDisambiguated
  )

  override def postConditions: Set[StepSequencer.Condition] = Set(
    StatementCondition(containsNamedPathOnlyForShortestPath)
  )

  override def invalidatedConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable

  override def getTransformer(pushdownPropertyReads: Boolean,
                              semanticFeatures: Seq[SemanticFeature]): Transformer[_ <: BaseContext, _ <: BaseState, BaseState] = this
}
