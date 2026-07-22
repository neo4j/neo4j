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
package org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping

import org.neo4j.cypher.internal.ast.FullSubqueryExpression
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.ast.semantics.scoping.SurveyorNameGenerator
import org.neo4j.cypher.internal.expressions.ExpressionWithComputedDependencies
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.frontend.phases.BaseContains
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.SEMANTIC_CHECK
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.ParsePipelineTransformerFactory
import org.neo4j.cypher.internal.frontend.phases.factories.ParsingConfig
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerConfig
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.rewriting.conditions.NoReferenceEqualityAmongVariables
import org.neo4j.cypher.internal.rewriting.rewriters.computeDependenciesForExpressions.ExpressionsHaveComputedDependencies
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.topDown

/**
 * Populates the `computedIntroducedVariables` and `computedScopeDependencies` of every
 * [[ExpressionWithComputedDependencies]] in the statement from the WorkingScope survey ([[ScopeSurveyor]]).
 *
 * Requires [[UpToDateScopes]] so the survey on the [[BaseState]] reflects the current tree; any rewriter
 * that mutates the tree before this phase must invalidate [[UpToDateScopes]] so the survey is rebuilt.
 */
case object ComputeExpressionDependencies extends Phase[BaseContext, BaseState, BaseState]
    with StepSequencer.Step
    with ParsePipelineTransformerFactory
    with PlanPipelineTransformerFactory {

  override def process(from: BaseState, context: BaseContext): BaseState = {
    val scopeState = from.scopeState()

    val rewritten: Statement = from.statement().endoRewrite(topDown(Rewriter.lift {
      case ecd: ExpressionWithComputedDependencies =>
        ecd match {
          case _: FullSubqueryExpression | _: PatternExpression | _: PatternComprehension =>
            val scope = scopeState.scopeOfOpt(ecd).getOrElse(
              throw new IllegalStateException(s"No working scope was recorded for expression: $ecd")
            )
            val introduced = (scope.collectAllDeclarations ++ scope.collectAllReturnAliases).iterator
              .map(_.value)
              .filter(v => SurveyorNameGenerator.named(v.name))
              .filterNot(v => scope.incoming.allSymbols.exists(_.name == v.name))
              .toSet
            val dependencies = scope.referenced.getVariables.toSet
            ecd
              .withComputedIntroducedVariables(introduced)
              .withComputedScopeDependencies(dependencies)
          case other =>
            throw new IllegalStateException(s"Unexpected expression during dependency computation: $other")
        }
    }))

    from.withStatement(rewritten)
  }

  override def phase: CompilationPhase = SEMANTIC_CHECK

  override def preConditions: Set[StepSequencer.Condition] =
    Set(BaseContains[SemanticTable](), BaseContains[Statement](), NoReferenceEqualityAmongVariables, UpToDateScopes)

  override def postConditions: Set[StepSequencer.Condition] = Set(ExpressionsHaveComputedDependencies)

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def getTransformer(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] = this

  override def getTransformer(planPipelineConfig: PlanPipelineTransformerConfig)
    : Transformer[BaseContext, BaseState, BaseState] = this
}
