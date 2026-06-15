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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.ScopeQueries
import org.neo4j.cypher.internal.ast.semantics.scoping.RegularContext
import org.neo4j.cypher.internal.ast.semantics.scoping.ScopeState
import org.neo4j.cypher.internal.ast.semantics.scoping.SurveyorNameGenerator
import org.neo4j.cypher.internal.ast.semantics.scoping.UnexpectedAstNodeScopingError
import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingScope
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.frontend.phases.BaseContains
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.ParsePipelineTransformerFactory
import org.neo4j.cypher.internal.frontend.phases.factories.ParsingConfig
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerConfig
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.StepSequencer

case object UpToDateScopes extends StepSequencer.Condition

/**
 * Produce a WorkingScope tree that makes it easy to check variable availability for the whole query
 */
case object ScopeSurveyor extends Phase[BaseContext, BaseState, BaseState]
    with StepSequencer.Step
    with ParsePipelineTransformerFactory
    with PlanPipelineTransformerFactory {

  override def process(from: BaseState, context: BaseContext): BaseState =
    if (from.maybeScopeState.isEmpty || (from.statement() ne from.scopeState().workingScope.astNode))
      from.withScopeState(runFromState(from, context))
    else from

  private def runFromState(from: BaseState, context: BaseContext): ScopeState =
    run(from.statement(), from.maybeScopeState, context.cypherVersion, context.semanticFeatures)

  def run(
    statement: Statement,
    maybeScopeState: Option[ScopeState],
    cypherVersion: CypherVersion,
    semanticFeatures: Seq[SemanticFeature]
  ): ScopeState = {
    val anonVarGen = SurveyorNameGenerator()

    val workingContextOfStatement = scope(
      statement,
      RegularContext.unit,
      PegContext(
        anonVarGen,
        cypherVersion,
        semanticFeatures.toSet,
        maybeScopeState.map(_.recordedScopes).getOrElse(ScopeState.emptyRecordedScopes)
      )
    )
    val recordedScopes = workingContextOfStatement.getRecordedScopes

    val explainScope =
      if (semanticFeatures.contains(ScopeQueries) && maybeScopeState.isEmpty)
        Some(workingContextOfStatement)
      else maybeScopeState.flatMap(x => x.explainScope)

    ScopeState(workingContextOfStatement, recordedScopes, explainScope)
  }

  def getTransformerWithoutCheck: Transformer[BaseContext, BaseState, BaseState] =

    new Transformer[BaseContext, BaseState, BaseState] {

      override def transform(from: BaseState, context: BaseContext): BaseState =
        from.withScopeState(runFromState(from, context))

      override def postConditions: Set[StepSequencer.Condition] = Set.empty

      override def name: String = "ScopeSurveyor without check statement"
    }

  override def getTransformer(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] = ScopeSurveyor

  override def getTransformer(planPipelineConfig: PlanPipelineTransformerConfig)
    : Transformer[BaseContext, BaseState, BaseState] = ScopeSurveyor

  override val phase = CompilationPhase.VARIABLE_SCOPING

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def preConditions: Set[StepSequencer.Condition] = Set(BaseContains[Statement]())

  override def postConditions: Set[StepSequencer.Condition] = Set(BaseContains[WorkingScope](), UpToDateScopes)

  def scope(
    astNode: ASTNode,
    incoming: RegularContext,
    ctx: PegContext
  ): WorkingScope = {
    implicit val c: PegContext = ctx
    astNode match {

      /**
       * Statement
       */
      case statement: Statement => pegStatement(statement, incoming)

      /**
       * Clause
       */
      case clause: Clause => pegClause(clause, incoming)

      /**
       * Expression
       */
      case expression: Expression => pegExpression(expression, incoming)
      case labelExpression: LabelExpression =>
        pegExpression(labelExpression, incoming)

      /**
       * Pattern
       */
      case pattern: Pattern         => pegPattern(pattern, incoming, foreachIterVar = None)
      case patternPart: PatternPart => pegPattern(patternPart, incoming, foreachIterVar = None)

      /**
       * To make match exhaustive
       */
      case _ => UnexpectedAstNodeScopingError(astNode, incoming)
    }
  }

}
