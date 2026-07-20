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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.ProjectionClause
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.scoping.ExpressionScope
import org.neo4j.cypher.internal.ast.semantics.scoping.ProjectionExpressionContext
import org.neo4j.cypher.internal.ast.semantics.scoping.SubclausePart
import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingScope
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.frontend.phases.BaseContains
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.SEMANTIC_CHECK
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.VisitorPhase
import org.neo4j.cypher.internal.frontend.phases.factories.ParsePipelineTransformerFactory
import org.neo4j.cypher.internal.frontend.phases.factories.ParsingConfig
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.AggregationChecker
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.SubclauseExpressionClassifier
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.SubclauseExpressionClassifier.Classification
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.UpToDateScopes
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.VariableChecker
import org.neo4j.cypher.internal.rewriting.conditions.FunctionInvocationsResolved
import org.neo4j.cypher.internal.rewriting.rewriters.computeDependenciesForExpressions.ExpressionsHaveComputedDependencies
import org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters.ReturnItemsAreAliased
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Condition

case object AggregationsChecked extends Condition

/**
 * Runs semantic checks related to aggregating functions and expressions.
 * Errors controlled by AggregationAnalysis:
 * - 42N44 - inaccessible variable
 * - 42I18 - reference to non-grouping sub-expression
 * - 42I24 - invalid use of aggregate function
 * - 42I79 - invalid reference in subclause expression
 */
case object AggregationAnalysis extends VisitorPhase[BaseContext, BaseState]
    with StepSequencer.Step
    with ParsePipelineTransformerFactory {

  override def visit(from: BaseState, context: BaseContext): Unit = {
    // Cypher 5 keeps the legacy behaviour and does not classify subclause expressions (CIP-248).
    val classifySubclauses = context.cypherVersion != CypherVersion.Cypher5

    val aggregationErrors = from.scopeState().recordedScopes.collect {
      case (Ref(clause: ProjectionClause), scope) =>
        val acErrors = AggregationChecker.checkClause(from, clause, scope, context.cypherVersion)
        val scErrors =
          if (classifySubclauses) classifyChildren(from, scope).collect {
            case classification: Classification =>
              classification.notifications.foreach(context.notificationLogger.log)
              classification.errors
          }.flatten
          else Seq.empty
        acErrors ++ scErrors
    }.flatten.toSeq

    context.errorHandler(aggregationErrors.sortBy(e => VariableChecker.getErrorOrder(e)))

  }

  /**
   * Per CIP-248: classify each subclause expression (sort key, WHERE predicate) of a projection
   * clause via the shared [[SubclauseExpressionClassifier]]. Runs only for Cypher 25+.
   */
  private def classifyChildren(
    from: BaseState,
    scope: WorkingScope
  ): Seq[SubclauseExpressionClassifier.Classification] =
    scope.children.collect {
      case es @ ExpressionScope(x: Expression, ProjectionExpressionContext(_, _, _, spec, _: SubclausePart), _, _, _) =>
        SubclauseExpressionClassifier(x, es, scope, spec, from.scopeState())
    }

  override def phase: CompilationPhaseTracer.CompilationPhase = SEMANTIC_CHECK

  override def preConditions: Set[StepSequencer.Condition] = Set(
    BaseContains[Statement](),
    ExpressionsHaveComputedDependencies,
    FunctionInvocationsResolved,
    UpToDateScopes,
    ReturnItemsAreAliased
  )

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def getTransformer(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] = this

  override def postConditions: Set[StepSequencer.Condition] = Set(AggregationsChecked)
}
