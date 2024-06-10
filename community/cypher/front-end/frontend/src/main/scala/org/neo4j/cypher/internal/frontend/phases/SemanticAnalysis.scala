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
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckContext
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult
import org.neo4j.cypher.internal.ast.semantics.SemanticChecker
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.SEMANTIC_CHECK
import org.neo4j.cypher.internal.frontend.phases.PreparatoryRewriting.SemanticAnalysisPossible
import org.neo4j.cypher.internal.frontend.phases.factories.ParsePipelineTransformerFactory
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.conditions.containsNoNodesOfType
import org.neo4j.cypher.internal.rewriting.rewriters.LiteralExtractionStrategy
import org.neo4j.cypher.internal.rewriting.rewriters.computeDependenciesForExpressions
import org.neo4j.cypher.internal.rewriting.rewriters.computeDependenciesForExpressions.ExpressionsHaveComputedDependencies
import org.neo4j.cypher.internal.util.ErrorMessageProvider
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo
import org.neo4j.kernel.database.DatabaseReference

/**
 * Do variable binding, typing, type checking and other semantic checks.
 */
case class SemanticAnalysis(warn: Boolean, features: SemanticFeature*)
    extends Phase[BaseContext, BaseState, BaseState] {

  override def process(from: BaseState, context: BaseContext): BaseState = {
    val startState =
      SemanticState.clean
        .withFeatures(features: _*)
        .semanticCheckHasRunOnce(from.maybeSemanticTable.isDefined)

    val checkContext = new SemanticCheckContext {
      override def errorMessageProvider: ErrorMessageProvider = context.errorMessageProvider

      override def sessionDatabaseReference: DatabaseReference = context.sessionDatabase
    }

    val SemanticCheckResult(state, errors) = SemanticChecker.check(from.statement(), startState, checkContext)
    if (warn) state.notifications.foreach(context.notificationLogger.log)

    context.errorHandler(errors)

    val table = from.maybeSemanticTable match {
      case Some(existingTable) =>
        // We might already have a SemanticTable from a previous run, and that might already have tokens.
        // We don't want to lose these
        existingTable.copy(types = state.typeTable, recordedScopes = state.recordedScopes.view.mapValues(_.scope).toMap)
      case None =>
        SemanticTable(types = state.typeTable, recordedScopes = state.recordedScopes.view.mapValues(_.scope).toMap)
    }

    val rewrittenStatement =
      if (errors.isEmpty) {
        // Some expressions record some semantic information in themselves.
        // This is done by the computeDependenciesForExpressions rewriter.
        // We need to apply it after each pass of SemanticAnalysis.
        from.statement().endoRewrite(computeDependenciesForExpressions(state))
      } else {
        // If we have errors we should rather avoid running computeDependenciesForExpressions, since the state might be incomplete.
        from.statement()
      }
    from
      .withStatement(rewrittenStatement)
      .withSemanticState(state)
      .withSemanticTable(table)
  }

  override def phase: CompilationPhaseTracer.CompilationPhase = SEMANTIC_CHECK

  override def postConditions: Set[StepSequencer.Condition] = SemanticAnalysis.postConditions
}

case object SemanticAnalysis extends StepSequencer.Step with ParsePipelineTransformerFactory
    with PlanPipelineTransformerFactory {

  override def preConditions: Set[StepSequencer.Condition] = Set(
    BaseContains[Statement](),
    SemanticAnalysisPossible
  )

  override def postConditions: Set[StepSequencer.Condition] = Set(
    BaseContains[SemanticState](),
    StatementCondition(containsNoNodesOfType[UnaliasedReturnItem]()),
    BaseContains[SemanticTable](),
    ExpressionsHaveComputedDependencies
  ) ++ SemanticInfoAvailable

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  /**
   * Transformer for the parse pipeline
   */
  override def getTransformer(
    literalExtractionStrategy: LiteralExtractionStrategy,
    parameterTypeMapping: Map[String, ParameterTypeInfo],
    semanticFeatures: Seq[SemanticFeature],
    obfuscateLiterals: Boolean
  ): Transformer[BaseContext, BaseState, BaseState] = SemanticAnalysis(warn = true, semanticFeatures: _*)

  /**
   * Transformer for the plan pipeline
   */
  override def getTransformer(
    pushdownPropertyReads: Boolean,
    semanticFeatures: Seq[SemanticFeature]
  ): Transformer[BaseContext, BaseState, BaseState] = SemanticAnalysis(warn = false, semanticFeatures: _*)
}
