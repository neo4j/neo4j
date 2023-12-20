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
package org.neo4j.cypher.internal.frontend

import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.frontend.helpers.ErrorCollectingContext
import org.neo4j.cypher.internal.frontend.helpers.NoPlannerName
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.OpenCypherJavaCCParsing
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.frontend.phases.PreparatoryRewriting
import org.neo4j.cypher.internal.frontend.phases.SemanticAnalysis
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.rewriting.rewriters.projectNamedPaths
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.ErrorMessageProvider
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.NotImplementedErrorMessageProvider
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName

case class SemanticAnalysisResult(context: ErrorCollectingContext, state: BaseState) {
  def errors: Seq[SemanticErrorDef] = context.errors

  def errorMessages: Seq[String] = errors.map(_.msg)

  def semanticTable: SemanticTable = state.semanticTable()
}

trait SemanticAnalysisTestSuite extends CypherFunSuite {

  type Pipeline = Transformer[BaseContext, BaseState, BaseState]

  def messageProvider: ErrorMessageProvider = NotImplementedErrorMessageProvider

  def runSemanticAnalysisWithPipelineAndState(pipeline: Pipeline, initialState: BaseState): SemanticAnalysisResult = {
    val context = new ErrorCollectingContext() {
      override def errorMessageProvider: ErrorMessageProvider = messageProvider
    }
    val state = pipeline.transform(initialState, context)
    SemanticAnalysisResult(context, state)
  }

  def initialStateWithQuery(query: String): InitialState =
    InitialState(query, None, NoPlannerName, new AnonymousVariableNameGenerator)

  def runSemanticAnalysisWithPipeline(pipeline: Pipeline, query: String): SemanticAnalysisResult =
    runSemanticAnalysisWithPipelineAndState(pipeline, initialStateWithQuery(query))

  // This test invokes SemanticAnalysis twice because that's what the production pipeline does
  def pipelineWithSemanticFeatures(semanticFeatures: SemanticFeature*): Pipeline =
    OpenCypherJavaCCParsing andThen
      PreparatoryRewriting andThen
      SemanticAnalysis(warn = true, semanticFeatures: _*) andThen
      SemanticAnalysis(warn = false, semanticFeatures: _*)

  def runSemanticAnalysisWithSemanticFeatures(
    semanticFeatures: Seq[SemanticFeature],
    query: String
  ): SemanticAnalysisResult =
    runSemanticAnalysisWithPipeline(pipelineWithSemanticFeatures(semanticFeatures: _*), query)

  def runSemanticAnalysis(query: String): SemanticAnalysisResult =
    runSemanticAnalysisWithSemanticFeatures(Seq.empty, query)

  // ------- Helpers ------------------------------

  def expectNoErrorsFrom(
    query: String,
    pipeline: Transformer[BaseContext, BaseState, BaseState] = pipelineWithSemanticFeatures()
  ): Unit =
    runSemanticAnalysisWithPipeline(pipeline, query).errors shouldBe empty

  def expectErrorsFrom(
    query: String,
    expectedErrors: Set[SemanticError],
    pipeline: Transformer[BaseContext, BaseState, BaseState] = pipelineWithSemanticFeatures()
  ): Unit =
    runSemanticAnalysisWithPipeline(pipeline, query).errors.toSet shouldEqual expectedErrors

  def expectErrorMessagesFrom(
    query: String,
    expectedErrors: Set[String],
    pipeline: Transformer[BaseContext, BaseState, BaseState] = pipelineWithSemanticFeatures()
  ): Unit =
    runSemanticAnalysisWithPipeline(pipeline, query).errorMessages.toSet shouldEqual expectedErrors

  def expectNotificationsFrom(
    query: String,
    expectedNotifications: Set[InternalNotification],
    pipeline: Transformer[BaseContext, BaseState, BaseState] = pipelineWithSemanticFeatures()
  ): Unit = {
    val normalisedQuery = normalizeNewLines(query)
    val result = runSemanticAnalysisWithPipeline(pipeline, normalisedQuery)
    result.state.semantics().notifications shouldEqual expectedNotifications
    result.errors shouldBe empty
  }

  final case object ProjectNamedPathsPhase extends Phase[BaseContext, BaseState, BaseState] {
    override def phase: CompilationPhaseTracer.CompilationPhase = AST_REWRITE

    override def process(from: BaseState, context: BaseContext): BaseState = {
      from.withStatement(from.statement().endoRewrite(projectNamedPaths))
    }
    override def postConditions: Set[StepSequencer.Condition] = Set.empty
  }

}

trait SemanticAnalysisTestSuiteWithDefaultQuery extends SemanticAnalysisTestSuite {

  def defaultQuery: String

  def runSemanticAnalysis(): SemanticAnalysisResult = runSemanticAnalysis(defaultQuery)

  def runSemanticAnalysisWithSemanticFeatures(semanticFeatures: SemanticFeature*): SemanticAnalysisResult =
    runSemanticAnalysisWithSemanticFeatures(semanticFeatures, defaultQuery)
}

trait NameBasedSemanticAnalysisTestSuite extends SemanticAnalysisTestSuiteWithDefaultQuery with TestName {

  override def defaultQuery: String = testName

}

trait ErrorMessageProviderAdapter extends ErrorMessageProvider {

  override def createMissingPropertyLabelHintError(
    operatorDescription: String,
    hintStringification: String,
    missingThingDescription: String,
    foundThingsDescription: String,
    entityDescription: String,
    entityName: String,
    additionalInfo: String
  ): String = ???

  override def createSelfReferenceError(name: String): String = ???

  override def createSelfReferenceError(name: String, variableType: String): String = ???

  override def createUseClauseUnsupportedError(): String = ???

  override def createDynamicGraphReferenceUnsupportedError(graphName: String): String = ???

  override def createMultipleGraphReferencesError(graphName: String, transactionalDefault: Boolean = false): String =
    ???
}
