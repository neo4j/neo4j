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
package org.neo4j.cypher.internal.frontend.scoping.inspection_tool

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.ScopeQueries
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.PipelineDebugInfo
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.Parse
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.ScopeSurveyor
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.VariableChecker
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.ErrorMessageProvider
import org.neo4j.cypher.internal.util.NotImplementedErrorMessageProvider

object Inspector {

  private val version = CypherVersion.Cypher25
  private val messageProvider: ErrorMessageProvider = NotImplementedErrorMessageProvider

  def apply(query: String): InspectionResult = {
    val context =
      new InspectionContext(version, semanticFeatures = Seq(ScopeQueries)) {
        override def errorMessageProvider: ErrorMessageProvider = messageProvider
      }
    val debugLogger = new VariableCheckerInMemoryDebugLogger()
    val transformer = Parse andThen ScopeSurveyor andThen VariableChecker
    val initialState = InitialState(
      query,
      InspectionPlannerName,
      new AnonymousVariableNameGenerator,
      maybeDebugInfo = Some(PipelineDebugInfo(Some(debugLogger)))
    )

    try {
      val state = transformer.transform(initialState, context)
      val workingScopeOpt = state.maybeScopeState.map(_.workingScope)
      val errors = context.errors.map(_.msg)
      val logs = state.maybeDebugInfo
        .flatMap(_.maybeVariableCheckerDebugLogger)
        .collect { case logger: VariableCheckerInMemoryDebugLogger => logger.logs }
        .getOrElse(Seq.empty)
      workingScopeOpt match {
        case Some(workingScope) =>
          InspectionSuccess(ViewModel.InspectionViewModelBuilder.build(workingScope, errors, logs))
        case None =>
          InspectionFailure(
            "No WorkingScope was produced for the query.",
            errors match {
              case Seq() => Seq("The scope state was empty after Parse and ScopeSurveyor.")
              case xs    => xs
            }
          )
      }
    } catch {
      case e: Throwable =>
        InspectionFailure(
          Option(e.getMessage).getOrElse(e.getClass.getSimpleName),
          e.getStackTrace.iterator.map(_.toString).take(25).toSeq
        )
    }
  }
}
