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

import org.neo4j.cypher.internal.ast.ProjectionClause
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticErrorDef
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
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.UpToDateScopes
import org.neo4j.cypher.internal.rewriting.conditions.FunctionInvocationsResolved
import org.neo4j.cypher.internal.rewriting.rewriters.computeDependenciesForExpressions.ExpressionsHaveComputedDependencies
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Condition

/**
 * Verify aggregation expressions and make sure there are no ambiguous grouping keys.
 */

case object AggregationsChecked extends Condition

case object AmbiguousAggregationAnalysis extends VisitorPhase[BaseContext, BaseState]
    with StepSequencer.Step
    with ParsePipelineTransformerFactory {

  def findErrors(from: BaseState): Seq[SemanticErrorDef] = {
    val errors = from.statement().folder.treeFold(Set.empty[SemanticErrorDef]) {
      case projectionClause: ProjectionClause if projectionClause.isAggregating =>
        acc => TraverseChildren(acc ++ AggregationChecker.checkAggregatingClause(from, projectionClause))
      case projectionClause: ProjectionClause =>
        acc => TraverseChildren(acc ++ AggregationChecker.checkNonAggregatingClause(projectionClause))
    }
    errors.toSeq
  }

  // TODO `skip42I18` is a transitional flag that will be used until a future PR
  //  with notifications and changed rules for aggregations is merged
  def collectErrors(from: BaseState, skip42I18: Boolean): Seq[SemanticErrorDef] = {
    val errors = findErrors(from)
    if (skip42I18)
      errors.filter(_.gqlStatusObject.cause().get.gqlStatus() != "42I18")
    else errors
  }

  override def visit(from: BaseState, context: BaseContext): Unit = {
    context.errorHandler(collectErrors(from, skip42I18 = false))
  }

  override def phase: CompilationPhaseTracer.CompilationPhase = SEMANTIC_CHECK

  override def preConditions: Set[StepSequencer.Condition] = Set(
    BaseContains[Statement](),
    // This is needed, because ExpressionWithComputedDependencies will otherwise have an incorrect
    // `.introducedVariables`, which is called in this Phase.
    ExpressionsHaveComputedDependencies,
    FunctionInvocationsResolved,
    UpToDateScopes
  )

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def getTransformer(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] = this

  override def postConditions: Set[StepSequencer.Condition] = Set(AggregationsChecked)
}
