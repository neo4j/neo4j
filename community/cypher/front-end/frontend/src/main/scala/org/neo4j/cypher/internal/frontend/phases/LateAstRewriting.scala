/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.rewriting.ListStepAccumulator
import org.neo4j.cypher.internal.rewriting.rewriters.collapseMultipleInPredicates
import org.neo4j.cypher.internal.rewriting.rewriters.projectNamedPaths
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.AccumulatedSteps
import org.neo4j.cypher.internal.util.inSequence

case object LateAstRewriting extends StatementRewriter with StepSequencer.Step with PlanPipelineTransformerFactory {

  private val steps: Set[StepSequencer.Step with Rewriter] = Set(
    collapseMultipleInPredicates,
    projectNamedPaths
  )

  private val AccumulatedSteps(orderedSteps, accumulatedConditions) = StepSequencer(ListStepAccumulator[StepSequencer.Step with Rewriter]()).orderSteps(steps, initialConditions = steps.flatMap(_.preConditions))
  private val rewriter = inSequence(orderedSteps: _*)

  override def instance(context: BaseContext): Rewriter = rewriter

  override def description: String = "normalize the AST"

  override def preConditions: Set[StepSequencer.Condition] = steps.flatMap(_.preConditions).map(StatementCondition.wrap)

  override def postConditions: Set[StepSequencer.Condition] = steps.flatMap(_.postConditions).intersect(accumulatedConditions).map(StatementCondition.wrap)

  override def invalidatedConditions: Set[StepSequencer.Condition] = (steps.flatMap(_.invalidatedConditions) -- accumulatedConditions).map(StatementCondition.wrap)

  override def getTransformer(pushdownPropertyReads: Boolean,
                              semanticFeatures: Seq[SemanticFeature]): Transformer[BaseContext, BaseState, BaseState] = this
}
