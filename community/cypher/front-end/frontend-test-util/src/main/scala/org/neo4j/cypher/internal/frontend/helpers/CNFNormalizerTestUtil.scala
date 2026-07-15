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
package org.neo4j.cypher.internal.frontend.helpers

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.frontend.phases.BaseContains
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.ObfuscationMetadataCollected
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerConfig
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.LocalFunctionsResolved
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.PreparatoryRewriting.SemanticAnalysisPossible
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.SemanticAnalysis
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.ShadowedFunctionsUnresolved
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.CNFNormalizer
import org.neo4j.cypher.internal.frontend.phases.transitiveEqualities
import org.neo4j.cypher.internal.rewriting.conditions.CallInvocationsResolved
import org.neo4j.cypher.internal.rewriting.conditions.FunctionInvocationsResolved
import org.neo4j.cypher.internal.rewriting.rewriters.astRewriters.NormalizePredicates
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Condition

object CNFNormalizerTestUtil {

  def transformerConfig(): PlanPipelineTransformerConfig =
    PlanPipelineTransformerConfig(
      pushdownPropertyReads = false,
      allowSubqueryDuplicationInCnf = false
    )

  case class SemanticWrapper()
      extends Transformer[BaseContext, BaseState, BaseState]
      with StepSequencer.Step
      with PlanPipelineTransformerFactory {

    private val transformer = SemanticAnalysis.getTransformer(transformerConfig())

    override def preConditions: Set[Condition] = SemanticAnalysis.preConditions

    override def postConditions: Set[Condition] = SemanticAnalysis.postConditions

    override def invalidatedConditions: Set[Condition] = SemanticAnalysis.invalidatedConditions

    override def getTransformer(planPipelineConfig: PlanPipelineTransformerConfig): SemanticWrapper = this

    override def transform(from: BaseState, context: BaseContext): BaseState = transformer.transform(from, context)

    override def name: String = transformer.name
  }

  def getTransformer(): Transformer[BaseContext, BaseState, BaseState] = {
    val orderedSteps: Seq[Transformer[BaseContext, BaseState, BaseState]] =
      StepSequencer[PlanPipelineTransformerFactory with StepSequencer.Step]()
        .orderSteps(
          Set(
            transitiveEqualities,
            SemanticWrapper()
          ) ++ CNFNormalizer.steps,
          initialConditions = Set(
            BaseContains[Statement](),
            ShadowedFunctionsUnresolved,
            LocalFunctionsResolved,
            CallInvocationsResolved,
            FunctionInvocationsResolved,
            SemanticAnalysisPossible,
            ObfuscationMetadataCollected,
            NormalizePredicates.completed
          )
        )
        .steps
        .map(_.getTransformer(transformerConfig()))
        .map(_.asInstanceOf[Transformer[BaseContext, BaseState, BaseState]])

    orderedSteps.reduceLeft[Transformer[BaseContext, BaseState, BaseState]]((t1, t2) => t1 andThen t2)
  }
}
