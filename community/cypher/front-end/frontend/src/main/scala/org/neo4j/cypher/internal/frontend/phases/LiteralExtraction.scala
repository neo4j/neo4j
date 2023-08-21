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

import org.neo4j.cypher.internal.ast.AdministrationCommand
import org.neo4j.cypher.internal.ast.SchemaCommand
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.expressions.AutoExtractedParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.neo4j.cypher.internal.frontend.phases.factories.ParsePipelineTransformerFactory
import org.neo4j.cypher.internal.rewriting.conditions.LiteralsExtracted
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.rewriters.LiteralExtractionStrategy
import org.neo4j.cypher.internal.rewriting.rewriters.literalReplacement
import org.neo4j.cypher.internal.rewriting.rewriters.sensitiveLiteralReplacement
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo

/**
 * Replace literals with parameters.
 */
case class LiteralExtraction(literalExtraction: LiteralExtractionStrategy)
    extends Phase[BaseContext, BaseState, BaseState] {

  override def process(in: BaseState, context: BaseContext): BaseState = {
    val statement = in.statement()
    val (extractParameters, extractedParameters) = statement match {
      case _: AdministrationCommand => sensitiveLiteralReplacement(statement)
      case _: SchemaCommand         => Rewriter.noop -> Map.empty[AutoExtractedParameter, Expression]
      case _                        => literalReplacement(statement, literalExtraction)
    }
    val rewrittenStatement = statement.endoRewrite(extractParameters)
    in.withStatement(rewrittenStatement).withParams(extractedParameters)
  }

  override def phase = AST_REWRITE

  override def postConditions: Set[StepSequencer.Condition] = LiteralExtraction.postConditions
}

case object LiteralExtraction extends StepSequencer.Step with ParsePipelineTransformerFactory {

  override def preConditions: Set[StepSequencer.Condition] = Set(
    BaseContains[Statement]
  )

  override def postConditions: Set[StepSequencer.Condition] = Set(LiteralsExtracted)

  override def invalidatedConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable

  override def getTransformer(
    literalExtractionStrategy: LiteralExtractionStrategy,
    parameterTypeMapping: Map[String, ParameterTypeInfo],
    semanticFeatures: Seq[SemanticFeature],
    obfuscateLiterals: Boolean
  ): Transformer[BaseContext, BaseState, BaseState] = LiteralExtraction(literalExtractionStrategy)
}
