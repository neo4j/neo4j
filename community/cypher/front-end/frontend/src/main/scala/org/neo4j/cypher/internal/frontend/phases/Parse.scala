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
import org.neo4j.cypher.internal.ast.factory.neo4j.JavaCCParser
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.CypherAstParser
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.PARSING
import org.neo4j.cypher.internal.frontend.phases.factories.ParsePipelineTransformerFactory
import org.neo4j.cypher.internal.rewriting.rewriters.LiteralExtractionStrategy
import org.neo4j.cypher.internal.util.CrossCompilation
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo

/**
 * Parse text into an AST object.
 */
case class Parse(useAntlr: Boolean) extends Phase[BaseContext, BaseState, BaseState] with StepSequencer.Step
    with ParsePipelineTransformerFactory {

  override def process(in: BaseState, context: BaseContext): BaseState = {
    if (useAntlr && !CrossCompilation.isTeaVM) {
      in.withStatement(CypherAstParser.parseStatements(
        in.queryText,
        context.cypherExceptionFactory,
        Some(context.notificationLogger)
      ))
    } else {
      in.withStatement(JavaCCParser.parse(
        in.queryText,
        context.cypherExceptionFactory,
        context.notificationLogger
      ))
    }
  }

  override val phase = PARSING

  override def preConditions: Set[StepSequencer.Condition] = Set.empty

  override def postConditions: Set[StepSequencer.Condition] =
    Set(
      BaseContains[Statement](),
      ValidSymbolicNamesInLabelExpressions
    )

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def getTransformer(
    literalExtractionStrategy: LiteralExtractionStrategy,
    parameterTypeMapping: Map[String, ParameterTypeInfo],
    semanticFeatures: Seq[SemanticFeature],
    obfuscateLiterals: Boolean = false
  ): Transformer[BaseContext, BaseState, BaseState] = this
}
