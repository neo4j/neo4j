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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.factory.neo4j.JavaCCParser
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.PARSING
import org.neo4j.cypher.internal.frontend.phases.factories.ParsePipelineTransformerFactory
import org.neo4j.cypher.internal.parser.v5.ast.factory.Cypher5AstParser
import org.neo4j.cypher.internal.rewriting.rewriters.LiteralExtractionStrategy
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo

/**
 * Parse text into an AST object.
 */
case class Parse(useAntlr: Boolean, version: CypherVersion) extends Phase[BaseContext, BaseState, BaseState]
    with StepSequencer.Step
    with ParsePipelineTransformerFactory {

  override def process(in: BaseState, context: BaseContext): BaseState = {
    in.withStatement(parse(in.queryText, context.cypherExceptionFactory, context.notificationLogger))
  }

  private def parse(
    query: String,
    exceptionFactory: CypherExceptionFactory,
    notificationLogger: InternalNotificationLogger
  ): Statement = {
    if (useAntlr) {
      version match {
        case CypherVersion.Cypher5 => new Cypher5AstParser(
            query,
            exceptionFactory,
            Some(notificationLogger)
          ).singleStatement()
      }
    } else {
      version match {
        case CypherVersion.Cypher5 => JavaCCParser.parse(query, exceptionFactory, notificationLogger)
      }
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
