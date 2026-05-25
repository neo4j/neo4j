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
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.frontend.phases.BaseContains
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.PARSING
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.ValidSymbolicNamesInLabelExpressions
import org.neo4j.cypher.internal.frontend.phases.factories.ParsePipelineTransformerFactory
import org.neo4j.cypher.internal.frontend.phases.factories.ParsingConfig
import org.neo4j.cypher.internal.notification.InternalNotificationLogger
import org.neo4j.cypher.internal.parser.AstParserFactory
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.exceptions.SyntaxException

/**
 * Parse text into an AST object.
 */
case object Parse extends Phase[BaseContext, BaseState, BaseState]
    with StepSequencer.Step
    with ParsePipelineTransformerFactory {

  override def process(in: BaseState, context: BaseContext): BaseState = {
    in.withStatement(
      parse(
        in.queryText,
        context.cypherVersion,
        context.cypherExceptionFactory,
        Some(context.notificationLogger),
        context.semanticFeatures
      )
    )
  }

  def parse(
    query: String,
    version: CypherVersion,
    exceptionFactory: CypherExceptionFactory,
    notificationLogger: Option[InternalNotificationLogger],
    semanticFeatures: Seq[SemanticFeature]
  ): Statement = {
    try {
      AstParserFactory(version)(
        query,
        exceptionFactory,
        notificationLogger,
        semanticFeatures
      ).singleStatement()
    } catch {
      case e: SyntaxException if version == CypherVersion.Cypher5 =>
        try {
          AstParserFactory(CypherVersion.Cypher25)(
            query,
            exceptionFactory,
            notificationLogger,
            semanticFeatures
          ).singleStatement()
          throw exceptionFactory.insertExistsInOtherLanguageVersion(5, 25, e)
        } catch {
          case _: Throwable => throw e
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

  override def getTransformer(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] = this
}
