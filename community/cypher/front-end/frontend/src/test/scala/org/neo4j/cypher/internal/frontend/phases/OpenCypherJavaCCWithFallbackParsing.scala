/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.factory.neo4j.OpenCypherJavaCCParserWithFallback
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.PARSING
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory

case object OpenCypherJavaCCWithFallbackParsing extends Phase[BaseContext, BaseState, BaseState] {
  private val exceptionFactory = OpenCypherExceptionFactory(None)

  override def process(in: BaseState, context: BaseContext): BaseState = {
    val statement = OpenCypherJavaCCParserWithFallback.parse(in.queryText, exceptionFactory, new AnonymousVariableNameGenerator)
    in.withStatement(statement)
  }

  override val phase = PARSING

  override def postConditions = Set(BaseContains[Statement])
}
