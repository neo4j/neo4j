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
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.PARSING
import org.neo4j.cypher.internal.parser.v5.ast.factory.Cypher5AstParser
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory

case object Cypher5Parsing extends Phase[BaseContext, BaseState, BaseState] {
  private val exceptionFactory = OpenCypherExceptionFactory(None)

  override def process(in: BaseState, context: BaseContext): BaseState = {
    val statement = new Cypher5AstParser(in.queryText, exceptionFactory, None).singleStatement()
    in.withStatement(statement)
  }

  override val phase = PARSING

  override def postConditions = Set(BaseContains[Statement]())
}
