/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.frontend.v3_4.phases

import org.neo4j.cypher.internal.frontend.v3_4.ast.Statement
import org.neo4j.cypher.internal.frontend.v3_4.parser.CypherParser
import org.neo4j.cypher.internal.frontend.v3_4.phases.CompilationPhaseTracer.CompilationPhase.PARSING

case object Parsing extends Phase[BaseContext, BaseState, BaseState] {
  private val parser = new CypherParser

  override def process(in: BaseState, ignored: BaseContext): BaseState =
    in.withStatement(parser.parse(in.queryText, in.startPosition))

  override val phase = PARSING

  override val description = "parse text into an AST object"

  override def postConditions = Set(BaseContains[Statement])
}
