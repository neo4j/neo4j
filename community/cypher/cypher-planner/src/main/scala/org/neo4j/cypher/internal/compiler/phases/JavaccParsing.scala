/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.phases

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.factory.neo4j.OpenCypherJavaCCParserWithFallback
import org.neo4j.cypher.internal.compiler.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.compiler.helpers.Neo4jJavaCCParserWithFallback
import org.neo4j.cypher.internal.frontend.phases.BaseContains
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.PARSING
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory

/**
 * Parse text into an AST object.
 */
case object JavaccParsing extends Phase[BaseContext, BaseState, BaseState] {

  override def process(in: BaseState, context: BaseContext): BaseState = {
    context.cypherExceptionFactory match {
      case exceptionFactory: Neo4jCypherExceptionFactory => in.withStatement(Neo4jJavaCCParserWithFallback.parse(in.queryText, exceptionFactory, in.anonymousVariableNameGenerator))
      case exceptionFactory: OpenCypherExceptionFactory => in.withStatement(OpenCypherJavaCCParserWithFallback.parse(in.queryText, exceptionFactory, in.anonymousVariableNameGenerator))
    }
  }

  override val phase = PARSING

  override def postConditions = Set(BaseContains[Statement])
}
