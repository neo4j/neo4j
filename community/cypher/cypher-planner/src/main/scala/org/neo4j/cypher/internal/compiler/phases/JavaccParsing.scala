/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.cypher.internal.ast.factory.neo4j.Neo4jASTExceptionFactory
import org.neo4j.cypher.internal.ast.factory.neo4j.Neo4jASTFactory
import org.neo4j.cypher.internal.frontend.phases.BaseContains
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.PARSING
import org.neo4j.cypher.internal.frontend.phases.Parsing
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.parser.javacc.Cypher
import org.neo4j.cypher.internal.parser.javacc.CypherCharStream
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.exceptions.SyntaxException

case object JavaccParsing extends Phase[BaseContext, BaseState, BaseState] {

  private val FALLBACK_TRIGGERS = Seq(
                                      // Schema commands
                                      "INDEX",
                                      "CONSTRAINT",
                                      // System commands
                                      "DROP",
                                      "DATABASE",
                                      "ROLE",
                                      "SHOW",
                                      "GRANT",
                                      "DENY",
                                      "ALTER",
                                      "USER",
                                      "REVOKE",
                                      // Graph commands
                                      "CONSTRUCT",
                                      "CATALOG",
                                      "~")

  private def shouldFallBack(errorMsg: String): Boolean = {
    val upper = errorMsg.toUpperCase()
    FALLBACK_TRIGGERS.exists(upper.contains)
  }

  override def process(in: BaseState, context: BaseContext): BaseState = {
    val charStream = new CypherCharStream(in.queryText)
    val astFactory = new Neo4jASTFactory(in.queryText)
    val astExceptionFactory = new Neo4jASTExceptionFactory(context.cypherExceptionFactory)

    try {
      val statements = new Cypher(astFactory, astExceptionFactory, charStream).Statements()
      if (statements.size() == 1) {
        in.withStatement(statements.get(0))
      } else {
        throw context.cypherExceptionFactory.syntaxException(s"Expected exactly one statement per query but got: ${statements.size}", InputPosition.NONE)
      }
    } catch {
      case e: SyntaxException if shouldFallBack(e.getMessage) =>
        Parsing.process(in, context)
    }
  }

  override val phase = PARSING

  override val description = "parse text into an AST object"

  override def postConditions = Set(BaseContains[Statement])
}
