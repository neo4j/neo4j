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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.parser.javacc.Cypher
import org.neo4j.cypher.internal.parser.javacc.CypherCharStream
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.InputPosition

case object JavaCCParser {

  // Triggers to fallback to parboiled parser
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
    "CATALOG")

  def shouldFallback(errorMsg: String): Boolean = {
    val upper = errorMsg.toUpperCase()
    FALLBACK_TRIGGERS.exists(upper.contains)
  }

  /**
   * parse() should only be used when parsing a query that is certain to not include an administration command that has not yet been ported to JavaCCParser.
   * Most likely, it should only be in tests.
   * @param queryText The query to be parsed.
   * @param cypherExceptionFactory A factory for producing error messages related to the specific implementation of the language.
   * @return
   */
  def parse(queryText: String, cypherExceptionFactory: CypherExceptionFactory, anonymousVariableNameGenerator: AnonymousVariableNameGenerator): Statement = {
    val charStream = new CypherCharStream(queryText)
    val astFactory = new Neo4jASTFactory(queryText, anonymousVariableNameGenerator)
    val astExceptionFactory = new Neo4jASTExceptionFactory(cypherExceptionFactory)

    val statements = new Cypher(astFactory, astExceptionFactory, charStream).Statements()
    if (statements.size() == 1) {
      statements.get(0)
    } else {
      throw cypherExceptionFactory.syntaxException(s"Expected exactly one statement per query but got: ${statements.size}", InputPosition.NONE)
    }
  }
}
