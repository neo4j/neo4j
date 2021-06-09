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
import org.neo4j.cypher.internal.parser.CypherParser
import org.neo4j.cypher.internal.parser.javacc.Cypher
import org.neo4j.cypher.internal.parser.javacc.CypherCharStream
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.exceptions.SyntaxException

case object JavaCCParser {
  // Triggers to fallback to parboiled parser

  private val oldParser = new CypherParser

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

  /**
   * parseWillFallback() should be used when parsing a query that might include administration commands which have not yet been ported to JavaCCParser.
   * It will try and parse with the JavaCCParser, and if that fails, we'll verify against shouldFallback that the query includes syntax which only the
   * old parser can parse. If that is the case we try to parse with the old parser.
   * @param queryText The query to be parsed.
   * @param cypherExceptionFactory A factory for producing error messages related to the specific implementation of the language.
   * @param offset An optional offset, only used in the old parser.
   * @return
   */
  def parseWithFallback(queryText: String, cypherExceptionFactory: CypherExceptionFactory, anonymousVariableNameGenerator: AnonymousVariableNameGenerator, offset: Option[InputPosition] = None): Statement = {
    try {
      parse(queryText, cypherExceptionFactory, anonymousVariableNameGenerator)
    } catch {
      // OpenCypherExceptionFactory error messages does not include the original query, so we need to verify if we should fallback using the original query.
      case _: OpenCypherExceptionFactory.SyntaxException if shouldFallback(queryText) =>
        oldParser.parse(queryText, cypherExceptionFactory, offset)

      // Neo4jCypherExceptionFactory error messages includes the original query, but removes some things like comments, so verifying if we should fallback is
      // "secured" against eventual comments that include any fallback triggers.
      case e: SyntaxException if shouldFallback(e.getMessage) =>
        oldParser.parse(queryText, cypherExceptionFactory, offset)
    }
  }
}
