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
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory

object OpenCypherJavaCCParserWithFallback {

  private val oldParser = new CypherParser()

  /**
   * This method should be used when parsing a query that might include administration commands which have not yet been ported to JavaCCParser.
   * It will try and parse with the JavaCCParser, and if that fails, we'll verify against shouldFallback that the query includes syntax which only the
   * old parser can parse. If that is the case we try to parse with the old parser.
   *
   * @param queryText                      The query to be parsed.
   * @param exceptionFactory               A factory for producing error messages related to the specific implementation of the language.
   * @param anonymousVariableNameGenerator Used to generate variable names during parsing.
   * @param offset                         An optional offset, only used in the old parser.
   * @return
   */
  def parse(queryText: String,
            exceptionFactory: OpenCypherExceptionFactory,
            anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
            offset: Option[InputPosition] = None): Statement = {
    try {
      JavaCCParser.parse(queryText, exceptionFactory, anonymousVariableNameGenerator)
    } catch {
      // OpenCypherExceptionFactory error messages does not include the original query, so we need to verify if we should fallback using the original query.
      case _: OpenCypherExceptionFactory.SyntaxException if JavaCCParser.shouldFallback(queryText) =>
        oldParser.parse(queryText, exceptionFactory)
    }
  }
}
