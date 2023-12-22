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
import org.neo4j.cypher.internal.parser.javacc.TokenMgrException
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.exceptions.SyntaxException

case object JavaCCParser {

  // Triggers to fallback to parboiled parser
  // The various SHOW PRIVILEGE commands and the EXECUTE privileges are still left to be ported.
  // The START keyword is only available in parboiled to get better error message for a removed feature,
  // and can be dropped entirely in 5.0 or when parboiled is removed.
  private val FALLBACK_TRIGGERS = Seq("PRIVILEGE", "EXECUTE", "START")

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

    val statements =
      try {
        new Cypher(astFactory, astExceptionFactory, charStream).Statements()
      } catch {
        // Specific error message for unclosed string literals and comments
        case _: TokenMgrException if findMismatchedDelimiters(queryText).nonEmpty =>
          val (errorMessage, index) = findMismatchedDelimiters(queryText).get
          throw new SyntaxException(errorMessage, queryText, index)

        case e =>
          throw e
      }

    if (statements.size() == 1) {
      statements.get(0)
    } else {
      throw cypherExceptionFactory.syntaxException(s"Expected exactly one statement per query but got: ${statements.size}", InputPosition.NONE)
    }
  }

  private def findMismatchedDelimiters(input: String): Option[(String, Int)] = {
    var currentDelimiter: Option[(Char, Int)] = None

    for ((char, index) <- input.zipWithIndex) {
      def updateCurrentDelimiter(): Unit = {
        currentDelimiter = currentDelimiter match {
          case None => Some((char, index)) // string literal or comment opened
          case Some((`char`, _)) => None // string literal or comment closed
          case _ => currentDelimiter // other delimiter in progress, ignore this one
        }
      }

      char match {
        case '"' if (index == 0) || (input.charAt(index - 1) != '\\') => // Unescaped double quote
          updateCurrentDelimiter()

        case '\'' if (index == 0) || (input.charAt(index - 1) != '\\') => // Unescaped single quote
          updateCurrentDelimiter()

        case '/' if (index < input.length - 1) || (input.charAt(index + 1) == '*') => // Start of comment
          updateCurrentDelimiter()

        case _ =>
      }
    }
    // Return position to unmatched quotes
    currentDelimiter match {
      case Some(('/', index)) =>
        Some(("Failed to parse comment. A comment starting on `/*` must have a closing `*/`.", index))
      case Some((_, index)) =>
        Some(("Failed to parse string literal. The query must contain an even number of non-escaped quotes.", index))
      case None => None
    }
  }
}
