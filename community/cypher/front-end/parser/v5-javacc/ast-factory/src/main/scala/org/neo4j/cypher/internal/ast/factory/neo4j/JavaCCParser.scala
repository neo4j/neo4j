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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.parser.javacc.Cypher
import org.neo4j.cypher.internal.parser.javacc.CypherCharStream
import org.neo4j.cypher.internal.parser.javacc.TokenMgrException
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.exceptions.CypherExecutionException
import org.neo4j.exceptions.SyntaxException
import org.neo4j.kernel.api.exceptions.Status.HasStatus

case object JavaCCParser {

  /**
   * @param queryText The query to be parsed.
   * @param cypherExceptionFactory A factory for producing error messages related to the specific implementation of the language.
   * @return
   */
  def parse(
    queryText: String,
    cypherExceptionFactory: CypherExceptionFactory,
    logger: InternalNotificationLogger = null
  ): Statement = {
    val charStream = new CypherCharStream(queryText)
    val astExceptionFactory = new Neo4jASTExceptionFactory(cypherExceptionFactory)
    val astFactory = new Neo4jASTFactory(queryText, astExceptionFactory, logger)

    val statements =
      try {
        new Cypher(astFactory, astExceptionFactory, charStream).Statements()
      } catch {
        // Specific error message for unclosed string literals and comments
        case _: TokenMgrException if findMismatchedDelimiters(queryText).nonEmpty =>
          val (errorMessage, position) = findMismatchedDelimiters(queryText).get
          throw new SyntaxException(errorMessage, queryText, position)

        // These are our own errors with Neo4j status codes so are safe to re-throw
        case e: OpenCypherExceptionFactory.SyntaxException => throw e
        case e: HasStatus                                  => throw e

        // Other errors which come from the underlying Javacc framework should not be exposed to the user
        case e: Exception => throw new CypherExecutionException(s"Failed to parse query `$queryText`.", e)
      }

    if (statements.size() == 1) {
      statements.get(0)
    } else {
      throw cypherExceptionFactory.syntaxException(
        s"Expected exactly one statement per query but got: ${statements.size}",
        InputPosition.NONE
      )
    }
  }

  private def findMismatchedDelimiters(input: String): Option[(String, Int)] = {
    var currentDelimiter: Option[(Char, Int)] = None

    for ((char, index) <- input.zipWithIndex) {
      def updateCurrentDelimiter(): Unit = {
        currentDelimiter = currentDelimiter match {
          case None              => Some((char, index)) // string literal or comment opened
          case Some((`char`, _)) => None // string literal or comment closed
          case _                 => currentDelimiter // other delimiter in progress, ignore this one
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
