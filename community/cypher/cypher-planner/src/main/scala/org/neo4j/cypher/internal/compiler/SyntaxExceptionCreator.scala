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
package org.neo4j.cypher.internal.compiler

import org.neo4j.cypher.internal.ast.semantics.FeatureError
import org.neo4j.cypher.internal.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.exceptions.ArithmeticException
import org.neo4j.exceptions.Neo4jException
import org.neo4j.exceptions.SyntaxException

case class Neo4jCypherExceptionFactory(queryText: String, preParserOffset: Option[InputPosition]) extends CypherExceptionFactory {

  override def arithmeticException(message: String, cause: Exception): Neo4jException = new ArithmeticException(message, cause)

  override def syntaxException(message: String, pos: InputPosition): Neo4jException = {
    val adjustedPosition = pos.withOffset(preParserOffset)
    new SyntaxException(s"$message ($adjustedPosition)", queryText, adjustedPosition.offset)
  }
}

object SyntaxExceptionCreator {
  def throwOnError(exceptionFactory: CypherExceptionFactory): Seq[SemanticErrorDef] => Unit =
    (errors: Seq[SemanticErrorDef]) => errors.foreach(e => throw createException(exceptionFactory, e))

  private def createException(exceptionFactory: CypherExceptionFactory, error: SemanticErrorDef): Exception = {
    val message = error match {

      // In neo4j, disabled SemanticFeature.UseGraphSelector indicates that you are trying to run a Fabric query
      case FeatureError(_, SemanticFeature.UseGraphSelector, _) =>
        "The USE clause is not available in embedded or http sessions. Try running the query using a Neo4j driver."

      case e =>
        e.msg
    }
    exceptionFactory.syntaxException(message, error.position)
  }
}
