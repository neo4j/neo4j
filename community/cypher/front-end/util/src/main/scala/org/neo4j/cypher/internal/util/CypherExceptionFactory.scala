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
package org.neo4j.cypher.internal.util

import org.neo4j.exceptions.ArithmeticException
import org.neo4j.exceptions.Neo4jException
import org.neo4j.exceptions.SyntaxException

trait CypherExceptionFactory {
  def arithmeticException(message: String, cause: Exception = null): RuntimeException
  def syntaxException(message: String, pos: InputPosition): RuntimeException
}

case class Neo4jCypherExceptionFactory(queryText: String, preParserOffset: Option[InputPosition])
    extends CypherExceptionFactory {

  override def arithmeticException(message: String, cause: Exception): Neo4jException =
    new ArithmeticException(message, cause)

  override def syntaxException(message: String, pos: InputPosition): Neo4jException = {
    val adjustedPosition = pos.withOffset(preParserOffset)
    new SyntaxException(s"$message ($adjustedPosition)", queryText, adjustedPosition.offset)
  }
}
