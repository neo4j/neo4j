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

import org.neo4j.cypher.internal.parser.common.ast.factory.ASTExceptionFactory
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.InputPosition

import java.util

import scala.collection.convert.AsScalaConverters

class Neo4jASTExceptionFactory(inner: CypherExceptionFactory) extends ASTExceptionFactory with AsScalaConverters {

  override def syntaxException(
    got: String,
    expected: util.List[String],
    source: Exception,
    offset: Int,
    line: Int,
    column: Int
  ): Exception = {
    val exp: Seq[String] = asScala(expected).toSeq

    val message =
      new StringBuilder("Invalid input '")
        .append(got)
        .append("':")
        .append(" expected").append(
          if (exp.size == 1)
            " " + exp.head
          else if (exp.size < 5)
            " " + exp.init.mkString(", ") + " or " + exp.last
          else
            System.lineSeparator() + exp.map("  " + _).mkString(System.lineSeparator())
        ).result()

    inner.syntaxException(message, InputPosition(offset, line, column))
  }

  override def syntaxException(source: Exception, offset: Int, line: Int, column: Int): Exception =
    inner.syntaxException(source.getMessage, InputPosition(offset, line, column))
}
