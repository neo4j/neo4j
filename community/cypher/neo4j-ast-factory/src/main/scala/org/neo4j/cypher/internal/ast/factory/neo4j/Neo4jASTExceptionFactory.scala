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
package org.neo4j.cypher.internal.ast.factory.neo4j

import java.util

import org.neo4j.cypher.internal.ast.factory.ASTExceptionFactory
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.InputPosition

import scala.collection.convert.AsScalaConverters



class Neo4jASTExceptionFactory(inner: CypherExceptionFactory) extends ASTExceptionFactory with AsScalaConverters {

  override def syntaxException(got: String,
                               expected: util.List[String],
                               source: Exception,
                               offset: Int,
                               line: Int,
                               column: Int): Exception = {
    val exp: Seq[String] = asScalaBuffer(expected).distinct

    val message =
      new StringBuilder("Invalid input '")
        .append(got)
        .append("':")
        .append(" expected ").append(
          if (exp.size == 1)
            exp.head
          else if (exp.size < 5)
            exp.init.mkString(", ") + " or " + exp.last
          else
            System.lineSeparator() + exp.map("  " + _).mkString(System.lineSeparator())
        ).result()

    inner.syntaxException(message, new InputPosition(offset, line, column))
  }

  override def syntaxException(source: Exception, offset: Int, line: Int, column: Int): Exception =
    inner.syntaxException(source.getMessage, new InputPosition(offset, line, column))
}
