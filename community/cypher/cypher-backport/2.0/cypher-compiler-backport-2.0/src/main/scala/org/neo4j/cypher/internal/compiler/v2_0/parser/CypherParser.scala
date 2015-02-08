/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_0.parser

import org.neo4j.cypher.internal.compiler.v2_0._
import org.neo4j.cypher.internal.compiler.v2_0.ast.convert.StatementConverters._
import org.neo4j.cypher.internal.compiler.v2_0.commands.AbstractQuery
import org.neo4j.cypher.SyntaxException
import org.neo4j.helpers.ThisShouldNotHappenError
import org.parboiled.scala._
import org.parboiled.errors.InvalidInputError

case class CypherParser() extends Parser
  with Statement
  with Expressions {


  @throws(classOf[SyntaxException])
  def parse(text: String): ast.Statement = {
    val parsingResult = ReportingParseRunner(CypherParser.SingleStatement).run(text)
    parsingResult.result match {
      case Some(statement: ast.Statement) => statement
      case _ => {
        parsingResult.parseErrors.map { error =>
          val message = if (error.getErrorMessage != null) {
            error.getErrorMessage
          } else {
            error match {
              case invalidInput: InvalidInputError => new InvalidInputErrorFormatter().format(invalidInput)
              case _                               => error.getClass.getSimpleName
            }
          }
          val position = BufferPosition(error.getInputBuffer, error.getStartIndex)
          throw new SyntaxException(s"$message ($position)", text, position.offset)
        }
      }
        throw new ThisShouldNotHappenError("cleishm", "Parsing failed but no parse errors were provided")
    }
  }

  @throws(classOf[SyntaxException])
  def parseToQuery(query: String): AbstractQuery = {
    val statement = parse(query)
    statement.semanticCheck(SemanticState.clean).errors.map { error =>
      throw new SyntaxException(s"${error.msg} (${error.position})", query, error.position.offset)
    }
    ReattachAliasedExpressions(statement.asQuery.setQueryText(query))
  }
}

object CypherParser extends Parser with Statement with Expressions {
  val SingleStatement: Rule1[ast.Statement] = rule {
    WS ~ Statement ~~ optional(ch(';') ~ WS) ~ EOI.label("end of input")
  }
}
