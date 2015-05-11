/*
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
package org.neo4j.cypher.internal.compiler.v2_3.parser


import org.neo4j.cypher.internal.compiler.v2_3.{ast, _}
import org.parboiled.errors.ParseError
import org.parboiled.scala._

class CypherParser(monitor: ParserMonitor[ast.Statement]) extends Parser
  with Statement
  with Expressions {


  @throws(classOf[SyntaxException])
  def parse(queryText: String, offset: Option[InputPosition] = None): ast.Statement =
    parseOrThrow(queryText, offset, CypherParser.Statements, Some(monitor))
}

object CypherParser extends Parser with Statement with Expressions {
  val Statements: Rule1[Seq[ast.Statement]] = rule {
    oneOrMore(WS ~ Statement ~ WS, separator = ch(';')) ~~ optional(ch(';')) ~~ EOI.label("end of input")
  }
}

trait ParserMonitor[T] {
  def startParsing(query: String)
  def finishParsingSuccess(query: String, result: T)
  def finishParsingError(query:String, errors: Seq[ParseError])
}

object ParserMonitor {
  def empty[T] = new ParserMonitor[T] {
    override def startParsing(query: String): Unit = ()
    override def finishParsingSuccess(query: String, result: T): Unit = ()
    override def finishParsingError(query: String, errors: Seq[ParseError]): Unit = ()
  }
}
