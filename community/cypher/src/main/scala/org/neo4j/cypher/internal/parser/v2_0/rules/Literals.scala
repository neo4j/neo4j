/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.parser.v2_0.rules

import org.neo4j.cypher.internal.parser.v2_0.ast
import org.parboiled.Context
import org.parboiled.scala._

trait Literals extends Parser
  with Base {

  def Expression : Rule1[ast.Expression]

  def Identifier : Rule1[ast.Identifier] = rule("an identifier") (
      group((Letter | ch('_')) ~ zeroOrMore(IdentifierCharacter)) ~> t(ast.Identifier(_, _)) ~ !(IdentifierCharacter)
    | EscapedIdentifier
  ) memoMismatches

  def EscapedIdentifier : Rule1[ast.Identifier] = rule {
    ((oneOrMore(
        ch('`') ~ zeroOrMore(!ch('`') ~ ANY) ~> (_.toString) ~ ch('`')
    ) memoMismatches) ~~> (_.reduce(_ + '`' + _))) ~>> token ~~> ast.Identifier
  }

  def Operator : Rule1[ast.Identifier] = rule {
    oneOrMore(OperatorCharacter) ~> t(ast.Identifier(_, _)) ~ !(OperatorCharacter)
  }

  def MapLiteral : Rule1[ast.MapExpression] = rule {
    group(ch('{') ~~ zeroOrMore(Identifier ~~ ch(':') ~~ Expression, separator = CommaSep) ~~ ch('}')) ~>> token ~~> ast.MapExpression
  }

  def Parameter : Rule1[ast.Parameter] = rule("a parameter") {
    ((ch('{') ~~ (
        oneOrMore(IdentifierCharacter)
      | UnsignedInteger
    ) ~> (_.toString) ~~ ch('}')) memoMismatches) ~>> token ~~> ast.Parameter
  }

  def NumberLiteral : Rule1[ast.Number] = rule("a number") (
      Decimal ~> t((s, t) => ast.Double(s.toDouble, t))
    | Integer ~> t((s, t) => ast.SignedInteger(s.toLong, t))
  ) memoMismatches

  def UnsignedIntegerLiteral : Rule1[ast.UnsignedInteger] = rule("an unsigned integer") {
    UnsignedInteger ~> t((s, t) => ast.UnsignedInteger(s.toLong, t))
  }

  def RangeLiteral : Rule1[ast.Range] = rule (
      group(
          (UnsignedIntegerLiteral ~~> (Some(_)) ~ WS | EMPTY ~ push(None)) ~
          ".." ~
          (WS ~ UnsignedIntegerLiteral ~~> (Some(_)) | EMPTY ~ push(None))
      ) ~>> token ~~> ast.Range
    | UnsignedIntegerLiteral ~~> (l => ast.Range(Some(l), Some(l), l.token))
  )

  def NodeLabels : Rule1[Seq[ast.Identifier]] = rule("node labels") {
    oneOrMore(NodeLabel, separator = WS)
  }

  def NodeLabel : Rule1[ast.Identifier] = rule {
    operator(":") ~~ Identifier
  }

  def StringLiteral : Rule1[ast.StringLiteral] = rule("\"...string...\"") {
    (((
       ch('\'') ~ StringCharacters('\'') ~ ch('\'')
     | ch('"') ~ StringCharacters('"') ~ ch('"')
    ) memoMismatches) suppressSubnodes) ~>> token ~~> ast.StringLiteral
  }

  private def StringCharacters(c: Char) = {
    push(new StringBuilder) ~ zeroOrMore(EscapedChar(c) | NormalChar(c)) ~~> (_.toString)
  }

  private def NormalChar(c: Char) = {
    !(ch('\\') | ch(c)) ~ ANY ~:% withContext(appendToStringBuffer(_)(_))
  }

  private def EscapedChar(c: Char) = {
    "\\" ~ (
        ch('\\') ~:% withContext(appendToStringBuffer(_)(_))
      | ch(c) ~:% withContext(appendToStringBuffer(_)(_))
      | ch('b') ~ appendToStringBuffer('\b')
      | ch('f') ~ appendToStringBuffer('\f')
      | ch('n') ~ appendToStringBuffer('\n')
      | ch('r') ~ appendToStringBuffer('\r')
      | ch('t') ~ appendToStringBuffer('\t')
      | Unicode ~~% withContext((code, ctx) => appendToStringBuffer(code.asInstanceOf[Char])(ctx))
    )
  }

  private def Unicode = rule { ch('u') ~ group(HexDigit ~ HexDigit ~ HexDigit ~ HexDigit) ~> (java.lang.Integer.parseInt(_, 16)) }

  private def appendToStringBuffer(c: Any): Context[Any] => Unit = { ctx =>
    ctx.getValueStack.peek.asInstanceOf[StringBuilder].append(c)
    ()
  }
}
