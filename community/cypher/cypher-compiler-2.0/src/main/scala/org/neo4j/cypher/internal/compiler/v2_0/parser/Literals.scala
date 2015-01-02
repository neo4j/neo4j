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

import org.neo4j.cypher.internal.compiler.v2_0.ast
import org.parboiled.Context
import org.parboiled.scala._
import org.neo4j.cypher.internal.compiler.v2_0.ast.StringLiteral

trait Literals extends Parser
  with Base with Strings {

  def Expression: Rule1[ast.Expression]

  def Identifier: Rule1[ast.Identifier] = rule("an identifier") (
      IdentifierString ~~>> (ast.Identifier(_))
    | EscapedIdentifier
  ).memoMismatches

  private def IdentifierString: Rule1[String] = rule("an identifier") {
    group(IdentifierStart ~ zeroOrMore(IdentifierPart)) ~> (_.toString) ~ !IdentifierPart
  }

  def EscapedIdentifier: Rule1[ast.Identifier] = rule("an identifier") {
    EscapedIdentifierString ~~>> (ast.Identifier(_))
  }

  private def EscapedIdentifierString: Rule1[String] = rule("an identifier") {
    (oneOrMore(
      ch('`') ~ zeroOrMore(!ch('`') ~ ANY) ~> (_.toString) ~ ch('`')
    ) memoMismatches) ~~> (_.reduce(_ + '`' + _))
  }

  def Operator: Rule1[ast.Identifier] = rule {
    oneOrMore(OpChar) ~>>> (ast.Identifier(_: String)) ~ !OpChar
  }

  def MapLiteral: Rule1[ast.MapExpression] = rule {
    group(
      ch('{') ~~ zeroOrMore(Identifier ~~ ch(':') ~~ Expression, separator = CommaSep) ~~ ch('}')
    ) ~~>> (ast.MapExpression(_))
  }

  def Parameter: Rule1[ast.Parameter] = rule("a parameter") {
    ((ch('{') ~~ (IdentifierString | EscapedIdentifierString | UnsignedDecimalInteger ~> (_.toString)) ~~ ch('}')) memoMismatches) ~~>> (ast.Parameter(_))
  }

  def NumberLiteral: Rule1[ast.Literal] = rule("a number") (
      DoubleLiteral
    | SignedIntegerLiteral
  ).memoMismatches

  def DoubleLiteral: Rule1[ast.DecimalDoubleLiteral] = rule("a floating point number") (
      ExponentDecimalReal ~>>> (ast.DecimalDoubleLiteral(_))
    | RegularDecimalReal ~>>> (ast.DecimalDoubleLiteral(_))
  )

  def SignedIntegerLiteral: Rule1[ast.SignedIntegerLiteral] = rule("an integer") (
      HexInteger ~>>> (ast.SignedHexIntegerLiteral(_))
    | OctalInteger ~>>> (ast.SignedOctalIntegerLiteral(_))
    | DecimalInteger ~>>> (ast.SignedDecimalIntegerLiteral(_))
  )

  def UnsignedIntegerLiteral: Rule1[ast.UnsignedIntegerLiteral] = rule("an unsigned integer") {
    UnsignedDecimalInteger ~>>> (ast.UnsignedDecimalIntegerLiteral(_))
  }

  def RangeLiteral: Rule1[ast.Range] = rule (
      group(
        optional(UnsignedIntegerLiteral ~ WS) ~
        ".." ~
        optional(WS ~ UnsignedIntegerLiteral)
      ) ~~>> (ast.Range(_, _))
    | UnsignedIntegerLiteral ~~>> (l => ast.Range(Some(l), Some(l)))
  )

  def NodeLabels: Rule1[Seq[ast.Identifier]] = rule("node labels") {
    (oneOrMore(NodeLabel, separator = WS) memoMismatches).suppressSubnodes
  }

  def NodeLabel: Rule1[ast.Identifier] = rule {
    ((operator(":") ~~ Identifier) memoMismatches).suppressSubnodes
  }

  def StringLiteral: Rule1[ast.StringLiteral] = rule("\"...string...\"") {
    (((
       ch('\'') ~ StringCharacters('\'') ~ ch('\'')
     | ch('"') ~ StringCharacters('"') ~ ch('"')
    ) memoMismatches) suppressSubnodes) ~~>> (ast.StringLiteral(_))
  }
}
