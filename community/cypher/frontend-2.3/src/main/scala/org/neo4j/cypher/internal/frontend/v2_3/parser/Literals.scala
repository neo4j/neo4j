/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v2_3.parser

import org.neo4j.cypher.internal.frontend.v2_3.ast
import org.parboiled.scala._

trait Literals extends Parser
  with Base with Strings {

  def Expression: Rule1[ast.Expression]

  def Identifier: Rule1[ast.Identifier] =
    rule("an identifier") { SymbolicNameString ~~>> (ast.Identifier(_) ) }.memoMismatches

  def FunctionName: Rule1[ast.FunctionName] =
    rule("a function name") { SymbolicNameString ~~>> (ast.FunctionName(_) ) }.memoMismatches

  def EscapedIdentifier: Rule1[ast.Identifier] =
    rule("an identifier") { EscapedSymbolicNameString ~~>> (ast.Identifier(_)) }

  def PropertyKeyName: Rule1[ast.PropertyKeyName] =
    rule("a property key name") { SymbolicNameString ~~>> (ast.PropertyKeyName(_) ) }.memoMismatches

  def LabelName: Rule1[ast.LabelName] =
    rule("a label name") { SymbolicNameString ~~>> (ast.LabelName(_) ) }.memoMismatches

  def RelTypeName: Rule1[ast.RelTypeName] =
    rule("a rel type name") { SymbolicNameString ~~>> (ast.RelTypeName(_) ) }.memoMismatches

  def Operator: Rule1[ast.Identifier] = rule {
    OpChar ~ zeroOrMore(OpCharTail) ~>>> (ast.Identifier(_: String)) ~ !OpCharTail
  }

  def MapLiteral: Rule1[ast.MapExpression] = rule {
    group(
      ch('{') ~~ zeroOrMore(PropertyKeyName ~~ ch(':') ~~ Expression, separator = CommaSep) ~~ ch('}')
    ) ~~>> (ast.MapExpression(_))
  }

  def Parameter: Rule1[ast.Parameter] = rule("a parameter") {
    ((ch('{') ~~ (UnescapedSymbolicNameString | EscapedSymbolicNameString | UnsignedDecimalInteger ~> (_.toString)) ~~ ch('}')) memoMismatches) ~~>> (ast.Parameter(_))
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

  def NodeLabels: Rule1[Seq[ast.LabelName]] = rule("node labels") {
    (oneOrMore(NodeLabel, separator = WS) memoMismatches).suppressSubnodes
  }

  def NodeLabel: Rule1[ast.LabelName] = rule {
    ((operator(":") ~~ LabelName) memoMismatches).suppressSubnodes
  }

  def RelType: Rule1[ast.RelTypeName] = rule {
    ((operator(":") ~~ RelTypeName) memoMismatches).suppressSubnodes
  }

  def StringLiteral: Rule1[ast.StringLiteral] = rule("\"...string...\"") {
    (((
       ch('\'') ~ StringCharacters('\'') ~ ch('\'')
     | ch('"') ~ StringCharacters('"') ~ ch('"')
    ) memoMismatches) suppressSubnodes) ~~>> (ast.StringLiteral(_))
  }
}
