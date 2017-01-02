/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.parser

import org.neo4j.cypher.internal.compiler.v2_2._
import org.parboiled.scala._

trait Expressions extends Parser
  with Literals
  with Patterns
  with Base {

  // Precedence loosely based on http://en.wikipedia.org/wiki/Operators_in_C_and_C%2B%2B#Operator_precedence

  def Expression = Expression14

  private def Expression14: Rule1[ast.Expression] = rule("an expression") {
    Expression13 ~ zeroOrMore(WS ~ (
        group(keyword("OR") ~~ Expression13) ~~>> (ast.Or(_: ast.Expression, _))
    ): ReductionRule1[ast.Expression, ast.Expression])
  }

  private def Expression13: Rule1[ast.Expression] = rule("an expression") {
    Expression12 ~ zeroOrMore(WS ~ (
        group(keyword("XOR") ~~ Expression12) ~~>> (ast.Xor(_: ast.Expression, _))
    ): ReductionRule1[ast.Expression, ast.Expression])
  }

  private def Expression12: Rule1[ast.Expression] = rule("an expression") {
    Expression11 ~ zeroOrMore(WS ~ (
        group(keyword("AND") ~~ Expression11) ~~>> (ast.And(_: ast.Expression, _))
    ): ReductionRule1[ast.Expression, ast.Expression])
  }

  private def Expression11 = Expression10

  private def Expression10: Rule1[ast.Expression] = rule("an expression") (
      group(keyword("NOT") ~~ Expression10) ~~>> (ast.Not(_))
    | Expression9
  )

  private def Expression9: Rule1[ast.Expression] = rule("an expression") {
    Expression8 ~ zeroOrMore(WS ~ (
        group(operator("=") ~~ Expression8) ~~>> (ast.Equals(_: ast.Expression, _))
      | group(operator("<>") ~~ Expression8) ~~>> (ast.NotEquals(_: ast.Expression, _))
      | group(operator("!=") ~~ Expression8) ~~>> (ast.InvalidNotEquals(_: ast.Expression, _))
    ))
  }

  private def Expression8: Rule1[ast.Expression] = rule("an expression") {
    Expression7 ~ zeroOrMore(WS ~ (
        group(operator("<") ~~ Expression7) ~~>> (ast.LessThan(_: ast.Expression, _))
      | group(operator(">") ~~ Expression7) ~~>> (ast.GreaterThan(_: ast.Expression, _))
      | group(operator("<=") ~~ Expression7) ~~>> (ast.LessThanOrEqual(_: ast.Expression, _))
      | group(operator(">=") ~~ Expression7) ~~>> (ast.GreaterThanOrEqual(_: ast.Expression, _))
    ))
  }

  private def Expression7: Rule1[ast.Expression] = rule("an expression") {
    Expression6 ~ zeroOrMore(WS ~ (
      group(operator("+") ~~ Expression6) ~~>> (ast.Add(_: ast.Expression, _))
        | group(operator("-") ~~ Expression6) ~~>> (ast.Subtract(_: ast.Expression, _))
      ))
  }

  private def Expression6: Rule1[ast.Expression] = rule("an expression") {
    Expression5 ~ zeroOrMore(WS ~ (
      group(operator("*") ~~ Expression5) ~~>> (ast.Multiply(_: ast.Expression, _))
        | group(operator("/") ~~ Expression5) ~~>> (ast.Divide(_: ast.Expression, _))
        | group(operator("%") ~~ Expression5) ~~>> (ast.Modulo(_: ast.Expression, _))
      ))
  }

  private def Expression5: Rule1[ast.Expression] = rule("an expression") {
    Expression4 ~ zeroOrMore(WS ~ (
      group(operator("^") ~~ Expression4) ~~>> (ast.Pow(_: ast.Expression, _))
      ))
  }

  private def Expression4: Rule1[ast.Expression] = rule("an expression") (
      Expression3
    | group(operator("+") ~~ Expression4) ~~>> (ast.UnaryAdd(_))
    | group(operator("-") ~~ Expression4) ~~>> (ast.UnarySubtract(_))
  )

  private def Expression3: Rule1[ast.Expression] = rule("an expression") {
    Expression2 ~ zeroOrMore(WS ~ (
        "[" ~~ Expression ~~ "]" ~~>> (ast.CollectionIndex(_: ast.Expression, _))
      | "[" ~~ optional(Expression) ~~ ".." ~~ optional(Expression) ~~ "]" ~~>> (ast.CollectionSlice(_: ast.Expression, _, _))
      | group(operator("=~") ~~ Expression2) ~~>> (ast.RegexMatch(_: ast.Expression, _))
      | group(keyword("IN") ~~ Expression2) ~~>> (ast.In(_: ast.Expression, _))
      | keyword("IS NULL") ~~>> (ast.IsNull(_: ast.Expression))
      | keyword("IS NOT NULL") ~~>> (ast.IsNotNull(_: ast.Expression))
    ): ReductionRule1[ast.Expression, ast.Expression])
  }

  private def Expression2: Rule1[ast.Expression] = rule("an expression") {
    Expression1 ~ zeroOrMore(WS ~ (
        PropertyLookup
      | NodeLabels ~~>> (ast.HasLabels(_: ast.Expression, _))
    ))
  }

  private def Expression1: Rule1[ast.Expression] = rule("an expression") (
      NumberLiteral
    | StringLiteral
    | Parameter
    | keyword("TRUE") ~ push(ast.True()(_))
    | keyword("FALSE") ~ push(ast.False()(_))
    | keyword("NULL") ~ push(ast.Null()(_))
    | CaseExpression
    | group(keyword("COUNT") ~~ "(" ~~ "*" ~~ ")") ~ push(ast.CountStar()(_))
    | MapLiteral
    | ListComprehension
    | group("[" ~~ zeroOrMore(Expression, separator = CommaSep) ~~ "]") ~~>> (ast.Collection(_))
    | group(keyword("FILTER") ~~ "(" ~~ FilterExpression ~~ ")") ~~>> (ast.FilterExpression(_, _, _))
    | group(keyword("EXTRACT") ~~ "(" ~~ FilterExpression ~ optional(WS ~ "|" ~~ Expression) ~~ ")") ~~>> (ast.ExtractExpression(_, _, _, _))
    | group(keyword("REDUCE") ~~ "(" ~~ Identifier ~~ "=" ~~ Expression ~~ "," ~~ IdInColl ~~ "|" ~~ Expression ~~ ")") ~~>> (ast.ReduceExpression(_, _, _, _, _))
    | group(keyword("ALL") ~~ "(" ~~ FilterExpression ~~ ")") ~~>> (ast.AllIterablePredicate(_, _, _))
    | group(keyword("ANY") ~~ "(" ~~ FilterExpression ~~ ")") ~~>> (ast.AnyIterablePredicate(_, _, _))
    | group(keyword("NONE") ~~ "(" ~~ FilterExpression ~~ ")") ~~>> (ast.NoneIterablePredicate(_, _, _))
    | group(keyword("SINGLE") ~~ "(" ~~ FilterExpression ~~ ")") ~~>> (ast.SingleIterablePredicate(_, _, _))
    | ShortestPathPattern ~~> ast.ShortestPathExpression
    | RelationshipsPattern ~~> ast.PatternExpression
    | parenthesizedExpression
    | FunctionInvocation
    | Identifier
  )

  def parenthesizedExpression: Rule1[ast.Expression] = "(" ~~ Expression ~~ ")"

  def PropertyExpression: Rule1[ast.Property] = rule {
    Expression1 ~ oneOrMore(WS ~ PropertyLookup)
  }

  private def PropertyLookup: ReductionRule1[ast.Expression, ast.Property] = rule("'.'") {
    operator(".") ~~ (
        (group(PropertyKeyName ~~ group(anyOf("?!") ~ !OpCharTail) ~> ((s:String) => s)) ~~>> (ast.LegacyProperty(_: ast.Expression, _, _)))
      | (PropertyKeyName ~~>> (ast.Property(_: ast.Expression, _)))
    )
  }

  private def FilterExpression: Rule3[ast.Identifier, ast.Expression, Option[ast.Expression]] =
    IdInColl ~ optional(WS ~ keyword("WHERE") ~~ Expression)

  private def IdInColl: Rule2[ast.Identifier, ast.Expression] =
    Identifier ~~ keyword("IN") ~~ Expression

  private def FunctionInvocation: Rule1[ast.FunctionInvocation] = rule("a function") {
    ((group(FunctionName ~~ "(" ~~
      (keyword("DISTINCT") ~ push(true) | EMPTY ~ push(false)) ~~
      zeroOrMore(Expression, separator = CommaSep) ~~ ")"
    ) ~~> (_.toIndexedSeq)) memoMismatches) ~~>> (ast.FunctionInvocation(_, _, _))
  }

  def ListComprehension: Rule1[ast.ListComprehension] = rule("[") {
    group("[" ~~ FilterExpression ~ optional(WS ~ "|" ~~ Expression) ~~ "]") ~~>> (ast.ListComprehension(_, _, _, _))
  }

  def CaseExpression: Rule1[ast.CaseExpression] = rule("CASE") {
    (group((
        keyword("CASE") ~~ push(None) ~ oneOrMore(WS ~ CaseAlternatives)
      | keyword("CASE") ~~ Expression ~~> (Some(_)) ~ oneOrMore(WS ~ CaseAlternatives)
      ) ~ optional(WS ~
        keyword("ELSE") ~~ Expression
      ) ~~ keyword("END")
    ) memoMismatches) ~~>> (ast.CaseExpression(_, _, _))
  }

  private def CaseAlternatives: Rule2[ast.Expression, ast.Expression] = rule("WHEN") {
    keyword("WHEN") ~~ Expression ~~ keyword("THEN") ~~ Expression
  }
}
