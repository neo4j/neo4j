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

import org.neo4j.cypher.internal.parser.v2_0._
import org.parboiled.scala._

trait Expressions extends Parser
  with Literals
  with Patterns
  with Base {

  def Expression = Expression12

  private def Expression12 : Rule1[ast.Expression] = rule {
    Expression11 ~ zeroOrMore(WS ~ (
      keyword("OR") ~> identifier ~~ Expression11 ~~> (ast.FunctionInvocation(_: ast.Expression, _, _))
    ) : ReductionRule1[ast.Expression, ast.Expression])
  }

  private def Expression11 : Rule1[ast.Expression] = rule {
    Expression10 ~ zeroOrMore(WS ~ (
        keyword("XOR") ~> identifier ~~ Expression10 ~~> (ast.FunctionInvocation(_: ast.Expression, _, _))
    ) : ReductionRule1[ast.Expression, ast.Expression])
  }

  private def Expression10 : Rule1[ast.Expression] = rule {
    Expression9 ~ zeroOrMore(WS ~ (
        keyword("AND") ~> identifier ~~ Expression9 ~~> (ast.FunctionInvocation(_: ast.Expression, _, _))
    ) : ReductionRule1[ast.Expression, ast.Expression])
  }

  private def Expression9 : Rule1[ast.Expression] = rule {
    Expression8 ~ zeroOrMore(WS ~ (
        operator("=") ~> identifier ~~ Expression8 ~~> (ast.FunctionInvocation(_: ast.Expression, _, _))
      | operator("<>") ~> identifier ~~ Expression8 ~~> (ast.FunctionInvocation(_: ast.Expression, _, _))
      | operator("!=") ~> identifier ~~ Expression8 ~~> (ast.FunctionInvocation(_: ast.Expression, _, _))
    ) : ReductionRule1[ast.Expression, ast.Expression])
  }

  private def Expression8 : Rule1[ast.Expression] = rule {
    Expression7 ~ zeroOrMore(WS ~ (
        operator("<") ~> identifier ~~ Expression7 ~~> (ast.FunctionInvocation(_: ast.Expression, _, _))
      | operator(">") ~> identifier ~~ Expression7 ~~> (ast.FunctionInvocation(_: ast.Expression, _, _))
      | operator("<=") ~> identifier ~~ Expression7 ~~> (ast.FunctionInvocation(_: ast.Expression, _, _))
      | operator(">=") ~> identifier ~~ Expression7 ~~> (ast.FunctionInvocation(_: ast.Expression, _, _))
    ) : ReductionRule1[ast.Expression, ast.Expression])
  }

  private def Expression7 = Expression6

  private def Expression6 : Rule1[ast.Expression] = rule {
    Expression5 ~ zeroOrMore(WS ~ (
        operator("+") ~> identifier ~~ Expression5 ~~> (ast.FunctionInvocation(_: ast.Expression, _, _))
      | operator("-") ~> identifier ~~ Expression5 ~~> (ast.FunctionInvocation(_: ast.Expression, _, _))
    ) : ReductionRule1[ast.Expression, ast.Expression])
  }

  private def Expression5 : Rule1[ast.Expression] = rule {
    Expression4 ~ zeroOrMore(WS ~ (
        operator("*") ~> identifier ~~ Expression4 ~~> (ast.FunctionInvocation(_: ast.Expression, _, _))
      | operator("/") ~> identifier ~~ Expression4 ~~> (ast.FunctionInvocation(_: ast.Expression, _, _))
      | operator("%") ~> identifier ~~ Expression4 ~~> (ast.FunctionInvocation(_: ast.Expression, _, _))
      | operator("^") ~> identifier ~~ Expression4 ~~> (ast.FunctionInvocation(_: ast.Expression, _, _))
    ) : ReductionRule1[ast.Expression, ast.Expression])
  }

  private def Expression4 = Expression3

  private def Expression3 : Rule1[ast.Expression] = rule {
    Expression2 ~ zeroOrMore(WS ~ (
        ((operator(".") ~~ Identifier) memoMismatches) ~>> token ~~> ast.Property
      | NodeLabels ~>> token ~~> ast.HasLabels
      | operator("=~") ~> identifier ~~ Expression2 ~~> (ast.FunctionInvocation(_: ast.Expression, _, _))
      | keyword("IN") ~> identifier ~~ Expression2 ~~> (ast.FunctionInvocation(_: ast.Expression, _, _))
      | keyword("IS", "NULL") ~> identifier ~~> (ast.FunctionInvocation(_: ast.Expression, _))
      | keyword("IS", "NOT", "NULL") ~> identifier ~~> (ast.FunctionInvocation(_: ast.Expression, _))
    ) : ReductionRule1[ast.Expression, ast.Expression])
  }

  private def Expression2 : Rule1[ast.Expression] = rule (
      Parameter
    | "(" ~~ Expression ~~ ")"
    | ListComprehension
    | group("[" ~~ zeroOrMore(Expression, separator = CommaSep) ~~ "]") ~>> token ~~> ast.Collection
    | StringLiteral
    | NumberLiteral
    | MapLiteral
    | group(keyword("NOT") ~> identifier ~~ ("(" ~~ Expression ~~ ")" | Expression)) ~>> token ~~> (ast.FunctionInvocation(_, _, _))
    | keyword("NULL") ~>> token ~~> ast.Null
    | keyword("TRUE") ~>> token ~~> ast.True
    | keyword("FALSE") ~>> token ~~> ast.False
    | CaseExpression
    | group(keyword("COUNT") ~~ "(" ~~ "*" ~~ ")") ~>> token ~~> ast.CountStar
    | group(keyword("FILTER") ~~ "(" ~~ FilterExpression ~~ ")") ~>> token ~~> ast.FilterExpression
    | group(keyword("EXTRACT") ~~ "(" ~~ FilterExpression ~ optional(WS ~ "|" ~~ Expression) ~~ ")") ~>> token ~~> ast.ExtractExpression
    | group(keyword("REDUCE") ~~ "(" ~~ Identifier ~~ "=" ~~ Expression ~~ "," ~~ IdInColl ~~ "|" ~~ Expression ~~ ")") ~>> token ~~> ast.ReduceExpression
    | group(keyword("ALL") ~~ "(" ~~ FilterExpression ~~ ")") ~>> token ~~> ast.AllIterablePredicate
    | group(keyword("ANY") ~~ "(" ~~ FilterExpression ~~ ")") ~>> token ~~> ast.AnyIterablePredicate
    | group(keyword("NONE") ~~ "(" ~~ FilterExpression ~~ ")") ~>> token ~~> ast.NoneIterablePredicate
    | group(keyword("SINGLE") ~~ "(" ~~ FilterExpression ~~ ")") ~>> token ~~> ast.SingleIterablePredicate
    | ShortestPathPattern ~~> ast.ShortestPathExpression
    | FunctionInvocation
    | RelationshipsPattern ~~> ast.PatternExpression
    | group(Identifier ~~ NodeLabels) ~>> token ~~> ast.HasLabels
    | Identifier
  )

  def PropertyExpression : Rule1[ast.Property] = rule {
    Expression2 ~ oneOrMore(WS ~ (
      group(operator(".") ~~ Identifier) ~>> token ~~> ast.Property
    ) : ReductionRule1[ast.Expression, ast.Property])
  }

  private def FilterExpression : Rule3[ast.Identifier, ast.Expression, Option[ast.Expression]] =
    IdInColl ~ optional(WS ~ keyword("WHERE") ~~ Expression)

  private def IdInColl: Rule2[ast.Identifier, ast.Expression] =
    Identifier ~~ keyword("IN") ~~ Expression

  private def FunctionInvocation : Rule1[ast.FunctionInvocation] = rule {
    group(Identifier ~~ "(" ~~
      (keyword("DISTINCT") ~ push(true) | EMPTY ~ push(false)) ~~
      zeroOrMore(Expression, separator = CommaSep) ~~ ")"
    ) ~~> (_.toIndexedSeq) ~>> token ~~> (ast.FunctionInvocation(_, _, _, _))
  }

  def ListComprehension : Rule1[ast.ListComprehension] = rule("[") {
    group("[" ~~ FilterExpression ~ optional(WS ~ "|" ~~ Expression) ~~ "]") ~>> token ~~> ast.ListComprehension
  }

  def CaseExpression : Rule1[ast.CaseExpression] = rule("CASE") {
    group((
        keyword("CASE") ~~ push(None) ~ oneOrMore(WS ~ CaseAlternatives)
      | keyword("CASE") ~~ Expression ~~> (Some(_)) ~ oneOrMore(WS ~ CaseAlternatives)
      ) ~ optional(WS ~
        keyword("ELSE") ~~ Expression
      ) ~~ keyword("END")
    ) ~>> token ~~> ast.CaseExpression
  }

  private def CaseAlternatives : Rule2[ast.Expression, ast.Expression] = rule("WHEN") {
    keyword("WHEN") ~~ Expression ~~ keyword("THEN") ~~ Expression
  }
}
