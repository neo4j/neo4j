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
package org.neo4j.cypher.internal.parser.experimental.rules

import org.neo4j.cypher.internal.parser.experimental._
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
        operator("=~") ~> identifier ~~ Expression2 ~~> (ast.FunctionInvocation(_: ast.Expression, _, _))
      | keyword("IN") ~> identifier ~~ Expression2 ~~> (ast.FunctionInvocation(_: ast.Expression, _, _))
      | keyword("IS", "NULL") ~> identifier ~~> (ast.FunctionInvocation(_: ast.Expression, _))
      | keyword("IS", "NOT", "NULL") ~> identifier ~~> (ast.FunctionInvocation(_: ast.Expression, _))
    ) : ReductionRule1[ast.Expression, ast.Expression])
  }

  private def Expression2 : Rule1[ast.Expression] = rule (
      "(" ~~ Expression ~~ ")"
    | group(keyword("NOT") ~> identifier ~~ ("(" ~~ Expression ~~ ")" | Expression)) ~>> token ~~> (ast.FunctionInvocation(_, _, _))
    | keyword("NULL") ~>> token ~~> ast.Null
    | keyword("TRUE") ~>> token ~~> ast.True
    | keyword("FALSE") ~>> token ~~> ast.False
    | group(keyword("COUNT") ~~ "(" ~~ "*" ~~ ")") ~>> token ~~> ast.CountStar
    | group(keyword("FILTER") ~~ "(" ~~ FilterExpression ~~ ")") ~>> token ~~> ast.FilterFunction
    | group(keyword("EXTRACT") ~~ "(" ~~ FilterExpression ~~ ("|" ~~ Expression ~~> (Some(_)) | EMPTY ~ push(None)) ~~ ")") ~>> token ~~> ast.ExtractFunction
    | group(keyword("REDUCE") ~~ "(" ~~ Identifier ~~ "=" ~~ Expression ~~ "," ~~ IdInColl ~~ "|" ~~ Expression ~~ ")") ~>> token ~~> ast.Reduce
    | group(keyword("ALL") ~~ "(" ~~ FilterExpression ~~ ")") ~>> token ~~> ast.AllIterablePredicate
    | group(keyword("ANY") ~~ "(" ~~ FilterExpression ~~ ")") ~>> token ~~> ast.AnyIterablePredicate
    | group(keyword("NONE") ~~ "(" ~~ FilterExpression ~~ ")") ~>> token ~~> ast.NoneIterablePredicate
    | group(keyword("SINGLE") ~~ "(" ~~ FilterExpression ~~ ")") ~>> token ~~> ast.SingleIterablePredicate
    | FunctionInvocation
    | RelationshipsPattern ~>> token ~~> ast.PatternExpression
    | Parameter
    | StringLiteral
    | NumberLiteral
    | MaybeNullableProperty
    | group(Identifier ~~ NodeLabels) ~>> token ~~> ast.HasLabels
    | Identifier
    | ListComprehension
    | group("[" ~~ zeroOrMore(Expression, separator = CommaSep) ~~ "]") ~>> token ~~> ast.Collection
  )

  private def FilterExpression : Rule3[ast.Identifier, ast.Expression, Option[ast.Expression]] =
    IdInColl ~~ (keyword("WHERE") ~~ Expression ~~> (Some(_)) | EMPTY ~ push(None))


  private def IdInColl: Rule2[ast.Identifier, ast.Expression] =
    Identifier ~~ keyword("IN") ~~ Expression

  private def FunctionInvocation : Rule1[ast.FunctionInvocation] = rule {
    group(Identifier ~~ "(" ~~
      (keyword("DISTINCT") ~ push(true) | EMPTY ~ push(false)) ~~
      zeroOrMore(Expression, separator = CommaSep) ~~ ")"
    ) ~~> (_.toIndexedSeq) ~>> token ~~> (ast.FunctionInvocation(_, _, _, _))
  }

  private def MaybeNullableProperty : Rule1[ast.Expression] = rule {
    Property ~~ (
        "?" ~>> token ~~> (new ast.Nullable(_: ast.Expression, _) with ast.DefaultTrue)
      | "!" ~>> token ~ !("=") ~~> ast.Nullable
      | EMPTY ~~> ((e: ast.Expression) => e)
    )
  }

  def ListComprehension : Rule1[ast.ListComprehension] = rule("[") {
    group("[" ~~
      FilterExpression ~~
      ("|" ~~ Expression ~~> (Some(_)) | EMPTY ~ push(None)) ~~
    "]") ~>> token ~~> ast.ListComprehension
  }

}
