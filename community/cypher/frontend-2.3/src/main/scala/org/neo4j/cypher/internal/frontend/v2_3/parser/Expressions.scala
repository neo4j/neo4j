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

import org.neo4j.cypher.internal.frontend.v2_3.{InputPosition, ast}
import org.parboiled.scala._

import scala.collection.mutable.ListBuffer

trait Expressions extends Parser
  with Literals
  with Patterns
  with Base {

  // Precedence loosely based on http://en.wikipedia.org/wiki/Operators_in_C_and_C%2B%2B#Operator_precedence

  def Expression = Expression12

  private def Expression12: Rule1[ast.Expression] = rule("an expression") {
    Expression11 ~ zeroOrMore(WS ~ (
        group(keyword("OR") ~~ Expression11) ~~>> (ast.Or(_: ast.Expression, _))
    ): ReductionRule1[ast.Expression, ast.Expression])
  }

  private def Expression11: Rule1[ast.Expression] = rule("an expression") {
    Expression10 ~ zeroOrMore(WS ~ (
        group(keyword("XOR") ~~ Expression10) ~~>> (ast.Xor(_: ast.Expression, _))
    ): ReductionRule1[ast.Expression, ast.Expression])
  }

  private def Expression10: Rule1[ast.Expression] = rule("an expression") {
    Expression9 ~ zeroOrMore(WS ~ (
        group(keyword("AND") ~~ Expression9) ~~>> (ast.And(_: ast.Expression, _))
    ): ReductionRule1[ast.Expression, ast.Expression])
  }

  private def Expression9: Rule1[ast.Expression] = rule("an expression") (
      group(keyword("NOT") ~~ Expression9) ~~>> (ast.Not(_))
    | Expression8
  )

  private def Expression8: Rule1[ast.Expression] = rule("comparison expression") {
    val produceComparisons: (ast.Expression, List[PartialComparison]) => InputPosition => ast.Expression = comparisons
    Expression7 ~ zeroOrMore(WS ~ PartialComparisonExpression) ~~>> produceComparisons
  }

  private case class PartialComparison(op: (ast.Expression, ast.Expression) => (InputPosition) => ast.Expression,
                                       expr: ast.Expression, pos: InputPosition) {
    def apply(lhs: ast.Expression) = op(lhs, expr)(pos)
  }

  private def PartialComparisonExpression: Rule1[PartialComparison] = (
      group(operator("=") ~~ Expression7) ~~>> { expr: ast.Expression => pos: InputPosition => PartialComparison(eq, expr, pos) }
    | group(operator("<>") ~~ Expression7) ~~>> { expr: ast.Expression => pos: InputPosition => PartialComparison(ne, expr, pos) }
    | group(operator("!=") ~~ Expression7) ~~>> { expr: ast.Expression => pos: InputPosition => PartialComparison(bne, expr, pos) }
    | group(operator("<") ~~ Expression7) ~~>> { expr: ast.Expression => pos: InputPosition => PartialComparison(lt, expr, pos) }
    | group(operator(">") ~~ Expression7) ~~>> { expr: ast.Expression => pos: InputPosition => PartialComparison(gt, expr, pos) }
    | group(operator("<=") ~~ Expression7) ~~>> { expr: ast.Expression => pos: InputPosition => PartialComparison(lte, expr, pos) }
    | group(operator(">=") ~~ Expression7) ~~>> { expr: ast.Expression => pos: InputPosition => PartialComparison(gte, expr, pos) } )

  private def eq(lhs:ast.Expression, rhs:ast.Expression): InputPosition => ast.Expression = ast.Equals(lhs, rhs)
  private def ne(lhs:ast.Expression, rhs:ast.Expression): InputPosition => ast.Expression = ast.NotEquals(lhs, rhs)
  private def bne(lhs:ast.Expression, rhs:ast.Expression): InputPosition => ast.Expression = ast.InvalidNotEquals(lhs, rhs)
  private def lt(lhs:ast.Expression, rhs:ast.Expression): InputPosition => ast.Expression = ast.LessThan(lhs, rhs)
  private def gt(lhs:ast.Expression, rhs:ast.Expression): InputPosition => ast.Expression = ast.GreaterThan(lhs, rhs)
  private def lte(lhs:ast.Expression, rhs:ast.Expression): InputPosition => ast.Expression = ast.LessThanOrEqual(lhs, rhs)
  private def gte(lhs:ast.Expression, rhs:ast.Expression): InputPosition => ast.Expression = ast.GreaterThanOrEqual(lhs, rhs)

  private def comparisons(first: ast.Expression, rest: List[PartialComparison]): InputPosition => ast.Expression = {
    rest match {
      case Nil => _ => first
      case second :: Nil => _ => second(first)
      case more =>
        var lhs = first
        val result = ListBuffer.empty[ast.Expression]
        for (rhs <- more) {
          result.append(rhs(lhs))
          lhs = rhs.expr
        }
        ast.Ands(Set(result: _*))
    }
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
        "[" ~~ Expression ~~ "]" ~~>> (ast.ContainerIndex(_: ast.Expression, _))
      | "[" ~~ optional(Expression) ~~ ".." ~~ optional(Expression) ~~ "]" ~~>> (ast.CollectionSlice(_: ast.Expression, _, _))
      | group(operator("=~") ~~ Expression2) ~~>> (ast.RegexMatch(_: ast.Expression, _))
      | group(keyword("IN") ~~ Expression2) ~~>> (ast.In(_: ast.Expression, _))
      | group(keyword("STARTS WITH") ~~ Expression2) ~~>> (ast.StartsWith(_: ast.Expression, _))
      | group(keyword("ENDS WITH") ~~ Expression2) ~~>> (ast.EndsWith(_: ast.Expression, _))
      | group(keyword("CONTAINS") ~~ Expression2) ~~>> (ast.Contains(_: ast.Expression, _))
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
