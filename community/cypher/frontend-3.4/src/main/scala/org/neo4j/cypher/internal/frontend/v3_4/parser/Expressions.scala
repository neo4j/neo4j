/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_4.parser

import org.neo4j.cypher.internal.util.v3_4.InputPosition
import org.neo4j.cypher.internal.v3_4.{expressions => ast}
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
      group(operator("=~") ~~ Expression2) ~~>> (ast.RegexMatch(_: ast.Expression, _))
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
      |  "[" ~~ Expression ~~ "]" ~~>> (ast.ContainerIndex(_: ast.Expression, _))
      | "[" ~~ optional(Expression) ~~ ".." ~~ optional(Expression) ~~ "]" ~~>> (ast.ListSlice(_: ast.Expression, _, _))
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
    | MapProjection
    | ListComprehension
    | PatternComprehension
    | group("[" ~~ zeroOrMore(Expression, separator = CommaSep) ~~ "]") ~~>> (ast.ListLiteral(_))
    | group(keyword("FILTER") ~~ "(" ~~ FilterExpression ~~ ")") ~~>> (ast.FilterExpression(_, _, _))
    | group(keyword("EXTRACT") ~~ "(" ~~ FilterExpression ~ optional(WS ~ "|" ~~ Expression) ~~ ")") ~~>> (ast.ExtractExpression(_, _, _, _))
    | group(keyword("REDUCE") ~~ "(" ~~ Variable ~~ "=" ~~ Expression ~~ "," ~~ IdInColl ~~ "|" ~~ Expression ~~ ")") ~~>> (ast.ReduceExpression(_, _, _, _, _))
    | group(keyword("ALL") ~~ "(" ~~ FilterExpression ~~ ")") ~~>> (ast.AllIterablePredicate(_, _, _))
    | group(keyword("ANY") ~~ "(" ~~ FilterExpression ~~ ")") ~~>> (ast.AnyIterablePredicate(_, _, _))
    | group(keyword("NONE") ~~ "(" ~~ FilterExpression ~~ ")") ~~>> (ast.NoneIterablePredicate(_, _, _))
    | group(keyword("SINGLE") ~~ "(" ~~ FilterExpression ~~ ")") ~~>> (ast.SingleIterablePredicate(_, _, _))
    | ShortestPathPattern ~~> ast.ShortestPathExpression
    | RelationshipsPattern ~~> ast.PatternExpression
    | parenthesizedExpression
    | FunctionInvocation
    | Variable
  )

  def parenthesizedExpression: Rule1[ast.Expression] = "(" ~~ Expression ~~ ")"

  def PropertyExpression: Rule1[ast.Property] = rule {
    Expression1 ~ oneOrMore(WS ~ PropertyLookup)
  }

  def PropertyLookup: ReductionRule1[ast.Expression, ast.Property] = rule("'.'") {
    operator(".") ~~ (PropertyKeyName ~~>> (ast.Property(_: ast.Expression, _)))
  }

  private def FilterExpression: Rule3[ast.Variable, ast.Expression, Option[ast.Expression]] =
    IdInColl ~ optional(WS ~ keyword("WHERE") ~~ Expression)

  private def IdInColl: Rule2[ast.Variable, ast.Expression] =
    Variable ~~ keyword("IN") ~~ Expression

  def FunctionInvocation: Rule1[ast.FunctionInvocation] = rule("a function") {
    ((group(Namespace ~~ FunctionName ~~ "(" ~~
      (keyword("DISTINCT") ~ push(true) | EMPTY ~ push(false)) ~~
      zeroOrMore(Expression, separator = CommaSep) ~~ ")"
    ) ~~> (_.toIndexedSeq)) memoMismatches) ~~>> (ast.FunctionInvocation(_, _, _, _))
  }

  def ListComprehension: Rule1[ast.ListComprehension] = rule("[") {
    group("[" ~~ FilterExpression ~ optional(WS ~ "|" ~~ Expression) ~~ "]") ~~>> (ast.ListComprehension(_, _, _, _))
  }

  def PatternComprehension: Rule1[ast.PatternComprehension] = rule("[") {
    group("[" ~~ optional(Variable ~~ operator("=")) ~~ RelationshipsPattern ~ optional(WS ~ keyword("WHERE") ~~ Expression) ~~ "|" ~~ Expression ~~ "]") ~~>> (ast.PatternComprehension(_, _, _, _))
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
