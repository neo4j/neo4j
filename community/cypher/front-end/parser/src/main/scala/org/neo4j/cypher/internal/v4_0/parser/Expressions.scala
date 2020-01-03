/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.v4_0.parser

import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.util.InputPosition
import org.neo4j.cypher.internal.v4_0.{expressions => ast}
import org.parboiled.scala._

import scala.collection.mutable.ListBuffer

trait Expressions extends Parser
  with Literals
  with Patterns
  with Base {

  // Precedence loosely based on http://en.wikipedia.org/wiki/Operators_in_C_and_C%2B%2B#Operator_precedence

  def Expression = Expression12

  private def Expression12: Rule1[org.neo4j.cypher.internal.v4_0.expressions.Expression] = rule("an expression") {
    Expression11 ~ zeroOrMore(WS ~ (
        group(keyword("OR") ~~ Expression11) ~~>> (Or(_: org.neo4j.cypher.internal.v4_0.expressions.Expression, _))
    ): ReductionRule1[org.neo4j.cypher.internal.v4_0.expressions.Expression, org.neo4j.cypher.internal.v4_0.expressions.Expression])
  }

  private def Expression11: Rule1[org.neo4j.cypher.internal.v4_0.expressions.Expression] = rule("an expression") {
    Expression10 ~ zeroOrMore(WS ~ (
        group(keyword("XOR") ~~ Expression10) ~~>> (ast.Xor(_: org.neo4j.cypher.internal.v4_0.expressions.Expression, _))
    ): ReductionRule1[org.neo4j.cypher.internal.v4_0.expressions.Expression, org.neo4j.cypher.internal.v4_0.expressions.Expression])
  }

  private def Expression10: Rule1[org.neo4j.cypher.internal.v4_0.expressions.Expression] = rule("an expression") {
    Expression9 ~ zeroOrMore(WS ~ (
        group(keyword("AND") ~~ Expression9) ~~>> (And(_: org.neo4j.cypher.internal.v4_0.expressions.Expression, _))
    ): ReductionRule1[org.neo4j.cypher.internal.v4_0.expressions.Expression, org.neo4j.cypher.internal.v4_0.expressions.Expression])
  }

  private def Expression9: Rule1[org.neo4j.cypher.internal.v4_0.expressions.Expression] = rule("an expression") (
      group(keyword("NOT") ~~ Expression9) ~~>> (ast.Not(_))
    | Expression8
  )

  private def Expression8: Rule1[org.neo4j.cypher.internal.v4_0.expressions.Expression] = rule("comparison expression") {
    val produceComparisons: (org.neo4j.cypher.internal.v4_0.expressions.Expression, List[PartialComparison]) => InputPosition => org.neo4j.cypher.internal.v4_0.expressions.Expression = comparisons
    Expression7 ~ zeroOrMore(WS ~ PartialComparisonExpression) ~~>> produceComparisons
  }

  private case class PartialComparison(op: (org.neo4j.cypher.internal.v4_0.expressions.Expression, org.neo4j.cypher.internal.v4_0.expressions.Expression) => InputPosition => org.neo4j.cypher.internal.v4_0.expressions.Expression,
                                       expr: org.neo4j.cypher.internal.v4_0.expressions.Expression, pos: InputPosition) {
    def apply(lhs: org.neo4j.cypher.internal.v4_0.expressions.Expression) = op(lhs, expr)(pos)
  }

  private def PartialComparisonExpression: Rule1[PartialComparison] = (
      group(operator("=") ~~ Expression7) ~~>> { expr: org.neo4j.cypher.internal.v4_0.expressions.Expression => pos: InputPosition => PartialComparison(eq, expr, pos) }
    | group(operator("~") ~~ Expression7) ~~>> { expr: ast.Expression => pos: InputPosition => PartialComparison(eqv, expr, pos) }
    | group(operator("<>") ~~ Expression7) ~~>> { expr: org.neo4j.cypher.internal.v4_0.expressions.Expression => pos: InputPosition => PartialComparison(ne, expr, pos) }
    | group(operator("!=") ~~ Expression7) ~~>> { expr: org.neo4j.cypher.internal.v4_0.expressions.Expression => pos: InputPosition => PartialComparison(bne, expr, pos) }
    | group(operator("<") ~~ Expression7) ~~>> { expr: org.neo4j.cypher.internal.v4_0.expressions.Expression => pos: InputPosition => PartialComparison(lt, expr, pos) }
    | group(operator(">") ~~ Expression7) ~~>> { expr: org.neo4j.cypher.internal.v4_0.expressions.Expression => pos: InputPosition => PartialComparison(gt, expr, pos) }
    | group(operator("<=") ~~ Expression7) ~~>> { expr: org.neo4j.cypher.internal.v4_0.expressions.Expression => pos: InputPosition => PartialComparison(lte, expr, pos) }
    | group(operator(">=") ~~ Expression7) ~~>> { expr: org.neo4j.cypher.internal.v4_0.expressions.Expression => pos: InputPosition => PartialComparison(gte, expr, pos) } )

  private def eq(lhs:org.neo4j.cypher.internal.v4_0.expressions.Expression, rhs:org.neo4j.cypher.internal.v4_0.expressions.Expression): InputPosition => org.neo4j.cypher.internal.v4_0.expressions.Expression = ast.Equals(lhs, rhs)
  private def eqv(lhs:ast.Expression, rhs:ast.Expression): InputPosition => ast.Expression = ast.Equivalent(lhs, rhs)
  private def ne(lhs:org.neo4j.cypher.internal.v4_0.expressions.Expression, rhs:org.neo4j.cypher.internal.v4_0.expressions.Expression): InputPosition => org.neo4j.cypher.internal.v4_0.expressions.Expression = ast.NotEquals(lhs, rhs)
  private def bne(lhs:org.neo4j.cypher.internal.v4_0.expressions.Expression, rhs:org.neo4j.cypher.internal.v4_0.expressions.Expression): InputPosition => org.neo4j.cypher.internal.v4_0.expressions.Expression = ast.InvalidNotEquals(lhs, rhs)
  private def lt(lhs:org.neo4j.cypher.internal.v4_0.expressions.Expression, rhs:org.neo4j.cypher.internal.v4_0.expressions.Expression): InputPosition => org.neo4j.cypher.internal.v4_0.expressions.Expression = ast.LessThan(lhs, rhs)
  private def gt(lhs:org.neo4j.cypher.internal.v4_0.expressions.Expression, rhs:org.neo4j.cypher.internal.v4_0.expressions.Expression): InputPosition => org.neo4j.cypher.internal.v4_0.expressions.Expression = ast.GreaterThan(lhs, rhs)
  private def lte(lhs:org.neo4j.cypher.internal.v4_0.expressions.Expression, rhs:org.neo4j.cypher.internal.v4_0.expressions.Expression): InputPosition => org.neo4j.cypher.internal.v4_0.expressions.Expression = ast.LessThanOrEqual(lhs, rhs)
  private def gte(lhs:org.neo4j.cypher.internal.v4_0.expressions.Expression, rhs:org.neo4j.cypher.internal.v4_0.expressions.Expression): InputPosition => org.neo4j.cypher.internal.v4_0.expressions.Expression = ast.GreaterThanOrEqual(lhs, rhs)

  private def comparisons(first: org.neo4j.cypher.internal.v4_0.expressions.Expression, rest: List[PartialComparison]): InputPosition => org.neo4j.cypher.internal.v4_0.expressions.Expression = {
    rest match {
      case Nil => _ => first
      case second :: Nil => _ => second(first)
      case more =>
        var lhs = first
        val result = ListBuffer.empty[org.neo4j.cypher.internal.v4_0.expressions.Expression]
        for (rhs <- more) {
          result.append(rhs(lhs))
          lhs = rhs.expr
        }
        Ands(Set(result: _*))
    }
  }

  private def Expression7: Rule1[org.neo4j.cypher.internal.v4_0.expressions.Expression] = rule("an expression") {
    Expression6 ~ zeroOrMore(WS ~ (
      group(operator("=~") ~~ Expression6) ~~>> (ast.RegexMatch(_: org.neo4j.cypher.internal.v4_0.expressions.Expression, _))
        | group(keyword("IN") ~~ Expression6) ~~>> (ast.In(_: org.neo4j.cypher.internal.v4_0.expressions.Expression, _))
        | group(keyword("STARTS WITH") ~~ Expression6) ~~>> (ast.StartsWith(_: org.neo4j.cypher.internal.v4_0.expressions.Expression, _))
        | group(keyword("ENDS WITH") ~~ Expression6) ~~>> (ast.EndsWith(_: org.neo4j.cypher.internal.v4_0.expressions.Expression, _))
        | group(keyword("CONTAINS") ~~ Expression6) ~~>> (ast.Contains(_: org.neo4j.cypher.internal.v4_0.expressions.Expression, _))
        | keyword("IS NULL") ~~>> (ast.IsNull(_: org.neo4j.cypher.internal.v4_0.expressions.Expression))
        | keyword("IS NOT NULL") ~~>> (ast.IsNotNull(_: org.neo4j.cypher.internal.v4_0.expressions.Expression))
      ): ReductionRule1[org.neo4j.cypher.internal.v4_0.expressions.Expression, org.neo4j.cypher.internal.v4_0.expressions.Expression])
  }

  private def Expression6: Rule1[org.neo4j.cypher.internal.v4_0.expressions.Expression] = rule("an expression") {
    Expression5 ~ zeroOrMore(WS ~ (
      group(operator("+") ~~ Expression5) ~~>> (ast.Add(_: org.neo4j.cypher.internal.v4_0.expressions.Expression, _))
        | group(operator("-") ~~ Expression5) ~~>> (ast.Subtract(_: org.neo4j.cypher.internal.v4_0.expressions.Expression, _))
      ))
  }

  private def Expression5: Rule1[org.neo4j.cypher.internal.v4_0.expressions.Expression] = rule("an expression") {
    Expression4 ~ zeroOrMore(WS ~ (
      group(operator("*") ~~ Expression4) ~~>> (ast.Multiply(_: org.neo4j.cypher.internal.v4_0.expressions.Expression, _))
        | group(operator("/") ~~ Expression4) ~~>> (ast.Divide(_: org.neo4j.cypher.internal.v4_0.expressions.Expression, _))
        | group(operator("%") ~~ Expression4) ~~>> (ast.Modulo(_: org.neo4j.cypher.internal.v4_0.expressions.Expression, _))
      ))
  }

  private def Expression4: Rule1[org.neo4j.cypher.internal.v4_0.expressions.Expression] = rule("an expression") {
    Expression3 ~ zeroOrMore(WS ~ (
      group(operator("^") ~~ Expression3) ~~>> (ast.Pow(_: org.neo4j.cypher.internal.v4_0.expressions.Expression, _))
      ))
  }

  private def Expression3: Rule1[org.neo4j.cypher.internal.v4_0.expressions.Expression] = rule("an expression") (
      Expression2
    | group(operator("+") ~~ Expression2) ~~>> (ast.UnaryAdd(_))
    | group(operator("-") ~~ Expression2) ~~>> (ast.UnarySubtract(_))
  )

  private def Expression2: Rule1[org.neo4j.cypher.internal.v4_0.expressions.Expression] = rule("an expression") {
    Expression1 ~ zeroOrMore(WS ~ (
        PropertyLookup
      | NodeLabels ~~>> (ast.HasLabels(_: org.neo4j.cypher.internal.v4_0.expressions.Expression, _))
      |  "[" ~~ Expression ~~ "]" ~~>> (ast.ContainerIndex(_: org.neo4j.cypher.internal.v4_0.expressions.Expression, _))
      | "[" ~~ optional(Expression) ~~ ".." ~~ optional(Expression) ~~ "]" ~~>> (ast.ListSlice(_: org.neo4j.cypher.internal.v4_0.expressions.Expression, _, _))
    ))
  }

  private def Expression1: Rule1[org.neo4j.cypher.internal.v4_0.expressions.Expression] = rule("an expression") (
      NumberLiteral
    | StringLiteral
    | Parameter
    | OldParameter
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
    | Exists
    | ShortestPathPattern ~~> ast.ShortestPathExpression
    | RelationshipsPattern ~~> PatternExpression
    | parenthesizedExpression
    | FunctionInvocation
    | Variable
  )

  def parenthesizedExpression: Rule1[org.neo4j.cypher.internal.v4_0.expressions.Expression] = "(" ~~ Expression ~~ ")"

  def PropertyExpression: Rule1[org.neo4j.cypher.internal.v4_0.expressions.Property] = rule {
    Expression1 ~ oneOrMore(WS ~ PropertyLookup)
  }

  def PropertyLookup: ReductionRule1[org.neo4j.cypher.internal.v4_0.expressions.Expression, org.neo4j.cypher.internal.v4_0.expressions.Property] = rule("'.'") {
    operator(".") ~~ (PropertyKeyName ~~>> (ast.Property(_: org.neo4j.cypher.internal.v4_0.expressions.Expression, _)))
  }

  private def ExistsSubClauseExpression: Rule2[Pattern, Option[Expression]] =
    WS ~ optional(keyword("MATCH")) ~~ Pattern ~ optional(WS ~ keyword("WHERE") ~~ Expression) //TODO: Support more stuff here, notably multiple patterns

  private def FilterExpression: Rule3[Variable, org.neo4j.cypher.internal.v4_0.expressions.Expression, Option[org.neo4j.cypher.internal.v4_0.expressions.Expression]] =
    IdInColl ~ optional(WS ~ keyword("WHERE") ~~ Expression)

  private def IdInColl: Rule2[Variable, org.neo4j.cypher.internal.v4_0.expressions.Expression] =
    Variable ~~ keyword("IN") ~~ Expression

  def FunctionInvocation: Rule1[org.neo4j.cypher.internal.v4_0.expressions.FunctionInvocation] = rule("a function") {
    ((group(Namespace ~~ FunctionName ~~ "(" ~~
      (keyword("DISTINCT") ~ push(true) | EMPTY ~ push(false)) ~~
      zeroOrMore(Expression, separator = CommaSep) ~~ ")"
    ) ~~> (_.toIndexedSeq)) memoMismatches) ~~>> (ast.FunctionInvocation(_, _, _, _))
  }

  def ListComprehension: Rule1[org.neo4j.cypher.internal.v4_0.expressions.ListComprehension] = rule("[") {
    group("[" ~~ FilterExpression ~ optional(WS ~ "|" ~~ Expression) ~~ "]") ~~>> (ast.ListComprehension(_, _, _, _))
  }

  def PatternComprehension: Rule1[ast.PatternComprehension] = rule("[") {
    group("[" ~~ optional(Variable ~~ operator("=")) ~~ RelationshipsPattern ~ optional(WS ~ keyword("WHERE") ~~ Expression) ~~ "|" ~~ Expression ~~ "]") ~~>> (
      (a, b, c, d) => pos => ast.PatternComprehension(a, b, c, d)(pos, Set.empty))
  }

  def Exists: Rule1[ExistsSubClause] =
    group(keyword("EXISTS") ~~ "{" ~~ ExistsSubClauseExpression ~~ "}") ~~>> (
      (a, b) => pos => ast.ExistsSubClause(a, b)(pos, Set.empty)) //TODO: This should NOT be a mere expression!


  def CaseExpression: Rule1[org.neo4j.cypher.internal.v4_0.expressions.CaseExpression] = rule("CASE") {
    (group((
        keyword("CASE") ~~ push(None) ~ oneOrMore(WS ~ CaseAlternatives)
      | keyword("CASE") ~~ Expression ~~> (Some(_)) ~ oneOrMore(WS ~ CaseAlternatives)
      ) ~ optional(WS ~
        keyword("ELSE") ~~ Expression
      ) ~~ keyword("END")
    ) memoMismatches) ~~>> (ast.CaseExpression(_, _, _))
  }

  private def CaseAlternatives: Rule2[org.neo4j.cypher.internal.v4_0.expressions.Expression, org.neo4j.cypher.internal.v4_0.expressions.Expression] = rule("WHEN") {
    keyword("WHEN") ~~ Expression ~~ keyword("THEN") ~~ Expression
  }
}
