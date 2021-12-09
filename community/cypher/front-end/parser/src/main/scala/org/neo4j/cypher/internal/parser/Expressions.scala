/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.parser

import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.ExistsSubClause
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.InputPosition
import org.parboiled.scala.EMPTY
import org.parboiled.scala.Parser
import org.parboiled.scala.ReductionRule1
import org.parboiled.scala.Rule1
import org.parboiled.scala.Rule2
import org.parboiled.scala.Rule3
import org.parboiled.scala.group

import scala.collection.mutable.ListBuffer
import scala.language.postfixOps

trait Expressions extends Parser
  with Literals
  with Patterns
  with Base {

  // Precedence loosely based on http://en.wikipedia.org/wiki/Operators_in_C_and_C%2B%2B#Operator_precedence

  def Expression = Expression12

  private def Expression12: Rule1[expressions.Expression] = rule("an expression") {
    Expression11 ~ zeroOrMore(WS ~ (
        group(keyword("OR") ~~ Expression11) ~~>> (Or(_: expressions.Expression, _))
    ): ReductionRule1[expressions.Expression, expressions.Expression])
  }

  private def Expression11: Rule1[expressions.Expression] = rule("an expression") {
    Expression10 ~ zeroOrMore(WS ~ (
        group(keyword("XOR") ~~ Expression10) ~~>> (expressions.Xor(_: expressions.Expression, _))
    ): ReductionRule1[expressions.Expression, expressions.Expression])
  }

  private def Expression10: Rule1[expressions.Expression] = rule("an expression") {
    Expression9 ~ zeroOrMore(WS ~ (
        group(keyword("AND") ~~ Expression9) ~~>> (And(_: expressions.Expression, _))
    ): ReductionRule1[expressions.Expression, expressions.Expression])
  }

  private def Expression9: Rule1[expressions.Expression] = rule("an expression") (
      group(keyword("NOT") ~~ Expression9) ~~>> (expressions.Not(_))
    | Expression8
  )

  private def Expression8: Rule1[expressions.Expression] = rule("comparison expression") {
    val produceComparisons: (expressions.Expression, List[PartialComparison]) => InputPosition => expressions.Expression = comparisons
    Expression7 ~ zeroOrMore(WS ~ PartialComparisonExpression) ~~>> produceComparisons
  }

  private case class PartialComparison(op: (expressions.Expression, expressions.Expression) => InputPosition => expressions.Expression,
                                       expr: expressions.Expression, pos: InputPosition) {
    def apply(lhs: expressions.Expression) = op(lhs, expr)(pos)
  }

  private def PartialComparisonExpression: Rule1[PartialComparison] = (
      group(operator("=") ~~ Expression7) ~~>> { expr: expressions.Expression => pos: InputPosition => PartialComparison(eq, expr, pos) }
    | group(operator("<>") ~~ Expression7) ~~>> { expr: expressions.Expression => pos: InputPosition => PartialComparison(ne, expr, pos) }
    | group(operator("!=") ~~ Expression7) ~~>> { expr: expressions.Expression => pos: InputPosition => PartialComparison(bne, expr, pos) }
    | group(operator("<") ~~ Expression7) ~~>> { expr: expressions.Expression => pos: InputPosition => PartialComparison(lt, expr, pos) }
    | group(operator(">") ~~ Expression7) ~~>> { expr: expressions.Expression => pos: InputPosition => PartialComparison(gt, expr, pos) }
    | group(operator("<=") ~~ Expression7) ~~>> { expr: expressions.Expression => pos: InputPosition => PartialComparison(lte, expr, pos) }
    | group(operator(">=") ~~ Expression7) ~~>> { expr: expressions.Expression => pos: InputPosition => PartialComparison(gte, expr, pos) } )

  private def eq(lhs:expressions.Expression, rhs:expressions.Expression): InputPosition => expressions.Expression = expressions.Equals(lhs, rhs)
  private def ne(lhs:expressions.Expression, rhs:expressions.Expression): InputPosition => expressions.Expression = expressions.NotEquals(lhs, rhs)
  private def bne(lhs:expressions.Expression, rhs:expressions.Expression): InputPosition => expressions.Expression = expressions.InvalidNotEquals(lhs, rhs)
  private def lt(lhs:expressions.Expression, rhs:expressions.Expression): InputPosition => expressions.Expression = expressions.LessThan(lhs, rhs)
  private def gt(lhs:expressions.Expression, rhs:expressions.Expression): InputPosition => expressions.Expression = expressions.GreaterThan(lhs, rhs)
  private def lte(lhs:expressions.Expression, rhs:expressions.Expression): InputPosition => expressions.Expression = expressions.LessThanOrEqual(lhs, rhs)
  private def gte(lhs:expressions.Expression, rhs:expressions.Expression): InputPosition => expressions.Expression = expressions.GreaterThanOrEqual(lhs, rhs)

  private def comparisons(first: expressions.Expression, rest: List[PartialComparison]): InputPosition => expressions.Expression = {
    rest match {
      case Nil => _ => first
      case second :: Nil => _ => second(first)
      case more =>
        var lhs = first
        val result = ListBuffer.empty[expressions.Expression]
        for (rhs <- more) {
          result.append(rhs(lhs))
          lhs = rhs.expr
        }
        Ands(result)
    }
  }

  private def Expression7: Rule1[expressions.Expression] = rule("an expression") {
    Expression6 ~ zeroOrMore(WS ~ (
      group(operator("=~") ~~ Expression6) ~~>> (expressions.RegexMatch(_: expressions.Expression, _))
        | group(keyword("IN") ~~ Expression6) ~~>> (expressions.In(_: expressions.Expression, _))
        | group(keyword("STARTS WITH") ~~ Expression6) ~~>> (expressions.StartsWith(_: expressions.Expression, _))
        | group(keyword("ENDS WITH") ~~ Expression6) ~~>> (expressions.EndsWith(_: expressions.Expression, _))
        | group(keyword("CONTAINS") ~~ Expression6) ~~>> (expressions.Contains(_: expressions.Expression, _))
        | keyword("IS NULL") ~~>> (expressions.IsNull(_: expressions.Expression))
        | keyword("IS NOT NULL") ~~>> (expressions.IsNotNull(_: expressions.Expression))
      ): ReductionRule1[expressions.Expression, expressions.Expression])
  }

  private def Expression6: Rule1[expressions.Expression] = rule("an expression") {
    Expression5 ~ zeroOrMore(WS ~ (
      group(operator("+") ~~ Expression5) ~~>> (expressions.Add(_: expressions.Expression, _))
        | group(operator("-") ~~ Expression5) ~~>> (expressions.Subtract(_: expressions.Expression, _))
      ))
  }

  private def Expression5: Rule1[expressions.Expression] = rule("an expression") {
    Expression4 ~ zeroOrMore(WS ~ (
      group(operator("*") ~~ Expression4) ~~>> (expressions.Multiply(_: expressions.Expression, _))
        | group(operator("/") ~~ Expression4) ~~>> (expressions.Divide(_: expressions.Expression, _))
        | group(operator("%") ~~ Expression4) ~~>> (expressions.Modulo(_: expressions.Expression, _))
      ))
  }

  private def Expression4: Rule1[expressions.Expression] = rule("an expression") {
    Expression3 ~ zeroOrMore(WS ~ (
      group(operator("^") ~~ Expression3) ~~>> (expressions.Pow(_: expressions.Expression, _))
      ))
  }

  private def Expression3: Rule1[expressions.Expression] = rule("an expression") (
      Expression2
    | group(operator("+") ~~ Expression2) ~~>> (expressions.UnaryAdd(_))
    | group(operator("-") ~~ Expression2) ~~>> (expressions.UnarySubtract(_))
  )

  private def Expression2: Rule1[expressions.Expression] = rule("an expression") {
    Expression1 ~ zeroOrMore(WS ~ (
        PropertyLookup
      | NodeLabelsOrRelTypes ~~>> (expressions.HasLabelsOrTypes(_: expressions.Expression, _))
      |  "[" ~~ Expression ~~ "]" ~~>> (expressions.ContainerIndex(_: expressions.Expression, _))
      | "[" ~~ optional(Expression) ~~ ".." ~~ optional(Expression) ~~ "]" ~~>> (expressions.ListSlice(_: expressions.Expression, _, _))
    ))
  }

  private def Expression1: Rule1[expressions.Expression] = rule("an expression") (
      NumberLiteral
    | StringLiteral
    | Parameter
    | keyword("TRUE") ~ push(expressions.True()(_))
    | keyword("FALSE") ~ push(expressions.False()(_))
    | keyword("NULL") ~ push(expressions.Null()(_))
    | CaseExpression
    | group(keyword("COUNT") ~~ "(" ~~ "*" ~~ ")") ~ push(expressions.CountStar()(_))
    | MapLiteral
    | MapProjection
    | ListComprehension
    | PatternComprehension
    | group("[" ~~ zeroOrMore(Expression, separator = CommaSep) ~~ "]") ~~>> (expressions.ListLiteral(_))
    | group(keyword("REDUCE") ~~ "(" ~~ Variable ~~ "=" ~~ Expression ~~ "," ~~ IdInColl ~~ "|" ~~ Expression ~~ ")") ~~>> (expressions.ReduceExpression(_, _, _, _, _))
    | group(keyword("ALL") ~~ "(" ~~ FilterExpression ~~ ")") ~~>> (expressions.AllIterablePredicate(_, _, _))
    | group(keyword("ANY") ~~ "(" ~~ FilterExpression ~~ ")") ~~>> (expressions.AnyIterablePredicate(_, _, _))
    | group(keyword("NONE") ~~ "(" ~~ FilterExpression ~~ ")") ~~>> (expressions.NoneIterablePredicate(_, _, _))
    | group(keyword("SINGLE") ~~ "(" ~~ FilterExpression ~~ ")") ~~>> (expressions.SingleIterablePredicate(_, _, _))
    | Exists
    | ShortestPathPattern ~~> expressions.ShortestPathExpression
    | RelationshipsPattern ~~> (PatternExpression(_)(Set.empty, "", ""))
    | parenthesizedExpression
    | FunctionInvocation
    | Variable
  )

  def parenthesizedExpression: Rule1[expressions.Expression] = "(" ~~ Expression ~~ ")"

  def PropertyExpression: Rule1[expressions.Property] = rule {
    Expression1 ~ oneOrMore(WS ~ PropertyLookup)
  }

  def PropertyLookup: ReductionRule1[expressions.Expression, expressions.Property] = rule("'.'") {
    operator(".") ~~ (PropertyKeyName ~~>> (expressions.Property(_: expressions.Expression, _)))
  }

  private def ExistsSubClauseExpression: Rule2[Pattern, Option[Expression]] =
    WS ~ optional(keyword("MATCH")) ~~ Pattern ~ optional(WS ~ keyword("WHERE") ~~ Expression) //TODO: Support more stuff here, notably multiple patterns

  private def FilterExpression: Rule3[Variable, expressions.Expression, Option[expressions.Expression]] =
    IdInColl ~ optional(WS ~ keyword("WHERE") ~~ Expression)

  private def IdInColl: Rule2[Variable, expressions.Expression] =
    Variable ~~ keyword("IN") ~~ Expression

  def FunctionInvocation: Rule1[expressions.FunctionInvocation] = rule("a function") {
    ((group(Namespace ~~ FunctionName ~~ "(" ~~
      (keyword("DISTINCT") ~ push(true) | EMPTY ~ push(false)) ~~
      zeroOrMore(Expression, separator = CommaSep) ~~ ")"
    ) ~~> (_.toIndexedSeq)) memoMismatches) ~~>> (expressions.FunctionInvocation(_, _, _, _))
  }

  def ListComprehension: Rule1[expressions.ListComprehension] = rule("[") {
    group("[" ~~ FilterExpression ~ optional(WS ~ "|" ~~ Expression) ~~ "]") ~~>> (expressions.ListComprehension(_, _, _, _))
  }

  def PatternComprehension: Rule1[expressions.PatternComprehension] = rule("[") {
    group("[" ~~ optional(Variable ~~ operator("=")) ~~ RelationshipsPattern ~ optional(WS ~ keyword("WHERE") ~~ Expression) ~~ "|" ~~ Expression ~~ "]") ~~>> (
      (a, b, c, d) => pos => expressions.PatternComprehension(a, b, c, d)(pos, Set.empty, "", ""))
  }

  def Exists: Rule1[ExistsSubClause] =
    group(keyword("EXISTS") ~~ "{" ~~ ExistsSubClauseExpression ~~ "}") ~~>> (
      (a, b) => pos => expressions.ExistsSubClause(a, b)(pos, Set.empty)) //TODO: This should NOT be a mere expression!


  def CaseExpression: Rule1[expressions.CaseExpression] = rule("CASE") {
    (group((
        keyword("CASE") ~~ push(None) ~ oneOrMore(WS ~ CaseAlternatives)
      | keyword("CASE") ~~ Expression ~~> (Some(_)) ~ oneOrMore(WS ~ CaseAlternatives)
      ) ~ optional(WS ~
        keyword("ELSE") ~~ Expression
      ) ~~ keyword("END")
    ) memoMismatches) ~~>> (expressions.CaseExpression(_, _, _))
  }

  private def CaseAlternatives: Rule2[expressions.Expression, expressions.Expression] = rule("WHEN") {
    keyword("WHEN") ~~ Expression ~~ keyword("THEN") ~~ Expression
  }
}
