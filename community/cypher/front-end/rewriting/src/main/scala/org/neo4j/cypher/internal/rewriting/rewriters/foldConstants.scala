/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.BinaryOperatorExpression
import org.neo4j.cypher.internal.expressions.DecimalDoubleLiteral
import org.neo4j.cypher.internal.expressions.Divide
import org.neo4j.cypher.internal.expressions.DoubleLiteral
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.False
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.IntegerLiteral
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.Modulo
import org.neo4j.cypher.internal.expressions.Multiply
import org.neo4j.cypher.internal.expressions.NumberLiteral
import org.neo4j.cypher.internal.expressions.Pow
import org.neo4j.cypher.internal.expressions.SensitiveLiteral
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.SignedIntegerLiteral
import org.neo4j.cypher.internal.expressions.Subtract
import org.neo4j.cypher.internal.expressions.True
import org.neo4j.cypher.internal.expressions.UnaryAdd
import org.neo4j.cypher.internal.expressions.UnarySubtract
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.rewriters.factories.ASTRewriterFactory
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo

case class foldConstants(cypherExceptionFactory: CypherExceptionFactory) extends Rewriter {

  override def apply(that: AnyRef): AnyRef =
    instance.apply(that)

  private def containsSensitive(e: BinaryOperatorExpression): Boolean = (e.lhs, e.rhs) match {
    case (_: SensitiveLiteral, _) => true
    case (_, _: SensitiveLiteral) => true
    case _                        => false
  }

  private val instance: Rewriter = bottomUp(tryRewrite)

  private def tryRewrite(expr: AnyRef): AnyRef =
    try {
      expr match {
        case e: BinaryOperatorExpression if containsSensitive(e) => e

        case e @ Add(lhs: SignedIntegerLiteral, rhs: SignedIntegerLiteral) =>
          SignedDecimalIntegerLiteral((lhs.value + rhs.value).toString)(e.position)
        case e @ Add(lhs: DecimalDoubleLiteral, rhs: SignedIntegerLiteral) =>
          DecimalDoubleLiteral((lhs.value + rhs.value).toString)(e.position)
        case e @ Add(lhs: SignedIntegerLiteral, rhs: DecimalDoubleLiteral) =>
          DecimalDoubleLiteral((lhs.value + rhs.value).toString)(e.position)
        case e @ Add(lhs: DecimalDoubleLiteral, rhs: DecimalDoubleLiteral) =>
          DecimalDoubleLiteral((lhs.value + rhs.value).toString)(e.position)

        case e @ Subtract(lhs: SignedIntegerLiteral, rhs: SignedIntegerLiteral) =>
          SignedDecimalIntegerLiteral((lhs.value - rhs.value).toString)(e.position)
        case e @ Subtract(lhs: DecimalDoubleLiteral, rhs: SignedIntegerLiteral) =>
          DecimalDoubleLiteral((lhs.value - rhs.value).toString)(e.position)
        case e @ Subtract(lhs: SignedIntegerLiteral, rhs: DecimalDoubleLiteral) =>
          DecimalDoubleLiteral((lhs.value - rhs.value).toString)(e.position)
        case e @ Subtract(lhs: DecimalDoubleLiteral, rhs: DecimalDoubleLiteral) =>
          DecimalDoubleLiteral((lhs.value - rhs.value).toString)(e.position)

        case e @ Multiply(lhs: SignedIntegerLiteral, rhs: SignedIntegerLiteral) =>
          SignedDecimalIntegerLiteral((lhs.value * rhs.value).toString)(e.position)
        case e @ Multiply(lhs: DecimalDoubleLiteral, rhs: SignedIntegerLiteral) =>
          DecimalDoubleLiteral((lhs.value * rhs.value).toString)(e.position)
        case e @ Multiply(lhs: SignedIntegerLiteral, rhs: DecimalDoubleLiteral) =>
          DecimalDoubleLiteral((lhs.value * rhs.value).toString)(e.position)
        case e @ Multiply(lhs: DecimalDoubleLiteral, rhs: DecimalDoubleLiteral) =>
          DecimalDoubleLiteral((lhs.value * rhs.value).toString)(e.position)

        case e @ Multiply(_: NumberLiteral, _: NumberLiteral) =>
          e
        case e @ Multiply(lhs: NumberLiteral, rhs) =>
          Multiply(rhs, lhs)(e.position).rewrite(instance)
        case e @ Multiply(lhs @ Multiply(innerLhs, innerRhs: NumberLiteral), rhs: NumberLiteral) =>
          Multiply(Multiply(innerRhs, rhs)(lhs.position), innerLhs)(e.position).rewrite(instance)
        case e @ Multiply(lhs @ Multiply(innerLhs: NumberLiteral, innerRhs), rhs: NumberLiteral) =>
          Multiply(Multiply(innerLhs, rhs)(lhs.position), innerRhs)(e.position).rewrite(instance)

        case e @ Divide(lhs: SignedIntegerLiteral, rhs: SignedIntegerLiteral) =>
          SignedDecimalIntegerLiteral((lhs.value / rhs.value).toString)(e.position)
        case e @ Divide(lhs: DecimalDoubleLiteral, rhs: SignedIntegerLiteral) =>
          DecimalDoubleLiteral((lhs.value / rhs.value).toString)(e.position)
        case e @ Divide(lhs: SignedIntegerLiteral, rhs: DecimalDoubleLiteral) =>
          DecimalDoubleLiteral((lhs.value / rhs.value).toString)(e.position)
        case e @ Divide(lhs: DecimalDoubleLiteral, rhs: DecimalDoubleLiteral) =>
          DecimalDoubleLiteral((lhs.value / rhs.value).toString)(e.position)

        case e @ Modulo(lhs: SignedIntegerLiteral, rhs: SignedIntegerLiteral) =>
          SignedDecimalIntegerLiteral((lhs.value % rhs.value).toString)(e.position)
        case e @ Modulo(lhs: DecimalDoubleLiteral, rhs: SignedIntegerLiteral) =>
          DecimalDoubleLiteral((lhs.value % rhs.value).toString)(e.position)
        case e @ Modulo(lhs: SignedIntegerLiteral, rhs: DecimalDoubleLiteral) =>
          DecimalDoubleLiteral((lhs.value % rhs.value).toString)(e.position)
        case e @ Modulo(lhs: DecimalDoubleLiteral, rhs: DecimalDoubleLiteral) =>
          DecimalDoubleLiteral((lhs.value % rhs.value).toString)(e.position)

        case e @ Pow(lhs: SignedIntegerLiteral, rhs: SignedIntegerLiteral) =>
          DecimalDoubleLiteral(Math.pow(lhs.value.toDouble, rhs.value.toDouble).toString)(e.position)
        case e @ Pow(lhs: DecimalDoubleLiteral, rhs: SignedIntegerLiteral) =>
          DecimalDoubleLiteral(Math.pow(lhs.value, rhs.value.toDouble).toString)(e.position)
        case e @ Pow(lhs: SignedIntegerLiteral, rhs: DecimalDoubleLiteral) =>
          DecimalDoubleLiteral(Math.pow(lhs.value.toDouble, rhs.value).toString)(e.position)
        case e @ Pow(lhs: DecimalDoubleLiteral, rhs: DecimalDoubleLiteral) =>
          DecimalDoubleLiteral(Math.pow(lhs.value, rhs.value).toString)(e.position)

        case e: UnaryAdd =>
          e.rhs

        case e @ UnarySubtract(_: SensitiveLiteral) => e
        case e @ UnarySubtract(rhs: SignedIntegerLiteral) =>
          SignedDecimalIntegerLiteral((-rhs.value).toString)(e.position)
        case e: UnarySubtract =>
          Subtract(SignedDecimalIntegerLiteral("0")(e.position), e.rhs)(e.position)

        case e @ Equals(lhs: IntegerLiteral, rhs: IntegerLiteral) => asAst(lhs.value == rhs.value, e)
        case e @ Equals(lhs: DoubleLiteral, rhs: DoubleLiteral)   => asAst(lhs.value == rhs.value, e)
        case e @ Equals(lhs: IntegerLiteral, rhs: DoubleLiteral)  => asAst(lhs.value.doubleValue() == rhs.value, e)
        case e @ Equals(lhs: DoubleLiteral, rhs: IntegerLiteral)  => asAst(lhs.value == rhs.value.doubleValue(), e)

        case e @ LessThan(lhs: IntegerLiteral, rhs: IntegerLiteral) => asAst(lhs.value < rhs.value, e)
        case e @ LessThan(lhs: DoubleLiteral, rhs: DoubleLiteral)   => asAst(lhs.value < rhs.value, e)
        case e @ LessThan(lhs: IntegerLiteral, rhs: DoubleLiteral)  => asAst(lhs.value.doubleValue() < rhs.value, e)
        case e @ LessThan(lhs: DoubleLiteral, rhs: IntegerLiteral)  => asAst(lhs.value < rhs.value.doubleValue(), e)

        case e @ GreaterThan(lhs: IntegerLiteral, rhs: IntegerLiteral) => asAst(lhs.value > rhs.value, e)
        case e @ GreaterThan(lhs: DoubleLiteral, rhs: DoubleLiteral)   => asAst(lhs.value > rhs.value, e)
        case e @ GreaterThan(lhs: IntegerLiteral, rhs: DoubleLiteral)  => asAst(lhs.value.doubleValue() > rhs.value, e)
        case e @ GreaterThan(lhs: DoubleLiteral, rhs: IntegerLiteral)  => asAst(lhs.value > rhs.value.doubleValue(), e)

        case e => e
      }
    } catch {
      // Don't fail planning because we failed to compute this. Rather leave the expression unchanged.
      // If the expression is never evaluated, then failing here would be wrong.
      case _: java.lang.ArithmeticException => expr
    }

  private def asAst(b: Boolean, e: Expression) = if (b) True()(e.position) else False()(e.position)
}

case object foldConstants extends StepSequencer.Step with DefaultPostCondition with ASTRewriterFactory {
  override def preConditions: Set[StepSequencer.Condition] = Set()

  override def invalidatedConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable

  override def getRewriter(
    semanticState: SemanticState,
    parameterTypeMapping: Map[String, ParameterTypeInfo],
    cypherExceptionFactory: CypherExceptionFactory,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    cancellationChecker: CancellationChecker
  ): Rewriter = foldConstants(cypherExceptionFactory)
}
