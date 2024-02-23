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
package org.neo4j.cypher.internal.expressions

import org.neo4j.cypher.internal.util.collection.immutable.ListSet

import java.util.Locale

trait OperatorExpression {
  self: Expression =>

  def signatures: Seq[ExpressionTypeSignature] = Seq.empty

  def canonicalOperatorSymbol: String = self.productPrefix.toUpperCase(Locale.ROOT)
}

/**
 * expression that is built up like `<OPERATOR> <rhs>`
 */
trait LeftUnaryOperatorExpression extends OperatorExpression {
  self: Expression =>

  def rhs: Expression

  override def asCanonicalStringVal: String = s"$canonicalOperatorSymbol(${rhs.asCanonicalStringVal})"

  override def isConstantForQuery: Boolean = rhs.isConstantForQuery
}

/**
 * expression that is built up like `<lhs> <OPERATOR>`
 */
trait RightUnaryOperatorExpression extends OperatorExpression {
  self: Expression =>

  def lhs: Expression
  override def asCanonicalStringVal: String = s"$canonicalOperatorSymbol(${lhs.asCanonicalStringVal})"

  override def isConstantForQuery: Boolean = lhs.isConstantForQuery
}

/**
 * expression that is built up like `<lhs> <OPERATOR> <rhs>`
 */
trait BinaryOperatorExpression extends OperatorExpression {
  self: Expression =>

  def lhs: Expression
  def rhs: Expression

  override def asCanonicalStringVal: String = {
    s"${lhs.asCanonicalStringVal} $canonicalOperatorSymbol ${rhs.asCanonicalStringVal}"
  }

  override def isConstantForQuery: Boolean = lhs.isConstantForQuery && rhs.isConstantForQuery
}

/**
 * An expression that can be chained, like for example `1 < x < 5`.
 */
trait ChainableBinaryOperatorExpression extends BinaryOperatorExpression {
  self: Expression =>
}

trait MultiOperatorExpression extends OperatorExpression {
  self: Expression =>

  def exprs: ListSet[Expression]

  override def asCanonicalStringVal: String =
    s"$canonicalOperatorSymbol( ${exprs.map(_.asCanonicalStringVal).mkString(", ")}"

  override def isConstantForQuery: Boolean = exprs.forall(_.isConstantForQuery)
}
