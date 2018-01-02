/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.v3_4.expressions

trait OperatorExpression {
  self: Expression =>

  def signatures: Seq[TypeSignature] = Seq.empty

  def canonicalOperatorSymbol: String = self.productPrefix.toUpperCase
}

trait LeftUnaryOperatorExpression extends OperatorExpression {
  self: Expression =>

  def rhs: Expression

  override def asCanonicalStringVal: String = s"$canonicalOperatorSymbol(${rhs.asCanonicalStringVal})"
}

trait RightUnaryOperatorExpression extends OperatorExpression {
  self: Expression =>

  def lhs: Expression
  override def asCanonicalStringVal: String = s"$canonicalOperatorSymbol(${lhs.asCanonicalStringVal})"
}

trait BinaryOperatorExpression extends OperatorExpression {
  self: Expression =>

  def lhs: Expression
  def rhs: Expression

  override def asCanonicalStringVal: String = {
    s"${lhs.asCanonicalStringVal} $canonicalOperatorSymbol ${rhs.asCanonicalStringVal}"
  }
}

trait MultiOperatorExpression  extends OperatorExpression {
  self: Expression =>

  def exprs: Set[Expression]

  override def asCanonicalStringVal: String = s"$canonicalOperatorSymbol( ${exprs.map(_.asCanonicalStringVal).mkString(", ")}"
}

