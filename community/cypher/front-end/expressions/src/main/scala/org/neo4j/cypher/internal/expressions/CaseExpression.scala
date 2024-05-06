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

import org.neo4j.cypher.internal.util.InputPosition

case class CaseExpression(
  expression: Option[Expression],
  alternatives: IndexedSeq[(Expression, Expression)],
  default: Option[Expression]
)(val position: InputPosition) extends Expression {

  lazy val possibleExpressions: IndexedSeq[Expression] = alternatives.map(_._2) ++ default

  override def isConstantForQuery: Boolean =
    expression.forall(_.isConstantForQuery) &&
      alternatives.forall(t => t._1.isConstantForQuery && t._2.isConstantForQuery) &&
      default.forall(_.isConstantForQuery)

  def withDefault(default: Expression): CaseExpression =
    copy(default = Some(default))(position)
}

object CaseExpression {

  def apply(
    expression: Option[Expression],
    alternatives: List[(Expression, Expression)],
    default: Option[Expression]
  )(position: InputPosition): CaseExpression =
    CaseExpression(expression, alternatives.toIndexedSeq, default)(position)
}
