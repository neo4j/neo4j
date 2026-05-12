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

sealed trait VectorFilterExpression {
  val propertyName: String
}

object VectorFilterExpression {
  sealed trait SeekRangeVectorFilterExpression extends VectorFilterExpression
  sealed trait LowerBoundVectorFilterExpression extends SeekRangeVectorFilterExpression
  sealed trait UpperBoundVectorFilterExpression extends SeekRangeVectorFilterExpression

  case class VectorFilterGreaterThan(
    propertyName: String,
    rhs: Expression
  ) extends LowerBoundVectorFilterExpression

  case class VectorFilterGreaterThanOrEqual(
    propertyName: String,
    rhs: Expression
  ) extends LowerBoundVectorFilterExpression

  case class VectorFilterLessThan(
    propertyName: String,
    rhs: Expression
  ) extends UpperBoundVectorFilterExpression

  case class VectorFilterLessThanOrEqual(
    propertyName: String,
    rhs: Expression
  ) extends UpperBoundVectorFilterExpression

  case class Equality(
    propertyName: String,
    rhs: Expression
  ) extends VectorFilterExpression

  case class Exists(
    propertyName: String
  ) extends VectorFilterExpression

  case class NotExists(
    propertyName: String
  ) extends VectorFilterExpression

  case class InSet(
    propertyName: String,
    rhs: Expression
  ) extends VectorFilterExpression

  def unapply(expression: Expression): Option[(LogicalVariable, Expression, VectorFilterExpression)] =
    expression match {
      case GreaterThan(Property(variable: LogicalVariable, PropertyKeyName(propName)), rhs) =>
        Some((variable, rhs, VectorFilterGreaterThan(propName, rhs)))
      case LessThan(Property(variable: LogicalVariable, PropertyKeyName(propName)), rhs) =>
        Some((variable, rhs, VectorFilterLessThan(propName, rhs)))
      case GreaterThanOrEqual(Property(variable: LogicalVariable, PropertyKeyName(propName)), rhs) =>
        Some((variable, rhs, VectorFilterGreaterThanOrEqual(propName, rhs)))
      case LessThanOrEqual(Property(variable: LogicalVariable, PropertyKeyName(propName)), rhs) =>
        Some((variable, rhs, VectorFilterLessThanOrEqual(propName, rhs)))
      case Equals(Property(variable: LogicalVariable, PropertyKeyName(propName)), rhs) =>
        Some((variable, rhs, Equality(propName, rhs)))
      case In(Property(variable: LogicalVariable, PropertyKeyName(propName)), ListLiteral(Seq(rhs))) =>
        Some((variable, rhs, Equality(propName, rhs)))
      case In(Property(variable: LogicalVariable, PropertyKeyName(propName)), rhs) =>
        Some((variable, rhs, InSet(propName, rhs)))
      case IsNotNull(Property(variable: LogicalVariable, PropertyKeyName(propName))) =>
        Some((variable, Null()(InputPosition.NONE), Exists(propName)))
      case IsNull(Property(variable: LogicalVariable, PropertyKeyName(propName))) =>
        Some((variable, Null()(InputPosition.NONE), NotExists(propName)))
      case _ =>
        None
    }

  object VectorFilterExpressionRange {

    def unapply(expressions: Seq[VectorFilterExpression])
      : Option[(LowerBoundVectorFilterExpression, UpperBoundVectorFilterExpression)] =
      expressions match {
        case Seq(lower: LowerBoundVectorFilterExpression, upper: UpperBoundVectorFilterExpression) =>
          Some((lower, upper))
        case Seq(upper: UpperBoundVectorFilterExpression, lower: LowerBoundVectorFilterExpression) =>
          Some((lower, upper))
        case _ => None
      }
  }
}
