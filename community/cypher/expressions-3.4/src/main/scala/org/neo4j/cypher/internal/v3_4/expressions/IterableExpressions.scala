/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.v3_4.expressions

import org.neo4j.cypher.internal.util.v3_4.InputPosition

trait FilteringExpression extends Expression {
  def name: String
  def variable: LogicalVariable
  def expression: Expression
  def innerPredicate: Option[Expression]

  override def arguments = Seq(expression)
}

case class FilterExpression(scope: FilterScope, expression: Expression)(val position: InputPosition) extends FilteringExpression {
  val name = "filter"

  def variable = scope.variable
  def innerPredicate = scope.innerPredicate
}

object FilterExpression {
  def apply(variable: Variable, expression: Expression, innerPredicate: Option[Expression])(position: InputPosition): FilterExpression =
    FilterExpression(FilterScope(variable, innerPredicate)(position), expression)(position)
}

case class ExtractExpression(scope: ExtractScope, expression: Expression)(val position: InputPosition) extends FilteringExpression
{
  val name = "extract"

  def variable = scope.variable
  def innerPredicate = scope.innerPredicate
  def extractExpression = scope.extractExpression
}

object ExtractExpression {
  def apply(variable: Variable,
            expression: Expression,
            innerPredicate: Option[Expression],
            extractExpression: Option[Expression])(position: InputPosition): ExtractExpression =
    ExtractExpression(ExtractScope(variable, innerPredicate, extractExpression)(position), expression)(position)
}

case class ListComprehension(scope: ExtractScope, expression: Expression)(val position: InputPosition)
  extends FilteringExpression {

  val name = "[...]"

  def variable = scope.variable
  def innerPredicate = scope.innerPredicate
  def extractExpression = scope.extractExpression
}

object ListComprehension {
  def apply(variable: Variable,
            expression: Expression,
            innerPredicate: Option[Expression],
            extractExpression: Option[Expression])(position: InputPosition): ListComprehension =
    ListComprehension(ExtractScope(variable, innerPredicate, extractExpression)(position), expression)(position)
}

case class PatternComprehension(namedPath: Option[LogicalVariable], pattern: RelationshipsPattern,
                                predicate: Option[Expression], projection: Expression)
                               (val position: InputPosition, val outerScope: Set[LogicalVariable])
  extends ScopeExpression {

  self =>

  def withOuterScope(outerScope: Set[LogicalVariable]): PatternComprehension =
    copy()(position, outerScope)

  override val introducedVariables: Set[LogicalVariable] = {
    val introducedInternally = namedPath.toSet ++ pattern.element.allVariables
    val introducedExternally = introducedInternally -- outerScope
    introducedExternally
  }

  override def dup(children: Seq[AnyRef]): this.type = {
    PatternComprehension(
      children(0).asInstanceOf[Option[LogicalVariable]],
      children(1).asInstanceOf[RelationshipsPattern],
      children(2).asInstanceOf[Option[Expression]],
      children(3).asInstanceOf[Expression]
    )(position, outerScope).asInstanceOf[this.type]
  }
}

sealed trait IterablePredicateExpression extends FilteringExpression {

  def scope: FilterScope
  def variable: LogicalVariable = scope.variable
  def innerPredicate: Option[Expression] = scope.innerPredicate

  override def asCanonicalStringVal: String = {
    val predicate = innerPredicate.map(p => s" where ${p.asCanonicalStringVal}").getOrElse("")
    s"$name(${variable.asCanonicalStringVal}) in ${expression.asCanonicalStringVal}$predicate"
  }
}

case class AllIterablePredicate(scope: FilterScope, expression: Expression)(val position: InputPosition) extends IterablePredicateExpression {
  val name = "all"
}

object AllIterablePredicate {
  def apply(variable: LogicalVariable, expression: Expression, innerPredicate: Option[Expression])(position: InputPosition): AllIterablePredicate =
    AllIterablePredicate(FilterScope(variable, innerPredicate)(position), expression)(position)
}

case class AnyIterablePredicate(scope: FilterScope, expression: Expression)(val position: InputPosition) extends IterablePredicateExpression {
  val name = "any"
}

object AnyIterablePredicate {
  def apply(variable: LogicalVariable, expression: Expression, innerPredicate: Option[Expression])(position: InputPosition): AnyIterablePredicate =
    AnyIterablePredicate(FilterScope(variable, innerPredicate)(position), expression)(position)
}

case class NoneIterablePredicate(scope: FilterScope, expression: Expression)(val position: InputPosition) extends IterablePredicateExpression {
  val name = "none"
}

object NoneIterablePredicate {
  def apply(variable: LogicalVariable, expression: Expression, innerPredicate: Option[Expression])(position: InputPosition): NoneIterablePredicate =
    NoneIterablePredicate(FilterScope(variable, innerPredicate)(position), expression)(position)
}

case class SingleIterablePredicate(scope: FilterScope, expression: Expression)(val position: InputPosition) extends IterablePredicateExpression {
  val name = "single"
}

object SingleIterablePredicate {
  def apply(variable: LogicalVariable, expression: Expression, innerPredicate: Option[Expression])(position: InputPosition): SingleIterablePredicate =
    SingleIterablePredicate(FilterScope(variable, innerPredicate)(position), expression)(position)
}

case class ReduceExpression(scope: ReduceScope, init: Expression, list: Expression)(val position: InputPosition) extends Expression {
  def variable = scope.variable
  def accumulator = scope.accumulator
  def expression = scope.expression
}

object ReduceExpression {
  val AccumulatorExpressionTypeMismatchMessageGenerator = (expected: String, existing: String) => s"accumulator is $expected but expression has type $existing"

  def apply(accumulator: Variable, init: Expression, variable: Variable, list: Expression, expression: Expression)(position: InputPosition): ReduceExpression =
    ReduceExpression(ReduceScope(accumulator, variable, expression)(position), init, list)(position)
}

