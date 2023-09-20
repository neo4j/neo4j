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

import org.neo4j.cypher.internal.expressions.functions.Category
import org.neo4j.cypher.internal.expressions.functions.FunctionWithName
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTList

trait FilteringExpression extends Expression {
  def name: String
  def variable: LogicalVariable
  def expression: Expression
  def innerPredicate: Option[Expression]

  override def arguments = Seq(expression)
}

case class ListComprehension(scope: ExtractScope, expression: Expression)(val position: InputPosition)
    extends FilteringExpression {

  val name = "[...]"

  def variable: LogicalVariable = scope.variable
  def innerPredicate: Option[Expression] = scope.innerPredicate
  def extractExpression: Option[Expression] = scope.extractExpression

  override def isConstantForQuery: Boolean =
    expression.isConstantForQuery &&
      innerPredicate.forall(_.isConstantForQuery) &&
      scope.extractExpression.forall(_.isConstantForQuery)
}

object ListComprehension {

  def apply(
    variable: LogicalVariable,
    expression: Expression,
    innerPredicate: Option[Expression],
    extractExpression: Option[Expression]
  )(position: InputPosition): ListComprehension =
    ListComprehension(ExtractScope(variable, innerPredicate, extractExpression)(position), expression)(position)
}

case class PatternComprehension(
  namedPath: Option[LogicalVariable],
  pattern: RelationshipsPattern,
  predicate: Option[Expression],
  projection: Expression
)(
  val position: InputPosition,
  override val computedIntroducedVariables: Option[Set[LogicalVariable]],
  override val computedScopeDependencies: Option[Set[LogicalVariable]]
) extends ScopeExpression with ExpressionWithComputedDependencies with SubqueryExpression {

  self =>

  override def withComputedIntroducedVariables(computedIntroducedVariables: Set[LogicalVariable])
    : ExpressionWithComputedDependencies =
    copy()(position, computedIntroducedVariables = Some(computedIntroducedVariables), computedScopeDependencies)

  override def withComputedScopeDependencies(computedScopeDependencies: Set[LogicalVariable])
    : ExpressionWithComputedDependencies =
    copy()(position, computedIntroducedVariables, computedScopeDependencies = Some(computedScopeDependencies))

  override def subqueryAstNode: ASTNode = pattern

  override def dup(children: Seq[AnyRef]): this.type = {
    PatternComprehension(
      children(0).asInstanceOf[Option[LogicalVariable]],
      children(1).asInstanceOf[RelationshipsPattern],
      children(2).asInstanceOf[Option[Expression]],
      children(3).asInstanceOf[Expression]
    )(position, computedIntroducedVariables, computedScopeDependencies).asInstanceOf[this.type]
  }
}

sealed trait IterableExpressionWithInfo extends FunctionWithName with TypeSignatures {
  def description: String

  // TODO: Get specification formalized by CLG
  override def signatures: Seq[TypeSignature] =
    Seq(FunctionTypeSignature(
      function = this,
      CTBoolean,
      names = Vector("variable", "list"),
      description = description,
      argumentTypes = Vector(CTAny, CTList(CTAny)),
      category = Category.PREDICATE,
      overrideDefaultAsString =
        Some(s"$name(variable :: VARIABLE IN list :: LIST<ANY> WHERE predicate :: ANY) :: BOOLEAN")
    ))
}

sealed trait IterablePredicateExpression extends FilteringExpression with BooleanExpression {
  def scope: FilterScope
  def variable: LogicalVariable = scope.variable
  def innerPredicate: Option[Expression] = scope.innerPredicate

  override def asCanonicalStringVal: String = {
    val predicate = innerPredicate.map(p => s" where ${p.asCanonicalStringVal}").getOrElse("")
    s"$name(${variable.asCanonicalStringVal}) in ${expression.asCanonicalStringVal}$predicate"
  }

  override def isConstantForQuery: Boolean =
    expression.isConstantForQuery &&
      innerPredicate.forall(_.isConstantForQuery)
}

object IterablePredicateExpression {

  private val knownPredicateFunctions: Seq[IterableExpressionWithInfo] = Vector(
    AllIterablePredicate,
    AnyIterablePredicate,
    NoneIterablePredicate,
    SingleIterablePredicate
  )

  def functionInfo: Seq[FunctionTypeSignature] = knownPredicateFunctions.flatMap(_.signatures.map {
    case f: FunctionTypeSignature => f
    case problem => throw new IllegalStateException("Did not expect the following at this point: " + problem)
  })
}

case class AllIterablePredicate(scope: FilterScope, expression: Expression)(val position: InputPosition)
    extends IterablePredicateExpression {
  val name: String = AllIterablePredicate.name
}

object AllIterablePredicate extends IterableExpressionWithInfo {
  val name = "all"
  val description = "Returns true if the predicate holds for all elements in the given `LIST<ANY>`."

  def apply(
    variable: LogicalVariable,
    expression: Expression,
    innerPredicate: Option[Expression]
  )(position: InputPosition): AllIterablePredicate =
    AllIterablePredicate(FilterScope(variable, innerPredicate)(position), expression)(position)
}

case class AnyIterablePredicate(scope: FilterScope, expression: Expression)(val position: InputPosition)
    extends IterablePredicateExpression {
  val name: String = AnyIterablePredicate.name
}

object AnyIterablePredicate extends IterableExpressionWithInfo {
  val name = "any"
  val description = "Returns true if the predicate holds for at least one element in the given `LIST<ANY>`."

  def apply(
    variable: LogicalVariable,
    expression: Expression,
    innerPredicate: Option[Expression]
  )(position: InputPosition): AnyIterablePredicate =
    AnyIterablePredicate(FilterScope(variable, innerPredicate)(position), expression)(position)
}

case class NoneIterablePredicate(scope: FilterScope, expression: Expression)(val position: InputPosition)
    extends IterablePredicateExpression {
  val name: String = NoneIterablePredicate.name
}

object NoneIterablePredicate extends IterableExpressionWithInfo {
  val name = "none"
  val description = "Returns true if the predicate holds for no element in the given `LIST<ANY>`."

  def apply(
    variable: LogicalVariable,
    expression: Expression,
    innerPredicate: Option[Expression]
  )(position: InputPosition): NoneIterablePredicate =
    NoneIterablePredicate(FilterScope(variable, innerPredicate)(position), expression)(position)
}

case class SingleIterablePredicate(scope: FilterScope, expression: Expression)(val position: InputPosition)
    extends IterablePredicateExpression {
  val name: String = SingleIterablePredicate.name
}

object SingleIterablePredicate extends IterableExpressionWithInfo {
  val name = "single"
  val description = "Returns true if the predicate holds for exactly one of the elements in the given `LIST<ANY>`."

  def apply(
    variable: LogicalVariable,
    expression: Expression,
    innerPredicate: Option[Expression]
  )(position: InputPosition): SingleIterablePredicate =
    SingleIterablePredicate(FilterScope(variable, innerPredicate)(position), expression)(position)
}

case class ReduceExpression(scope: ReduceScope, init: Expression, list: Expression)(val position: InputPosition)
    extends Expression {
  def variable: LogicalVariable = scope.variable
  def accumulator: LogicalVariable = scope.accumulator
  def expression: Expression = scope.expression

  override def isConstantForQuery: Boolean =
    scope.expression.isConstantForQuery && init.isConstantForQuery && list.isConstantForQuery
}

object ReduceExpression {

  val AccumulatorExpressionTypeMismatchMessageGenerator: (String, String) => String =
    (expected: String, existing: String) => s"accumulator is $expected but expression has type $existing"

  def apply(
    accumulator: Variable,
    init: Expression,
    variable: Variable,
    list: Expression,
    expression: Expression
  )(position: InputPosition): ReduceExpression =
    ReduceExpression(ReduceScope(accumulator, variable, expression)(position), init, list)(position)
}
