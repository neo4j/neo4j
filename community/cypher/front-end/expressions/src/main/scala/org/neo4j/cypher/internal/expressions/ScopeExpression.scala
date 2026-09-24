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

import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition

/**
 * Scope expressions bundle together variables of a new scope
 * together with any child expressions that get evaluated in a context where
 * these variables are bound
 *
 * This is a hard contract(!): All child expressions of a scope expression must be:
 * - either an introduced variable
 * - or child expressions in a scope where those variables are bound
 */
trait ScopeExpression extends Expression {
  def introducedVariables: Set[LogicalVariable]
  def scopeDependencies: Set[LogicalVariable]

  /**
   * Whether [[introducedVariables]] shadow same-named variables of the enclosing scope.
   *
   * False where the set also collects nested scopes' declarations, as for `FullSubqueryExpression`:
   * shadowing all of them at once would hide an outer occurrence that a nested declaration behind a
   * [[ScopeBoundary]] merely shares a name with.
   */
  def introducedVariablesShadow: Boolean = true

  // We need to override dependencies because the default implementation relies on scope Expressions computing the dependencies manually,
  // so that it does not need to recurse into them.
  final override def dependencies: Set[LogicalVariable] = scopeDependencies

  override def isConstantForQuery: Boolean = false
}

case class FilterScope(variable: LogicalVariable, innerPredicate: Option[Expression])(val position: InputPosition)
    extends ScopeExpression {
  val introducedVariables: Set[LogicalVariable] = Set(variable)

  override def scopeDependencies: Set[LogicalVariable] =
    innerPredicate.fold(Set.empty[LogicalVariable])(_.dependencies) -- introducedVariables
}

case class ExtractScope(
  variable: LogicalVariable,
  innerPredicate: Option[Expression],
  extractExpression: Option[Expression]
)(val position: InputPosition) extends ScopeExpression {
  val introducedVariables: Set[LogicalVariable] = Set(variable)

  override def scopeDependencies: Set[LogicalVariable] =
    innerPredicate.fold(Set.empty[LogicalVariable])(_.dependencies) ++
      extractExpression.fold(Set.empty[LogicalVariable])(_.dependencies) --
      introducedVariables
}

case class ExtractMapScope(
  variable: LogicalVariable,
  innerPredicate: Option[Expression],
  extractKeyExpression: Expression,
  extractValueExpression: Expression
)(val position: InputPosition) extends ScopeExpression {
  val introducedVariables: Set[LogicalVariable] = Set(variable)

  override def scopeDependencies: Set[LogicalVariable] =
    innerPredicate.fold(Set.empty[LogicalVariable])(_.dependencies) ++
      extractKeyExpression.dependencies ++
      extractValueExpression.dependencies --
      introducedVariables
}

case class ExtractMapEntriesScope(
  keyVariable: LogicalVariable,
  valueVariable: LogicalVariable,
  innerPredicate: Option[Expression],
  extractKeyExpression: Expression,
  extractValueExpression: Expression
)(val position: InputPosition) extends ScopeExpression {
  val introducedVariables: Set[LogicalVariable] = Set(keyVariable, valueVariable)

  override def scopeDependencies: Set[LogicalVariable] =
    innerPredicate.fold(Set.empty[LogicalVariable])(_.dependencies) ++
      extractKeyExpression.dependencies ++
      extractValueExpression.dependencies --
      introducedVariables
}

case class ReduceScope(accumulator: LogicalVariable, variable: LogicalVariable, expression: Expression)(
  val position: InputPosition
) extends ScopeExpression {
  val introducedVariables: Set[LogicalVariable] = Set(accumulator, variable)

  override def scopeDependencies: Set[LogicalVariable] = expression.dependencies -- introducedVariables
}

/**
 * A scope expression which holds pre-computed dependencies from RecordScope.
 * introducedVariables: Variables introduced by this scope
 * scopeDependencies: Variables that are referencing outer scope variables
 * subqueryAstNode: Refers to the inner ASTNode that can used to compute the dependencies
 */
trait ExpressionWithComputedDependencies extends Expression {
  self: ScopeExpression =>

  val computedIntroducedVariables: Option[Set[LogicalVariable]]
  val computedScopeDependencies: Option[Set[LogicalVariable]]

  def subqueryAstNode: ASTNode

  def withComputedIntroducedVariables(computedIntroducedVariables: Set[LogicalVariable])
    : ExpressionWithComputedDependencies
  def withComputedScopeDependencies(computedScopeDependencies: Set[LogicalVariable]): ExpressionWithComputedDependencies

  final override def introducedVariables: Set[LogicalVariable] = computedIntroducedVariables.getOrElse(
    throw new IllegalStateException("Introduced variables have not been computed yet")
  )

  final override def scopeDependencies: Set[LogicalVariable] = computedScopeDependencies.getOrElse(
    throw new IllegalStateException("Scope dependencies have not been computed yet")
  )

}
