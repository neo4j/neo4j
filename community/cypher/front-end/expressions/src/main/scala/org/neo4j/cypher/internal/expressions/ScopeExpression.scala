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
package org.neo4j.cypher.internal.expressions

import org.neo4j.cypher.internal.util.InputPosition

// Scope expressions bundle together variables of a new scope
// together with any child expressions that get evaluated in a context where
// these variables are bound
//
// This is a hard contract: There must be no child expressions of a scope expressions
// that are not
// - either introduced variables
// - or child expressions in a scope where those variables are bound
//
trait ScopeExpression extends Expression {
  def introducedVariables: Set[LogicalVariable]
  def scopeDependencies: Set[LogicalVariable]

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

case class ReduceScope(accumulator: LogicalVariable, variable: LogicalVariable, expression: Expression)(
  val position: InputPosition
) extends ScopeExpression {
  val introducedVariables: Set[LogicalVariable] = Set(accumulator, variable)

  override def scopeDependencies: Set[LogicalVariable] = expression.dependencies -- introducedVariables
}

/**
 * A scope expression which also keeps track of the scope around it.
 */
trait ExpressionWithOuterScope extends Expression {
  self: ScopeExpression =>

  def outerScope: Set[LogicalVariable]
  def withOuterScope(outerScope: Set[LogicalVariable]): Expression
}
