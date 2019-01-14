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
}

case class FilterScope(variable: LogicalVariable, innerPredicate: Option[Expression])(val position: InputPosition) extends ScopeExpression {
  val introducedVariables = Set(variable)
}

case class ExtractScope(variable: LogicalVariable, innerPredicate: Option[Expression], extractExpression: Option[Expression])(val position: InputPosition) extends ScopeExpression {
  val introducedVariables = Set(variable)
}

case class ReduceScope(accumulator: LogicalVariable, variable: LogicalVariable, expression: Expression)(val position: InputPosition) extends ScopeExpression {
  val introducedVariables = Set(accumulator, variable)
}
