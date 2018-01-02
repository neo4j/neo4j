/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.frontend.v2_3.ast

import org.neo4j.cypher.internal.frontend.v2_3.{InputPosition, SemanticCheckResult}
import org.neo4j.cypher.internal.frontend.v2_3.ast.Expression.SemanticContext

// Scope expressions bundle together identifiers of a new scope
// together with any child expressions that get evaluated in a context where
// these identifiers are bound
//
trait ScopeExpression extends Expression {
  def identifiers: Set[Identifier]

  override def semanticCheck(ctx: SemanticContext) = SemanticCheckResult.success
}

case class FilterScope(identifier: Identifier, innerPredicate: Option[Expression])(val position: InputPosition) extends ScopeExpression {
  val identifiers = Set(identifier)
}

case class ExtractScope(identifier: Identifier, innerPredicate: Option[Expression], extractExpression: Option[Expression])(val position: InputPosition) extends ScopeExpression {
  val identifiers = Set(identifier)
}

case class ReduceScope(accumulator: Identifier, identifier: Identifier, expression: Expression)(val position: InputPosition) extends ScopeExpression {
  val identifiers = Set(accumulator, identifier)
}
