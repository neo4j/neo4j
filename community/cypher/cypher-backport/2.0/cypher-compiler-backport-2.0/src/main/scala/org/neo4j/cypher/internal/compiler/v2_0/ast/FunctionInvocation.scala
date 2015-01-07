/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.ast

import Expression.SemanticContext
import org.neo4j.cypher.internal.compiler.v2_0._

object FunctionInvocation {
  def apply(identifier: Identifier, argument: Expression)(position: InputPosition): FunctionInvocation =
    FunctionInvocation(identifier, distinct = false, IndexedSeq(argument))(position)
  def apply(left: Expression, identifier: Identifier, right: Expression): FunctionInvocation =
    FunctionInvocation(identifier, distinct = false, IndexedSeq(left, right))(identifier.position)
  def apply(expression: Expression, identifier: Identifier): FunctionInvocation =
    FunctionInvocation(identifier, distinct = false, IndexedSeq(expression))(identifier.position)
}

case class FunctionInvocation(identifier: Identifier, distinct: Boolean, arguments: IndexedSeq[Expression])(val position: InputPosition) extends Expression {
  val name = identifier.name
  val function = Function.lookup.get(name.toLowerCase)

  def semanticCheck(ctx: SemanticContext) = function match {
    case None    => SemanticError(s"Unknown function '$name'", position)
    case Some(f) => f.semanticCheckHook(ctx, this)
  }
}
