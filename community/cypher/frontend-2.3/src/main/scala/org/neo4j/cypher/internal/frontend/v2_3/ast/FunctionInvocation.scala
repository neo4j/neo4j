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

import org.neo4j.cypher.internal.frontend.v2_3.{SemanticError, _}
import org.neo4j.cypher.internal.frontend.v2_3.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v2_3.InputPosition

object FunctionInvocation {
  def apply(name: FunctionName, argument: Expression)(position: InputPosition): FunctionInvocation =
    FunctionInvocation(name, distinct = false, IndexedSeq(argument))(position)
  def apply(left: Expression, name: FunctionName, right: Expression): FunctionInvocation =
    FunctionInvocation(name, distinct = false, IndexedSeq(left, right))(name.position)
  def apply(expression: Expression, name: FunctionName): FunctionInvocation =
    FunctionInvocation(name, distinct = false, IndexedSeq(expression))(name.position)
}

case class FunctionInvocation(functionName: FunctionName, distinct: Boolean, args: IndexedSeq[Expression])
                             (val position: InputPosition) extends Expression {
  val name = functionName.name
  val function = Function.lookup.get(name.toLowerCase)

  def semanticCheck(ctx: SemanticContext) = function match {
    case None    => SemanticError(s"Unknown function '$name'", position)
    case Some(f) => f.semanticCheckHook(ctx, this)
  }
}

case class FunctionName(name: String)(val position: InputPosition) extends SymbolicName {
  override def equals(x: Any): Boolean = x match {
    case FunctionName(other) => other.toLowerCase == name.toLowerCase
    case _ => false
  }
  override def hashCode = name.toLowerCase.hashCode
}
