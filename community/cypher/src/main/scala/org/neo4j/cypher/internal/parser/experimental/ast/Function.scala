/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.parser.experimental.ast

import org.neo4j.cypher.internal.parser.experimental._
import org.neo4j.cypher.internal.commands.{expressions => commandexpressions, Predicate => CommandPredicate}
import org.neo4j.cypher.internal.commands.expressions.{Expression => CommandExpression}

import Expression._

object FunctionInvocation {
  def apply(identifier: Identifier, arguments: Seq[Expression], token: InputToken) : FunctionInvocation =
      FunctionInvocation(identifier, arguments.toIndexedSeq, token)
  def apply(identifier: Identifier, argument: Expression, token: InputToken) : FunctionInvocation =
      FunctionInvocation(identifier, IndexedSeq(argument), token)
  def apply(left: Expression, identifier: Identifier, right: Expression) : FunctionInvocation =
      FunctionInvocation(identifier, IndexedSeq(left, right), identifier.token)
  def apply(expression: Expression, identifier: Identifier) : FunctionInvocation =
      FunctionInvocation(identifier, IndexedSeq(expression), identifier.token)
}
case class FunctionInvocation(identifier: Identifier, arguments: IndexedSeq[Expression], token: InputToken)  extends Expression {
  val name = identifier.name
  private val function = Function.lookup.get(name.toLowerCase)

  def semanticCheck(ctx: SemanticContext) = function match {
    case None    => SemanticError(s"Unknown function '${name}'", token)
    case Some(f) => arguments.semanticCheck(ctx) >>= f.semanticCheck(ctx, this)
  }

  def toCommand = function match {
    case None    => throw new IllegalStateException("Unknown function should have failed semantic check")
    case Some(f) => f.toCommand(this)
  }
}
