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
package org.neo4j.cypher.internal.compiler.v2_0.functions

import org.neo4j.cypher.internal.compiler.v2_0._
import org.neo4j.cypher.internal.compiler.v2_0.symbols._
import org.neo4j.cypher.internal.compiler.v2_0.commands
import org.neo4j.cypher.internal.compiler.v2_0.commands.{expressions => commandexpressions}
import org.neo4j.cypher.internal.compiler.v2_0.commands.values.TokenType.PropertyKey
import org.neo4j.cypher.internal.compiler.v2_0.ast.FunctionInvocation

case object Has extends PredicateFunction {
  def name = "HAS"

  def semanticCheck(ctx: ast.Expression.SemanticContext, invocation: ast.FunctionInvocation) : SemanticCheck =
    checkArgs(invocation, 1) ifOkThen {
      invocation.arguments(0) match {
        case _: ast.Property => None
        case e => Some(SemanticError(s"Argument to ${invocation.name} is not a property", e.token, invocation.token))
      }
    } then invocation.specifyType(BooleanType())

  protected def internalToPredicate(invocation: FunctionInvocation) = {
    val property = invocation.arguments(0).asInstanceOf[ast.Property]
    commands.Has(property.map.toCommand, PropertyKey(property.identifier.name))
  }
}
