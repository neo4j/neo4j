/*
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
package org.neo4j.cypher.internal.compiler.v2_2.functions

import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.commands.ExpressionConverters
import ExpressionConverters._
import commands.values.TokenType.PropertyKey
import symbols._

case object Has extends Function {
  def name = "HAS"

  def semanticCheck(ctx: ast.Expression.SemanticContext, invocation: ast.FunctionInvocation): SemanticCheck =
    checkArgs(invocation, 1) ifOkChain {
      invocation.arguments(0).expectType(CTAny.covariant) chain
      (invocation.arguments(0) match {
        case _: ast.Property => None
        case e => Some(SemanticError(s"Argument to ${invocation.name} is not a property", e.position, invocation.position))
      })
    } chain invocation.specifyType(CTBoolean)

  def asCommandExpression(invocation: ast.FunctionInvocation) = {
    val property = invocation.arguments(0).asInstanceOf[ast.Property]
    commands.PropertyExists(property.map.asCommandExpression, PropertyKey(property.propertyKey.name))
  }
}
