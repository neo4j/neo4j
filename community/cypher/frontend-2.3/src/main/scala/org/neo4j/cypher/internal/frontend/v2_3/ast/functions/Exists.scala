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
package org.neo4j.cypher.internal.frontend.v2_3.ast.functions

import org.neo4j.cypher.internal.frontend.v2_3.ast.{ContainerIndex, Function}
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.{SemanticError, ast}

case object Exists extends Function {
  def name = "EXISTS"

  def semanticCheck(ctx: ast.Expression.SemanticContext, invocation: ast.FunctionInvocation) =
    checkArgs(invocation, 1) ifOkChain {
      invocation.arguments.head.expectType(CTAny.covariant) chain
        (invocation.arguments.head match {
          case _: ast.Property => None
          case _: ast.PatternExpression => None
          case _: ContainerIndex => None
          case e =>
            Some(SemanticError(s"Argument to ${invocation.name}(...) is not a property or pattern", e.position, invocation.position))
        })
    } chain invocation.specifyType(CTBoolean)
}
