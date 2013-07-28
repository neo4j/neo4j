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
package org.neo4j.cypher.internal.parser.experimental.functions

import org.neo4j.cypher.internal.parser.experimental._
import org.neo4j.cypher.internal.symbols._
import org.neo4j.cypher.internal.commands.{expressions => commandexpressions}

case object StdDev extends AggregatingFunction {
  def name = "STDEV"

  override def semanticCheck(ctx: ast.Expression.SemanticContext, invocation: ast.FunctionInvocation): SemanticCheck = {
    super.semanticCheck(ctx, invocation) then
      checkArgs(invocation, 1) then
      invocation.limitType(invocation.arguments(0).types)
  }

  def toCommand(invocation: ast.FunctionInvocation) = {
    val inner = invocation.arguments(0).toCommand
    val command = commandexpressions.Stdev(inner)
    if (invocation.distinct)
      commandexpressions.Distinct(command, inner)
    else
      command
  }
}
