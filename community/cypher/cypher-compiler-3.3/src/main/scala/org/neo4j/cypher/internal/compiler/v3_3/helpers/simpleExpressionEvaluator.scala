/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_3.helpers

import org.neo4j.cypher.internal.compiler.v3_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_3.ast.convert.commands.ExpressionConverters
import org.neo4j.cypher.internal.compiler.v3_3.pipes.{NullPipeDecorator, QueryState}
import org.neo4j.cypher.internal.frontend.v3_2.ast.functions.{Rand, Timestamp}
import org.neo4j.cypher.internal.frontend.v3_2.ast.{Expression, FunctionInvocation, Parameter}
import org.neo4j.cypher.internal.frontend.v3_2.{CypherException => InternalCypherException}

import scala.collection.mutable

object simpleExpressionEvaluator {

  def hasParameters(expr: Expression): Boolean =
    expr.inputs.exists {
      case (Parameter(_, _), _) => true
      case _ => false
    }

  def isNonDeterministic(expr: Expression): Boolean =
    expr.inputs.exists {
      case (func@FunctionInvocation(_, _, _, _), _) if func.function == Rand => true
      case (func@FunctionInvocation(_, _, _, _), _) if func.function == Timestamp => true
      case _ => false
    }

  // Returns Some(value) if the expression can be independently evaluated in an empty context/query state, otherwise None
  def evaluateExpression(expr: Expression): Option[Any] = {
    val commandExpr = ExpressionConverters.toCommandExpression(expr)

    implicit val emptyQueryState = new QueryState(query = null, resources = null, params = Map.empty,
                                                  decorator = NullPipeDecorator, triadicState = mutable.Map.empty,
                                                  repeatableReads = mutable.Map.empty)

    try {
      Some(commandExpr(ExecutionContext.empty))
    }
    catch {
      case e: InternalCypherException => None // Silently disregard expressions that cannot be evaluated in an empty context
    }
  }
}
