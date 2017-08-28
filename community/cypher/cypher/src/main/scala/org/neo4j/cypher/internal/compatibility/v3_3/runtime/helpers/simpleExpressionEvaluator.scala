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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.helpers

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.{NullPipeDecorator, QueryState}
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.ExpressionEvaluator
import org.neo4j.cypher.internal.frontend.v3_3.ast.Expression
import org.neo4j.cypher.internal.frontend.v3_3.{CypherException => InternalCypherException}
import org.neo4j.values.virtual.VirtualValues

import scala.collection.mutable

object simpleExpressionEvaluator extends ExpressionEvaluator {

  // Returns Some(value) if the expression can be independently evaluated in an empty context/query state, otherwise None
  def evaluateExpression(expr: Expression): Option[Any] = {
    val converters = new ExpressionConverters(CommunityExpressionConverter)
    val commandExpr = converters.toCommandExpression(expr)

    implicit val emptyQueryState = new QueryState(query = null, resources = null, params = VirtualValues.EMPTY_MAP,
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
