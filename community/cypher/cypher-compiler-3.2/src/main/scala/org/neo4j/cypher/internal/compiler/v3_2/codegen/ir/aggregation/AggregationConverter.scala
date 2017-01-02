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
package org.neo4j.cypher.internal.compiler.v3_2.codegen.ir.aggregation

import org.neo4j.cypher.internal.compiler.v3_2.codegen.ir.expressions.CodeGenType
import org.neo4j.cypher.internal.compiler.v3_2.codegen.ir.expressions.ExpressionConverter._
import org.neo4j.cypher.internal.compiler.v3_2.codegen.{CodeGenContext, Variable}
import org.neo4j.cypher.internal.compiler.v3_2.planner.CantCompileQueryException
import org.neo4j.cypher.internal.frontend.v3_2.ast

/*
* Conversion methods for aggregation functions
*/
object AggregationConverter {

  def aggregateExpressionConverter(opName: String, groupingVariables: Iterable[Variable], name: String, e: ast.Expression) (implicit context: CodeGenContext): AggregateExpression = {
    val variable = Variable(context.namer.newVarName(), CodeGenType.primitiveInt)
    context.addVariable(name, variable)
    e match {
      case func: ast.FunctionInvocation => func.function match {
        case ast.functions.Count if groupingVariables.isEmpty =>
          SimpleCount(variable, createExpression(func.args(0)), func.distinct)
        case ast.functions.Count  =>
          new DynamicCount(opName, variable, createExpression(func.args(0)), groupingVariables, func.distinct)

        case f => throw new CantCompileQueryException(s"$f is not supported")
      }
      case _ => throw new CantCompileQueryException(s"$e is not supported")
    }
  }
}
