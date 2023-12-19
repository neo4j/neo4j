/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.aggregation

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions.ExpressionConverter._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions.{CodeGenExpression, CodeGenType}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.{CodeGenContext, Variable}
import org.neo4j.cypher.internal.compiler.v3_4.planner.CantCompileQueryException
import org.neo4j.cypher.internal.v3_4.{expressions => ast}
import org.neo4j.cypher.internal.v3_4.{functions => astFunctions}

/*
* Conversion methods for aggregation functions
*/
object AggregationConverter {

  def aggregateExpressionConverter(opName: String, groupingVariables: Iterable[(String,CodeGenExpression)], name: String, e: ast.Expression) (implicit context: CodeGenContext): AggregateExpression = {
    val variable = Variable(context.namer.newVarName(), CodeGenType.primitiveInt)
    context.addVariable(name, variable)
    context.addProjectedVariable(name, variable)
    e match {
      case func: ast.FunctionInvocation => func.function match {
        case astFunctions.Count if groupingVariables.isEmpty =>
          SimpleCount(opName, variable, createExpression(func.args(0)), func.distinct)
        case astFunctions.Count  =>
          new DynamicCount(opName, variable, createExpression(func.args(0)), groupingVariables, func.distinct)

        case f => throw new CantCompileQueryException(s"$f is not supported")
      }
      case _ => throw new CantCompileQueryException(s"$e is not supported")
    }
  }
}
