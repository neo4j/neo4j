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

  def aggregateExpressionConverter(opName: String, groupingVariables: Iterable[Variable], name: String, e: ast.Expression) (implicit context: CodeGenContext) = {
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
