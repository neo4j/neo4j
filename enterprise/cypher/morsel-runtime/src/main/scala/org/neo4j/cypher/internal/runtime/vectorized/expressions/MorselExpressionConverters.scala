package org.neo4j.cypher.internal.runtime.vectorized.expressions

import org.neo4j.cypher.internal.compiler.v3_4.planner.CantCompileQueryException
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{ExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.v3_4.expressions._
import org.neo4j.cypher.internal.v3_4.functions.AggregatingFunction
import org.neo4j.cypher.internal.v3_4.{functions, expressions => ast}


object MorselExpressionConverters extends ExpressionConverter {

  override def toCommandExpression(expression: ast.Expression,
                                   self: ExpressionConverters): Option[Expression] = expression match {

    case c: FunctionInvocation if c.function == functions.Count =>
      val inner = self.toCommandExpression(c.arguments.head)
      Some(CountOperatorExpression(inner))
    case f: FunctionInvocation if f.function.isInstanceOf[AggregatingFunction] => throw new CantCompileQueryException()
    case _ => None
  }
}






