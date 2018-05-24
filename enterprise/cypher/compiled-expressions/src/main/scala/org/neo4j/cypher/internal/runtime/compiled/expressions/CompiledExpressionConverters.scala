/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{ExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue
import org.opencypher.v9_0.expressions.FunctionInvocation
import org.opencypher.v9_0.expressions.functions.AggregatingFunction
import org.opencypher.v9_0.{expressions => ast}

class CompiledExpressionConverters(inner: ExpressionConverters) extends ExpressionConverter {

  override def toCommandExpression(expression: ast.Expression,
                                   self: ExpressionConverters): Option[Expression] = expression match {

    //we don't deal with aggregations
    case f: FunctionInvocation if f.function.isInstanceOf[AggregatingFunction] => None

    case e => IntermediateCodeGeneration.compile(e) match {
      case Some(ir) =>
        Some(new CompileWrappingExpression(CodeGeneration.compile(ir),
                                           inner.toCommandExpression(expression)))
      case _ => None
    }
  }

  class CompileWrappingExpression(ce: CompiledExpression, legacy: Expression) extends Expression {

    override def rewrite(f: Expression => Expression): Expression = f(new CompileWrappingExpression(ce, legacy))

    override def arguments: Seq[Expression] = legacy.arguments

    override def apply(ctx: ExecutionContext, state: QueryState): AnyValue = ce.compute(state.params)

    override def symbolTableDependencies: Set[String] = legacy.symbolTableDependencies
  }


}








