/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
      Some(CountOperatorExpression(self.toCommandExpression(c.arguments.head)))
    case c: FunctionInvocation if c.function == functions.Avg =>
      Some(AvgOperatorExpression(self.toCommandExpression(c.arguments.head)))
    case c: FunctionInvocation if c.function == functions.Max =>
      Some(MaxOperatorExpression(self.toCommandExpression(c.arguments.head)))
    case c: FunctionInvocation if c.function == functions.Min =>
      Some(MinOperatorExpression(self.toCommandExpression(c.arguments.head)))
    case c: FunctionInvocation if c.function == functions.Collect =>
      Some(CollectOperatorExpression(self.toCommandExpression(c.arguments.head)))
    case _: CountStar => Some(CountStarOperatorExpression)
    case f: FunctionInvocation if f.function.isInstanceOf[AggregatingFunction] => throw new CantCompileQueryException()
    case _ => None
  }
}






