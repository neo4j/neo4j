/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
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






