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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes.QueryState
import org.neo4j.cypher.internal.frontend.v3_4.ArithmeticException
import org.neo4j.values._
import org.neo4j.values.storable._

case class Divide(a: Expression, b: Expression) extends Arithmetics(a, b) {
  def operand = "/"

  def verb = "divide"

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue = {
    val aVal = a(ctx, state)
    val bVal = b(ctx, state)

    (aVal, bVal) match {
      case (_, l:IntegralValue) if l.longValue() == 0L  => throw new ArithmeticException("/ by zero")
      case (_, l:DoubleValue) if l.doubleValue() == 0L  => throw new ArithmeticException("/ by zero")
      case (_, l:FloatValue) if l.doubleValue() == 0L  => throw new ArithmeticException("/ by zero")
      case (x, y) if x == Values.NO_VALUE || y == Values.NO_VALUE => Values.NO_VALUE
      case (x: NumberValue, y: NumberValue) => calc(x, y)
      case _ => throwTypeError(bVal, aVal)
    }
  }

  def calc(a: NumberValue, b: NumberValue): AnyValue = divide(a, b)

  def rewrite(f: (Expression) => Expression) = f(Divide(a.rewrite(f), b.rewrite(f)))

  def symbolTableDependencies = a.symbolTableDependencies ++ b.symbolTableDependencies
}
