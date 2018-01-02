/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.cypher.internal.frontend.v2_3.ArithmeticException

case class Divide(a: Expression, b: Expression) extends Arithmetics(a, b) {
  def operand = "/"

  def verb = "divide"

  override def apply(ctx: ExecutionContext)(implicit state: QueryState) = {
    val aVal = a(ctx)
    val bVal = b(ctx)

    (aVal, bVal) match {
      case (_, 0) => throw new ArithmeticException("/ by zero")
      case (null, _) => null
      case (_, null) => null
      case (x: Number, y: Number) => calc(x, y)
      case _ => throwTypeError(bVal, aVal)
    }
  }

  def calc(a: Number, b: Number) = divide(a, b)

  def rewrite(f: (Expression) => Expression) = f(Divide(a.rewrite(f), b.rewrite(f)))

  def symbolTableDependencies = a.symbolTableDependencies ++ b.symbolTableDependencies
}
