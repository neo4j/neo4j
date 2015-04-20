/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_2._
import pipes.QueryState
import symbols._

case class ToIntFunction(a: Expression) extends NullInNullOutExpression(a) {
  def symbolTableDependencies: Set[String] = a.symbolTableDependencies

  protected def calculateType(symbols: SymbolTable): CypherType = CTInteger

  def arguments: Seq[Expression] = Seq(a)

  def rewrite(f: (Expression) => Expression): Expression = f(ToIntFunction(a.rewrite(f)))

  def compute(value: Any, m: ExecutionContext)(implicit state: QueryState): Any = a(m) match {
    case v: Number =>
      v.longValue()
    case v: String =>
      try {
        val d = BigDecimal(v)
        if (d <= Long.MaxValue && d >= Long.MinValue) d.toLong
        else throw new CypherTypeException(s"integer, $v, is too large")
      } catch {
        case e: NumberFormatException =>
          null
      }
    case v =>
      throw new CypherTypeException("Expected a String or Number, got: " + v.toString)
  }
}
