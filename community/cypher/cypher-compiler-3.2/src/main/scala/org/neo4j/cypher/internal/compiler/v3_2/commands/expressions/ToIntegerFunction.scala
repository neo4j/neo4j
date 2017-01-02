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
package org.neo4j.cypher.internal.compiler.v3_2.commands.expressions

import org.neo4j.cypher.internal.compiler.v3_2._
import org.neo4j.cypher.internal.compiler.v3_2.pipes.QueryState
import org.neo4j.cypher.internal.frontend.v3_2.{CypherTypeException, ParameterWrongTypeException}

case class ToIntegerFunction(a: Expression) extends NullInNullOutExpression(a) {
  def symbolTableDependencies: Set[String] = a.symbolTableDependencies

  def arguments: Seq[Expression] = Seq(a)

  def rewrite(f: (Expression) => Expression): Expression = f(ToIntegerFunction(a.rewrite(f)))

  def compute(value: Any, m: ExecutionContext)(implicit state: QueryState): Any = value match {
    case v: Number =>
      v.longValue()
    case v: String =>
      try {
        java.lang.Long.parseLong(v)
      } catch {
        case e: Exception =>
        try {
          val d = BigDecimal(v)
          if (d <= Long.MaxValue && d >= Long.MinValue) d.toLong
          else throw new CypherTypeException(s"integer, $v, is too large")
        } catch {
          case e: NumberFormatException =>
            null
        }
      }
    case v =>
      throw new ParameterWrongTypeException("Expected a String or Number, got: " + v.toString)
  }
}
