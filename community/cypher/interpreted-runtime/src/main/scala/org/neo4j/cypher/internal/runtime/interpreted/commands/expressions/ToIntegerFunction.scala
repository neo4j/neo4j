/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.util.v3_4.{CypherTypeException, ParameterWrongTypeException}
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values._
import org.neo4j.values.storable.{LongValue, NumberValue, TextValue, Values}

case class ToIntegerFunction(a: Expression) extends NullInNullOutExpression(a) {

  def symbolTableDependencies: Set[String] = a.symbolTableDependencies

  def arguments: Seq[Expression] = Seq(a)

  def rewrite(f: (Expression) => Expression): Expression = f(ToIntegerFunction(a.rewrite(f)))

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue = value match {
    case v: LongValue => v
    case v: NumberValue => Values.longValue(v.longValue())
    case v: TextValue =>
      try {
        Values.longValue(java.lang.Long.parseLong(v.stringValue()))
      } catch {
        case e: Exception =>
          try {
            val d = BigDecimal(v.stringValue())
            if (d <= Long.MaxValue && d >= Long.MinValue) Values.longValue(d.toLong)
            else throw new CypherTypeException(s"integer, ${v.stringValue()}, is too large")
          } catch {
            case _: NumberFormatException =>
              Values.NO_VALUE
          }
      }
    case v =>
      throw new ParameterWrongTypeException("Expected a String or Number, got: " + v.toString)
  }
}
