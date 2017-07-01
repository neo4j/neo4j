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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.helpers.{CastSupport, IsList, IsMap, ListSupport}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.QueryState
import org.neo4j.cypher.internal.frontend.v3_3.{CypherTypeException, InvalidArgumentException}
import org.neo4j.values._
import org.neo4j.values.storable.{FloatValue, NumberValue, TextValue, Values}

case class ContainerIndex(expression: Expression, index: Expression) extends NullInNullOutExpression(expression)
with ListSupport {
  def arguments = Seq(expression, index)

  def compute(value: AnyValue, ctx: ExecutionContext)(implicit state: QueryState): AnyValue = {
    value match {
      case IsMap(m) =>
        val item = index(ctx)
        if (item == Values.NO_VALUE) Values.NO_VALUE
        else {
          val key = CastSupport.castOrFail[TextValue](item)
          m.get(key.stringValue())
        }

      case IsList(collection) =>
        val item = index(ctx)
        if (item == Values.NO_VALUE) Values.NO_VALUE
        else {
          var idx = validateTypeAndRange(item)

          if (idx < 0)
            idx = collection.size + idx

          if (idx >= collection.size || idx < 0) Values.NO_VALUE
          else collection.value(idx)
        }

      case _ =>
        throw new CypherTypeException(
          s"`$value` is not a collection or a map. Element access is only possible by performing a collection lookup using an integer index, or by performing a map lookup using a string key (found: $value[${index(ctx)}])")
    }
  }

  private def validateTypeAndRange(item: AnyValue): Int = {
    val number = CastSupport.castOrFail[NumberValue](item)

    val longValue = number match {
      case _: FloatValue =>
        throw new CypherTypeException(s"Cannot index a list using an non-integer number, got $number")
      case _ => number.longValue()
    }

    if (longValue > Int.MaxValue || longValue < Int.MinValue)
      throw new InvalidArgumentException(
        s"Cannot index a list using a value greater than ${Int.MaxValue} or lesser than ${Int.MinValue}, got $number")

    longValue.toInt
  }

  def rewrite(f: (Expression) => Expression): Expression = f(ContainerIndex(expression.rewrite(f), index.rewrite(f)))

  def symbolTableDependencies: Set[String] = expression.symbolTableDependencies ++ index.symbolTableDependencies
}
