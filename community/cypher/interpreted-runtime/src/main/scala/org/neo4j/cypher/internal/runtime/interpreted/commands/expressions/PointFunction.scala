/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import java.util.function.{BiConsumer, BiFunction}

import org.opencypher.v9_0.util.CypherTypeException
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.IsMap
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.function.ThrowingBiConsumer
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{PointValue, Values}
import org.neo4j.values.virtual.{MapValue, VirtualNodeValue, VirtualRelationshipValue}

object PointFunction {
  private val FILTER_VALID_KEYS = new BiFunction[String, AnyValue, java.lang.Boolean] {
    override def apply(t: String,
                       ignore: AnyValue): java.lang.Boolean = PointValue.ALLOWED_KEYS.exists(_.equalsIgnoreCase(t))
  }
}
case class PointFunction(data: Expression) extends NullInNullOutExpression(data) {


  override def compute(value: AnyValue, ctx: ExecutionContext, state: QueryState): AnyValue = value match {
    case IsMap(mapCreator) =>
      val map = mapCreator(state.query)
      if (containsNull(map)) {
        Values.NO_VALUE
      } else {
        //TODO: We might consider removing this code if the PointBuilder.allowOpenMaps=true remains default
        if (value.isInstanceOf[VirtualNodeValue] || value.isInstanceOf[VirtualRelationshipValue]) {
          PointValue.fromMap(map.filter(PointFunction.FILTER_VALID_KEYS))
        }
        else {
          PointValue.fromMap(map)
        }
      }
    case x => throw new CypherTypeException(s"Expected a map but got $x")
  }

  private def containsNull(map: MapValue) = {
    var hasNull = false
    map.foreach(new ThrowingBiConsumer[String, AnyValue, RuntimeException] {
      override def accept(ignore: String, value: AnyValue): Unit = if (value == Values.NO_VALUE) hasNull = true
    })
    hasNull
  }
  override def rewrite(f: (Expression) => Expression) = f(PointFunction(data.rewrite(f)))

  override def arguments = data.arguments

  override def symbolTableDependencies = data.symbolTableDependencies

  override def toString = "Point(" + data + ")"
}
