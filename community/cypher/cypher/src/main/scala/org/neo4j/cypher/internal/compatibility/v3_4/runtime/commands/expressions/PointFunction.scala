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

import java.util.function.BiConsumer

import org.neo4j.cypher.internal.aux.v3_4.CypherTypeException
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.helpers.IsMap
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes.QueryState
import org.neo4j.helpers.ValueUtils
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue

case class PointFunction(data: Expression) extends NullInNullOutExpression(data) {
  override def compute(value: AnyValue, ctx: ExecutionContext, state: QueryState): AnyValue = value match {
    case IsMap(mapCreator) =>
      val map = mapCreator(state.query)
      if (containsNull(map)) Values.NO_VALUE
      else ValueUtils.fromMap(map)
    case x => throw new CypherTypeException(s"Expected a map but got $x")
  }

  private def containsNull(map: MapValue) = {
    var hasNull = false
    map.foreach(new BiConsumer[String, AnyValue] {
      override def accept(t: String, u: AnyValue): Unit = if (u == Values.NO_VALUE) hasNull = true
    })
    hasNull
  }
  override def rewrite(f: (Expression) => Expression) = f(PointFunction(data.rewrite(f)))

  override def arguments = data.arguments

  override def symbolTableDependencies = data.symbolTableDependencies

  override def toString = "Point(" + data + ")"
}
