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

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.IsMap
import org.neo4j.cypher.internal.runtime.interpreted.GraphElementPropertyFunctions
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

import scala.collection.JavaConverters._
import scala.collection.Map

case class DesugaredMapProjection(id: String, includeAllProps: Boolean, literalExpressions: Map[String, Expression])
  extends Expression with GraphElementPropertyFunctions {

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue = {
    val variableValue = ctx(id)

    val mapOfProperties = variableValue match {
      case v if v == Values.NO_VALUE => return Values.NO_VALUE
      case IsMap(m) => if (includeAllProps) m(state.query) else VirtualValues.emptyMap()
    }
    val mapOfLiteralValues = literalExpressions.map {
      case (k, e) => (k, e(ctx, state))
    }.toMap.asJava


    VirtualValues.combine(mapOfProperties, VirtualValues.map(mapOfLiteralValues))
  }

  override def rewrite(f: (Expression) => Expression) =
    f(DesugaredMapProjection(id, includeAllProps, literalExpressions.rewrite(f)))

  override def arguments = literalExpressions.values.toIndexedSeq

  override def symbolTableDependencies = literalExpressions.symboltableDependencies + id

  override def toString = s"$id{.*, " + literalExpressions.mkString + "}"
}
