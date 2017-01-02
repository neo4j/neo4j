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
import org.neo4j.cypher.internal.compiler.v3_2.helpers.IsMap
import org.neo4j.cypher.internal.compiler.v3_2.mutation.GraphElementPropertyFunctions
import org.neo4j.cypher.internal.compiler.v3_2.pipes.QueryState

import scala.collection.Map

case class DesugaredMapProjection(id: String, includeAllProps: Boolean, literalExpressions: Map[String, Expression])
  extends Expression with GraphElementPropertyFunctions {

  override def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = {
    val variableValue = ctx(id)

    val mapOfProperties = variableValue match {
      case null => return null
      case IsMap(m) => if (includeAllProps) m(state.query) else Map.empty
    }
    val mapOfLiteralValues = literalExpressions.map {
      case (k, e) => (k, e(ctx))
    }.toMap

    mapOfProperties ++ mapOfLiteralValues
  }

  override def rewrite(f: (Expression) => Expression) =
    f(DesugaredMapProjection(id, includeAllProps, literalExpressions.rewrite(f)))

  override def arguments = literalExpressions.values.toIndexedSeq

  override def symbolTableDependencies = literalExpressions.symboltableDependencies + id

  override def toString = s"$id{.*, " + literalExpressions.mkString + "}"
}
