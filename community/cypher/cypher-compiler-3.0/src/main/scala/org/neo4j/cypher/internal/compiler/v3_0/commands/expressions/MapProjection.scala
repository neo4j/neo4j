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
package org.neo4j.cypher.internal.compiler.v3_0.commands.expressions

import org.neo4j.cypher.internal.compiler.v3_0._
import org.neo4j.cypher.internal.compiler.v3_0.helpers.IsMap
import org.neo4j.cypher.internal.compiler.v3_0.mutation.GraphElementPropertyFunctions
import org.neo4j.cypher.internal.compiler.v3_0.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v3_0.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v3_0.symbols._

import scala.collection.Map

case class MapProjection(id: String, includeAllProps: Boolean, data: Map[String, Expression]) extends Expression with GraphElementPropertyFunctions {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = {
    val propsMap = ctx(id) match {
      case null => return null
      case IsMap(m) => if (includeAllProps) m(state.query) else Map.empty
    }
    val literalEntries = data.map {
      case (k, e) => (k, e(ctx))
    }
    propsMap.toMap ++ literalEntries.toMap
  }

  def rewrite(f: (Expression) => Expression) = f(MapProjection(id, includeAllProps, data.rewrite(f)))

  def arguments = data.values.toSeq

  def calculateType(symbols: SymbolTable): CypherType = {
    data.values.foreach(_.evaluateType(CTAny, symbols))
    CTMap
  }

  def symbolTableDependencies = data.symboltableDependencies + id

  override def toString = s"$id{.*, " + data.mkString + "}"
}
