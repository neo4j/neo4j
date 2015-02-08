/**
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
package org.neo4j.cypher.internal.compiler.v1_9.commands.expressions

import org.neo4j.cypher.internal.compiler.v1_9.symbols._
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.cypher.internal.compiler.v1_9.helpers.IsMap
import org.neo4j.cypher.internal.compiler.v1_9.ExecutionContext
import org.neo4j.cypher.internal.compiler.v1_9.pipes.QueryState

case class Property(mapExpr: Expression, property: String) extends Expression {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = mapExpr(ctx) match {
    case null           => null
    case IsMap(mapFunc) => mapFunc(state.query).apply(property)
    case _              => throw new ThisShouldNotHappenError("Andres", "Need something with properties")
  }

  def rewrite(f: (Expression) => Expression) = f(Property(mapExpr.rewrite(f), property))

  def children = Seq(mapExpr)

  def calculateType(symbols: SymbolTable) =
    throw new ThisShouldNotHappenError("Andres", "This class should override evaluateType, and this method should never be run")

  override def evaluateType(expectedType: CypherType, symbols: SymbolTable) = {
    mapExpr.evaluateType(MapType(), symbols)
    expectedType
  }

  def symbolTableDependencies = mapExpr.symbolTableDependencies
}
