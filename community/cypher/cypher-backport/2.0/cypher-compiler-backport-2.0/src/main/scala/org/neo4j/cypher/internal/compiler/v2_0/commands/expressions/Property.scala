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
package org.neo4j.cypher.internal.compiler.v2_0.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_0._
import commands.values.KeyToken
import pipes.QueryState
import symbols._
import org.neo4j.cypher.EntityNotFoundException
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.graphdb.NotFoundException
import org.neo4j.cypher.internal.compiler.v2_0.helpers.IsMap

case class Property(mapExpr: Expression, propertyKey: KeyToken)
  extends Expression with Product with Serializable
{
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = mapExpr(ctx) match {
    case null           => null
    case IsMap(mapFunc) => try {
      mapFunc(state.query).getOrElse(propertyKey.name, null)
    } catch {
      case _: EntityNotFoundException => null
      case _: NotFoundException       => null
    }
    case _              => throw new ThisShouldNotHappenError("Andres", "Need something with properties")
  }

  def rewrite(f: (Expression) => Expression) = f(Property(mapExpr.rewrite(f), propertyKey.rewrite(f)))

  override def children = Seq(mapExpr, propertyKey)

  def arguments = Seq(mapExpr)

  def calculateType(symbols: SymbolTable) =
    throw new ThisShouldNotHappenError("Andres", "This class should override evaluateType, and this method should never be run")

  override def evaluateType(expectedType: CypherType, symbols: SymbolTable) = {
    mapExpr.evaluateType(CTMap, symbols)
    expectedType
  }

  def symbolTableDependencies = mapExpr.symbolTableDependencies
}
