/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.KeyToken
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_3.helpers.IsMap
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.{CypherTypeException, EntityNotFoundException}
import org.neo4j.graphdb.{Node, NotFoundException, Relationship}
import org.neo4j.helpers.ThisShouldNotHappenError


case class Property(mapExpr: Expression, propertyKey: KeyToken)
  extends Expression with Product with Serializable
{
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = mapExpr(ctx) match {
    case null => null
    case n: Node =>
      val propId = propertyKey.getOrCreateId(state.query)
      state.query.nodeOps.getProperty(n.getId, propId)
    case r: Relationship =>
      val propId = propertyKey.getOrCreateId(state.query)
      state.query.relationshipOps.getProperty(r.getId, propId)
    case IsMap(mapFunc) => try {
      mapFunc(state.query).getOrElse(propertyKey.name, null)
    } catch {
      case _: EntityNotFoundException => null
      case _: NotFoundException => null
    }
    case other => throw new CypherTypeException(s"Type mismatch: expected a map but was $other")
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

  override def toString = s"$mapExpr.${propertyKey.name}"

  override def localEffects(symbols: SymbolTable): Effects = Effects.propertyRead(mapExpr, symbols)(propertyKey.name)
}
