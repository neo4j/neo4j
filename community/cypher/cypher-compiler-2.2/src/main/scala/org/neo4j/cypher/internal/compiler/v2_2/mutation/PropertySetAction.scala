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
package org.neo4j.cypher.internal.compiler.v2_2.mutation

import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.commands.expressions._
import org.neo4j.cypher.internal.compiler.v2_2.executionplan._
import org.neo4j.cypher.internal.compiler.v2_2.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_2.symbols.{NodeType, RelationshipType, SymbolTable}
import org.neo4j.graphdb.{Node, Relationship}
import org.neo4j.helpers.ThisShouldNotHappenError

case class PropertySetAction(prop: Property, e: Expression)
  extends UpdateAction with GraphElementPropertyFunctions {

  val Property(mapExpr, propertyKey) = prop

  def localEffects(symbols: SymbolTable) = Effects.propertyWrite(mapExpr, symbols)(propertyKey.name)

  def exec(context: ExecutionContext, state: QueryState) = {
    implicit val s = state

    val qtx = state.query

    val expr = mapExpr(context)
    if (expr != null) {
      val (id, ops) = expr match {
        case (e: Relationship) => (e.getId, qtx.relationshipOps)
        case (e: Node) => (e.getId, qtx.nodeOps)
        case _ => throw new ThisShouldNotHappenError("Stefan", "This should be a node or a relationship")
      }

      makeValueNeoSafe(e(context)) match {
        case null => propertyKey.getOptId(qtx).foreach(ops.removeProperty(id, _))
        case value => ops.setProperty(id, propertyKey.getOrCreateId(qtx), value)
      }
    }

    Iterator(context)
  }

  def identifiers = Nil

  def children = Seq(prop, e)

  def rewrite(f: (Expression) => Expression): UpdateAction = PropertySetAction(prop, e.rewrite(f))

  def symbolTableDependencies = prop.symbolTableDependencies ++ e.symbolTableDependencies
}
