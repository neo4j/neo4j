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
package org.neo4j.cypher.internal.compiler.v3_0.mutation

import org.neo4j.cypher.internal.compiler.v3_0._
import org.neo4j.cypher.internal.compiler.v3_0.commands.expressions._
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v3_0.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v3_0.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v3_0.InvalidArgumentException
import org.neo4j.graphdb.{Node, Relationship}

case class PropertySetAction(prop: Property, valueExpression: Expression)
  extends SetAction {

  val Property(mapExpr, propertyKey) = prop

  private val needsExclusiveLock = mapExpr match {
    case Variable(entityName) => Expression.hasPropertyReadDependency(entityName, valueExpression, propertyKey.name)
    case _ => true // we don't know so better safe than sorry!
  }

  def localEffects(symbols: SymbolTable) = Effects.propertyWrite(mapExpr, symbols)(propertyKey.name)

  def exec(context: ExecutionContext, state: QueryState) = {
    implicit val s = state

    val qtx = state.query

    val expr = mapExpr(context)
    if (expr != null) {
      val (id, ops) = expr match {
        case (e: Relationship) => (e.getId, qtx.relationshipOps)
        case (e: Node) => (e.getId, qtx.nodeOps)
        case _ => throw new InvalidArgumentException(s"The expression $mapExpr should have been a node or a relationship, but got $expr")
      }

      if (needsExclusiveLock) ops.acquireExclusiveLock(id)

      makeValueNeoSafe(valueExpression(context)) match {
        case null => propertyKey.getOptId(qtx).foreach(ops.removeProperty(id, _))
        case value => ops.setProperty(id, propertyKey.getOrCreateId(qtx), value)
      }

      if (needsExclusiveLock) ops.releaseExclusiveLock(id)
    }

    Iterator(context)
  }

  def variables = Nil

  def children = Seq(prop, valueExpression)

  def rewrite(f: (Expression) => Expression): PropertySetAction = PropertySetAction(prop, valueExpression.rewrite(f))

  def symbolTableDependencies = prop.symbolTableDependencies ++ valueExpression.symbolTableDependencies
}
