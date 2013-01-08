/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.mutation

import org.neo4j.cypher.internal.symbols.SymbolTable
import org.neo4j.cypher.internal.pipes.{QueryState, ExecutionContext}
import org.neo4j.graphdb.PropertyContainer
import org.neo4j.cypher.internal.commands.expressions.{Expression, Property}

case class PropertySetAction(prop: Property, e: Expression)
  extends UpdateAction with GraphElementPropertyFunctions {
  val Property(entityKey, propertyKey) = prop

  def exec(context: ExecutionContext, state: QueryState) = {
    val value = makeValueNeoSafe(e(context))
    val entity = context(entityKey).asInstanceOf[PropertyContainer]

    value match {
      case null => entity.removeProperty(propertyKey)
      case _    => entity.setProperty(propertyKey, value)
    }

    state.propertySet.increase()

    Stream(context)
  }

  def identifiers = Seq.empty

  def filter(f: (Expression) => Boolean): Seq[Expression] = prop.filter(f) ++ e.filter(f)

  def rewrite(f: (Expression) => Expression): UpdateAction = PropertySetAction(prop, e.rewrite(f))

  def assertTypes(symbols: SymbolTable) {
    e.checkTypes(symbols)
  }

  def symbolTableDependencies = prop.symbolTableDependencies ++ e.symbolTableDependencies
}