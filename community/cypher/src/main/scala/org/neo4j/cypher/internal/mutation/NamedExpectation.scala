/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import org.neo4j.cypher.internal.commands.IterableSupport
import org.neo4j.cypher.internal.commands.expressions.Expression
import org.neo4j.cypher.internal.symbols.{SymbolTable, TypeSafe}
import org.neo4j.graphdb.PropertyContainer
import org.neo4j.cypher.internal.pipes.ExecutionContext
import collection.Map

case class NamedExpectation(name: String, properties: Map[String, Expression])
  extends GraphElementPropertyFunctions
  with IterableSupport
  with TypeSafe {
  def this(name: String) = this(name, Map.empty)

  def compareWithExpectations(pc: PropertyContainer, ctx: ExecutionContext): Boolean = properties.forall {
    case ("*", expression) => getMapFromExpression(expression(ctx)).forall {
      case (k, value) => pc.hasProperty(k) && pc.getProperty(k) == value
    }
    case (k, exp)          =>
      if (!pc.hasProperty(k)) false
      else {
        val expectationValue = exp(ctx)
        val elementValue = pc.getProperty(k)

        if (expectationValue == elementValue) true
        else isCollection(expectationValue) && isCollection(elementValue) && makeTraversable(expectationValue).toList == makeTraversable(elementValue).toList
      }
  }

  def symbolTableDependencies = symbolTableDependencies(properties)

  def assertTypes(symbols: SymbolTable) {
    checkTypes(properties, symbols)
  }
}