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

import org.neo4j.cypher.internal.commands.expressions.{Literal, Identifier, Expression}
import org.neo4j.cypher.internal.symbols.{SymbolTable, TypeSafe}
import org.neo4j.graphdb.PropertyContainer
import org.neo4j.cypher.internal.pipes.ExecutionContext
import collection.Map
import org.neo4j.cypher.internal.helpers.{IsMap, CollectionSupport}

object NamedExpectation {
  def apply(name: String): NamedExpectation = NamedExpectation(name, Map.empty)

  def apply(name: String, properties: Map[String, Expression]): NamedExpectation =
    new NamedExpectation(name, Identifier(name), properties)
}

case class NamedExpectation(name: String, e:Expression, properties: Map[String, Expression])
  extends GraphElementPropertyFunctions
  with CollectionSupport
  with TypeSafe {

  /*
  The expectation expression for a node can either be an expression that returns a node,
  and if so, we check our given properties map to see if we have a match.

  If the expectation returns a map, we'll use the map as our property expectations
  */
  def getExpectations(ctx: ExecutionContext): Map[String, Expression] = e match {
    case _: Identifier => properties
    case _             =>
      e(ctx) match {
        case _: PropertyContainer => properties
        case IsMap(m)             => m.map {
          case (k, v) => k -> Literal(v)
        }
      }
  }


  def compareWithExpectations(pc: PropertyContainer, ctx: ExecutionContext): Boolean = getExpectations(ctx).forall {
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