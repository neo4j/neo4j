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
package org.neo4j.cypher.internal.compiler.v1_9.mutation

import org.neo4j.cypher.internal.compiler.v1_9.commands.expressions.{Literal, Identifier, Expression}
import org.neo4j.cypher.internal.compiler.v1_9.symbols.{SymbolTable, TypeSafe}
import org.neo4j.graphdb.{Relationship, Node, PropertyContainer}
import collection.Map
import org.neo4j.cypher.internal.helpers.{IsCollection, CollectionSupport}
import org.neo4j.cypher.internal.compiler.v1_9.spi.Operations
import org.neo4j.cypher.internal.compiler.v1_9.ExecutionContext
import org.neo4j.cypher.internal.compiler.v1_9.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v1_9.helpers.IsMap

object NamedExpectation {
  def apply(name: String): NamedExpectation = NamedExpectation(name, Map.empty)

  def apply(name: String, properties: Map[String, Expression]): NamedExpectation =
    new NamedExpectation(name, Identifier(name), properties)
}

case class NamedExpectation(name: String, e: Expression, properties: Map[String, Expression])
  extends GraphElementPropertyFunctions
  with CollectionSupport
  with TypeSafe {

  /*
  The expectation expression for a node can either be an expression that returns a node,
  and if so, we check our given properties map to see if we have a match.

  If the expectation returns a map, we'll use the map as our property expectations
  */
  def getExpectations(ctx: ExecutionContext, state:QueryState): Map[String, Expression] = e match {
    case _: Identifier => properties
    case _             =>
      e(ctx)(state) match {
        case _: PropertyContainer => properties
        case IsMap(f)             =>
          val m = f(state.query)
          m.map {
            case (k, v) => k -> Literal(v)
          }
      }
  }


  def compareWithExpectations(pc: PropertyContainer, ctx: ExecutionContext, state:QueryState): Boolean = {
    val expectations = getExpectations(ctx, state)

    pc match {
      case n: Node         => compareWithExpectation(n, state.query.nodeOps, ctx, expectations, state)
      case n: Relationship => compareWithExpectation(n, state.query.relationshipOps, ctx, expectations, state)
    }
  }

  private def compareWithExpectation[T <: PropertyContainer](x: T,
                                                             ops: Operations[T],
                                                             ctx: ExecutionContext,
                                                             expectations: Map[String, Expression],
                                                             state: QueryState): Boolean =
    expectations.forall {
      case ("*", expression) => getMapFromExpression(expression(ctx)(state)).forall {
        case (k, value) => ops.getProperty(x, k) == value
      }

      case (k, _) if !ops.hasProperty(x, k) => false

      case (k, exp) =>
        val expectationValue = exp(ctx)(state)
        val elementValue = ops.getProperty(x, k)

        (expectationValue, elementValue) match {
          case (IsCollection(l), IsCollection(r)) => l == r
          case (l, r)                             => l == r
        }
    }

  def symbolTableDependencies = symbolTableDependencies(properties)

  def throwIfSymbolsMissing(symbols: SymbolTable) {
    throwIfSymbolsMissing(properties, symbols)
  }
}
