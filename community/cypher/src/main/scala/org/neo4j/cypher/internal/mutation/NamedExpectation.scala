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
import org.neo4j.graphdb.{Relationship, Node, PropertyContainer}
import collection.Map
import org.neo4j.cypher.internal.helpers.{IsCollection, IsMap, CollectionSupport}
import org.neo4j.cypher.internal.spi.Operations
import org.neo4j.cypher.internal.ExecutionContext

object NamedExpectation {
  def apply(name: String, bare: Boolean): NamedExpectation = NamedExpectation(name, Map.empty, bare)

  def apply(name: String, properties: Map[String, Expression], bare: Boolean): NamedExpectation =
    NamedExpectation(name, properties, Literal(Seq.empty), bare)

  def apply(name: String, e: Expression, properties: Map[String, Expression], bare: Boolean): NamedExpectation =
    new NamedExpectation(name, e, properties, Literal(Seq.empty), bare)

  def apply(name: String, properties: Map[String, Expression], labels: Expression, bare: Boolean): NamedExpectation =
    new NamedExpectation(name, Identifier(name), properties, labels, bare)
}

case class NamedExpectation(name: String, e: Expression, properties: Map[String, Expression],
                            labels: Expression, bare: Boolean)
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
        case IsMap(f)             =>
          val m = f(ctx.state.queryContext)
          m.map {
            case (k, v) => k -> Literal(v)
          }
      }
  }


  def compareWithExpectations(pc: PropertyContainer, ctx: ExecutionContext): Boolean = {
    val expectations = getExpectations(ctx)

    pc match {
      case n: Node         => compareWithExpectation(n, ctx.state.queryContext.nodeOps, ctx, expectations)
      case n: Relationship => compareWithExpectation(n, ctx.state.queryContext.relationshipOps, ctx, expectations)
    }
  }

  private def compareWithExpectation[T <: PropertyContainer](x: T,
                                                             ops: Operations[T],
                                                             ctx: ExecutionContext,
                                                             expectations: Map[String, Expression]): Boolean =
    expectations.forall {
      case ("*", expression) => getMapFromExpression(expression(ctx)).forall {
        case (k, value) => ops.hasProperty(x, k) && ops.getProperty(x, k) == value
      }

      case (k, _) if !ops.hasProperty(x, k) => false

      case (k, exp) =>
        val expectationValue = exp(ctx)
        val elementValue = ops.getProperty(x, k)

        (expectationValue, elementValue) match {
          case (IsCollection(l), IsCollection(r)) => l == r
          case (l, r)                             => l == r
        }
    }

  def symbolTableDependencies = properties.symboltableDependencies

  def throwIfSymbolsMissing(symbols: SymbolTable) {
    properties.throwIfSymbolsMissing(symbols)
  }
}