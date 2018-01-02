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
package org.neo4j.cypher.internal.compiler.v2_3.mutation

import org.neo4j.cypher.internal.compiler.v2_3._
import commands.expressions._
import commands.values.KeyToken
import org.neo4j.cypher.internal.compiler.v2_3.symbols.TypeSafe
import org.neo4j.cypher.internal.frontend.v2_3.helpers.Eagerly
import pipes.QueryState
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.compiler.v2_3.spi.Operations
import org.neo4j.graphdb.{Relationship, Node, PropertyContainer}
import collection.Map
import org.neo4j.cypher.internal.compiler.v2_3.helpers._

object NamedExpectation {
  def apply(name: String): NamedExpectation = NamedExpectation(name, Map.empty)

  def apply(name: String, properties: Map[String, Expression]): NamedExpectation =
    NamedExpectation(name, properties, Seq.empty)

  def apply(name: String, e: Expression, properties: Map[String, Expression]): NamedExpectation =
    new NamedExpectation(name, e, properties, Seq.empty)

  def apply(name: String, properties: Map[String, Expression], labels: Seq[KeyToken]): NamedExpectation =
    new NamedExpectation(name, Identifier(name), properties, labels)
}

case class NamedExpectation(name: String, e: Expression, properties: Map[String, Expression],
                            labels: Seq[KeyToken])
  extends GraphElementPropertyFunctions
  with CollectionSupport
  with TypeSafe {

  case class DataExpectation(properties: Map[String, Expression], labels: Seq[KeyToken])

  /*
  The expectation expression for a node can either be an expression that returns a node,
  and if so, we check our given properties map to see if we have a match.

  If the expectation returns a map, we'll use the map as our property expectations
  */
  def getExpectations(ctx: ExecutionContext, state: QueryState): DataExpectation = {
    val expectedProps = e match {
      case _: Identifier =>
        properties
      case _             =>
        e(ctx)(state) match {
          case _: PropertyContainer =>
            properties
          case IsMap(f)             =>
            val m = f(state.query)
            Eagerly.immutableMapValues(m, Literal)
        }
    }
    DataExpectation(expectedProps, labels)
  }


  def compareWithExpectations(pc: PropertyContainer, ctx: ExecutionContext, state: QueryState): Boolean = {
    val expectations = getExpectations(ctx, state)

    pc match {
      case n: Node         => compareWithExpectation(n, state.query.nodeOps, ctx, expectations, state)
      case n: Relationship => compareWithExpectation(n, state.query.relationshipOps, ctx, expectations, state)
    }
  }

  private def compareWithExpectation[T <: PropertyContainer](x: T,
                                                             ops: Operations[T],
                                                             ctx: ExecutionContext,
                                                             expectations: DataExpectation,
                                                             state: QueryState): Boolean = {
    val propsOk = expectations.properties.forall {
      case ("*", expression) =>
        getMapFromExpression(expression(ctx)(state)).forall {
          case (k, value) => state.query.getOptPropertyKeyId(k).exists(key => {
            val expectedValue = ops.getProperty(id(x), key)
            (expectedValue, value) match {
              case (IsCollection(l), IsCollection(r)) => l == r
              case (l, r) => l == r
            }
          })
        }

      // case (k, _) if !ops.hasProperty(x, state.query.getOrCreatePropertyKeyId(k)) => false
      case (k, _) if state.query.getOptPropertyKeyId(k).map(!ops.hasProperty(id(x), _)).getOrElse(true) => false

      case (k, exp) =>
        val expectationValue = exp(ctx)(state)
        val elementValue = ops.getProperty(id(x), state.query.getPropertyKeyId(k))

        (expectationValue, elementValue) match {
          case (IsCollection(l), IsCollection(r)) => l == r
          case (l, r)                             => l == r
        }
    }

    val labelsOk = x match {
      case node: Node =>
        val qtx      = state.query
        val nodeId   = node.getId
        val labelIds = labels.map(_.getOrCreateId(state.query))
        labelIds.forall( qtx.isLabelSetOnNode(_, nodeId) )
      case _ =>
        true
    }

    propsOk && labelsOk
  }

  private def id(x: PropertyContainer) = x match {
    case n: Node         => n.getId
    case r: Relationship => r.getId
  }

  def bare = properties.isEmpty && labels.isEmpty

  def symbolTableDependencies = properties.symboltableDependencies

  override def toString: String = {
    val nameStr = if (UnNamedNameGenerator.notNamed(name)) s"(${name.drop(9)})" else name
    val props = if (properties.isEmpty)
      ""
    else
      " " + properties.map {
        case (k, v) => k.toString + ": " + v.toString
      }.mkString(",")

    "(" + nameStr + props + ")"
  }
}
