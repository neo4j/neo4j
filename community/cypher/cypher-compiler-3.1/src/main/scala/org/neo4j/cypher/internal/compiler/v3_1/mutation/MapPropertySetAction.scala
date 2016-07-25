/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.mutation

import org.neo4j.cypher.internal.compiler.v3_1._
import org.neo4j.cypher.internal.compiler.v3_1.commands.expressions.{Expression, Variable}
import org.neo4j.cypher.internal.compiler.v3_1.executionplan.{Effects, _}
import org.neo4j.cypher.internal.compiler.v3_1.helpers.{IsMap, MapSupport}
import org.neo4j.cypher.internal.compiler.v3_1.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v3_1.spi.{Operations, QueryContext}
import org.neo4j.cypher.internal.compiler.v3_1.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v3_1.CypherTypeException
import org.neo4j.cypher.internal.frontend.v3_1.symbols._
import org.neo4j.graphdb.{Node, PropertyContainer, Relationship}

import scala.collection.Map

case class MapPropertySetAction(element: Expression, mapExpression: Expression, removeOtherProps:Boolean)
  extends SetAction with MapSupport {

  private val needsExclusiveLock = element match {
    case Variable(elementName) =>
      Expression.mapExpressionHasPropertyReadDependency(elementName, mapExpression)
    case _ => false
  }

  def exec(context: ExecutionContext, state: QueryState) = {
    val qtx = state.query
    val item = element(context)(state)
    if (item != null) {
      val ops = item match {
        case n: Node =>
          qtx.nodeOps
        case r: Relationship =>
          qtx.relationshipOps
        case x =>
          throw new CypherTypeException("Expected %s to be a node or a relationship, but it was :`%s`".format(element, x))
      }

      val itemId = id(item)
      if (needsExclusiveLock) ops.acquireExclusiveLock(itemId)

      /* Make the map expression look like a map */
      val map = mapExpression(context)(state) match {
        case IsMap(createMapFrom) => propertyKeyMap(qtx, createMapFrom(state.query))
        case x =>
          throw new CypherTypeException(s"Expected $mapExpression to be a map, but it was :`$x`")
      }

      setProperties(qtx, ops, itemId, map)

      if (needsExclusiveLock) ops.releaseExclusiveLock(itemId)
    }
    Iterator(context)
  }

  def propertyKeyMap(qtx: QueryContext, map: Map[String, Any]): Map[Int, Any] = {
    var builder = Map.newBuilder[Int, Any]

    for ( (k,v) <- map ) {
      if ( null == v ) {
        val optPropertyKeyId = qtx.getOptPropertyKeyId(k)
        if ( optPropertyKeyId.isDefined ) {
          builder += optPropertyKeyId.get -> v
        }
      }
      else {
        builder += qtx.getOrCreatePropertyKeyId(k) -> v
      }
    }

    builder.result()
  }

  def setProperties[T <: PropertyContainer](qtx: QueryContext, ops: Operations[T], itemId: Long, map: Map[Int, Any]) {
    /*Set all map values on the property container*/
    for ( (k, v) <- map) {
      if (null == v)
        ops.removeProperty(itemId, k)
      else
        ops.setProperty(itemId, k, makeValueNeoSafe(v))
    }

    val properties = ops.propertyKeyIds(itemId).filterNot(map.contains).toSet

    /*Remove all other properties from the property container*/
    if (removeOtherProps) {
      for (propertyKeyId <- properties) {
        ops.removeProperty(itemId, propertyKeyId)
      }
    }
  }

  def variables = Nil

  def children = Seq(element, mapExpression)

  def rewrite(f: (Expression) => Expression): MapPropertySetAction =
    MapPropertySetAction(element.rewrite(f).asInstanceOf[Variable], mapExpression.rewrite(f), removeOtherProps)

  def symbolTableDependencies = element.symbolTableDependencies ++ mapExpression.symbolTableDependencies

  private def id(x: Any) = x match {
    case n: Node         => n.getId
    case r: Relationship => r.getId
    case _ =>
      throw new CypherTypeException(s"Expected $x to be a node or a relationship")
  }

  def localEffects(symbols: SymbolTable) = element match {
    case v: Variable => symbols.variables(v.entityName) match {
      case _: NodeType => Effects(WriteAnyNodeProperty)
      case _: RelationshipType => Effects(WriteAnyRelationshipProperty)
      case _ => Effects()
    }
    case _ => Effects(WriteAnyNodeProperty, WriteAnyRelationshipProperty)
  }

}

