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

import org.neo4j.cypher.internal.symbols.{MapType, SymbolTable}
import org.neo4j.cypher.internal.pipes.QueryState
import org.neo4j.graphdb.{Node, Relationship, PropertyContainer}
import org.neo4j.cypher.internal.commands.expressions.Expression
import org.neo4j.cypher.internal.helpers.{MapSupport, IsMap}
import org.neo4j.cypher.CypherTypeException
import collection.Map
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.spi.{QueryContext, Operations}

case class MapPropertySetAction(element: Expression, mapExpression: Expression)
  extends UpdateAction with GraphElementPropertyFunctions with MapSupport {

  def exec(context: ExecutionContext, state: QueryState) = {
    val qtx = state.query

    /* Make the map expression look like a map */
    val map = mapExpression(context)(state) match {
      case IsMap(createMapFrom) => propertyKeyMap(qtx, createMapFrom(state.query))
      case x                    =>
        throw new CypherTypeException("Expected %s to be a map, but it was :`%s`".format(element, x))
    }

    /*Find the property container we'll be working on*/
    element(context)(state) match {
      case n: Node         => setProperties(qtx, qtx.nodeOps, n, map)
      case r: Relationship => setProperties(qtx, qtx.relationshipOps, r, map)
      case x               =>
        throw new CypherTypeException("Expected %s to be a node or a relationship, but it was :`%s`".format(element, x))
    }

    Iterator(context)
  }

  def propertyKeyMap(qtx: QueryContext, map: Map[String, Any]): Map[Long, Any] = {
    var builder = Map.newBuilder[Long, Any]

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


  def setProperties[T <: PropertyContainer](qtx: QueryContext, ops: Operations[T], target: T, map: Map[Long, Any]) {
    /*Set all map values on the property container*/
    for ( (k, v) <- map) {
      if (null == v)
        ops.removeProperty(target, k)
      else
        ops.setProperty(target, k, makeValueNeoSafe(v))
    }

    /*Remove all other properties from the property container*/
    for ( propertyKeyId <- ops.propertyKeyIds(target) if !map.contains(propertyKeyId) ) {
      ops.removeProperty(target, propertyKeyId)
    }
  }

  def identifiers = Nil

  def children = Seq(element, mapExpression)

  def rewrite(f: (Expression) => Expression) = MapPropertySetAction(element.rewrite(f), mapExpression.rewrite(f))

  def throwIfSymbolsMissing(symbols: SymbolTable) {
    element.evaluateType(MapType(), symbols)
    mapExpression.evaluateType(MapType(), symbols)
  }

  def symbolTableDependencies = element.symbolTableDependencies ++ mapExpression.symbolTableDependencies
}

