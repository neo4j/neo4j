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
package org.neo4j.cypher.internal.compiler.v2_0.mutation

import org.neo4j.cypher.internal.compiler.v2_0._
import commands.expressions.Expression
import pipes.QueryState
import symbols._
import org.neo4j.cypher.CypherTypeException
import org.neo4j.cypher.internal.compiler.v2_0.spi.{QueryContext, Operations}
import org.neo4j.graphdb.{Node, Relationship, PropertyContainer}
import collection.Map
import org.neo4j.cypher.internal.compiler.v2_0.helpers.{IsMap, MapSupport}

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


  def setProperties[T <: PropertyContainer](qtx: QueryContext, ops: Operations[T], target: T, map: Map[Int, Any]) {
    /*Set all map values on the property container*/
    for ( (k, v) <- map) {
      if (null == v)
        ops.removeProperty(id(target), k)
      else
        ops.setProperty(id(target), k, makeValueNeoSafe(v))
    }

    val properties = ops.propertyKeyIds(id(target)).filterNot(map.contains).toSet

    /*Remove all other properties from the property container*/
    for ( propertyKeyId <- properties ) {
      ops.removeProperty(id(target), propertyKeyId)
    }
  }

  def identifiers = Nil

  def children = Seq(element, mapExpression)

  def rewrite(f: (Expression) => Expression) = MapPropertySetAction(element.rewrite(f), mapExpression.rewrite(f))

  def symbolTableDependencies = element.symbolTableDependencies ++ mapExpression.symbolTableDependencies

  private def id(x: PropertyContainer) = x match {
    case n: Node         => n.getId
    case r: Relationship => r.getId
  }

}

