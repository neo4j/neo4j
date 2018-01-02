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
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Expression, Identifier}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{Effects, _}
import org.neo4j.cypher.internal.compiler.v2_3.helpers.{IsMap, MapSupport}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_3.spi.{Operations, QueryContext}
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.CypherTypeException
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.graphdb.{Node, PropertyContainer, Relationship}

import scala.collection.Map

case class MapPropertySetAction(element: Expression, mapExpression: Expression, removeOtherProps:Boolean)
  extends SetAction with MapSupport {

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
    if (removeOtherProps) {
      for (propertyKeyId <- properties) {
        ops.removeProperty(id(target), propertyKeyId)
      }
    }
  }

  def identifiers = Nil

  def children = Seq(element, mapExpression)

  def rewrite(f: (Expression) => Expression): MapPropertySetAction = MapPropertySetAction(element.rewrite(f), mapExpression.rewrite(f), removeOtherProps)

  def symbolTableDependencies = element.symbolTableDependencies ++ mapExpression.symbolTableDependencies

  private def id(x: PropertyContainer) = x match {
    case n: Node         => n.getId
    case r: Relationship => r.getId
  }

  def localEffects(symbols: SymbolTable) = element match {
    case i: Identifier => symbols.identifiers(i.entityName) match {
      case _: NodeType => Effects(WritesAnyNodeProperty)
      case _: RelationshipType => Effects(WritesAnyRelationshipProperty)
      case _ => Effects()
    }
    case _ => Effects(WritesAnyNodeProperty, WritesAnyRelationshipProperty)
  }

}

