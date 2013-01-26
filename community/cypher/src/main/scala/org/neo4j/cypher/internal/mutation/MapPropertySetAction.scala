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

case class MapPropertySetAction(element: Expression, mapExpression: Expression)
  extends UpdateAction with GraphElementPropertyFunctions with MapSupport {

  def exec(context: ExecutionContext, state: QueryState) = {
    /*Find the property container we'll be working on*/
    val pc = element(context) match {
      case x: PropertyContainer => x
      case x                    =>
        throw new CypherTypeException("Expected %s to be a node or a relationship, but it was :`%s`".format(element, x))
    }

    def setProperties(map:Map[String, Any]) {
      /*Set all map values on the property container*/
      map.foreach(kv => {
        kv match {
          case (k, v) =>
            (v, pc) match {
              case (null, r: Relationship) => state.queryContext.relationshipOps.removeProperty(r, k)
              case (null, n: Node)         => state.queryContext.nodeOps.removeProperty(n, k)
              case (_, n: Node)            => state.queryContext.nodeOps.setProperty(n, k, makeValueNeoSafe(v))
              case (_, r: Relationship)    => state.queryContext.relationshipOps.setProperty(r, k, makeValueNeoSafe(v))
            }
        }
      })

      /*Remove all other properties from the property container*/
      pc match {
        case n:Node=> state.queryContext.nodeOps.propertyKeys(n).foreach {
          case k if map.contains(k) => //Do nothing
          case k                    =>
            state.queryContext.nodeOps.removeProperty(n, k)
        }

        case r:Relationship=> state.queryContext.relationshipOps.propertyKeys(r).foreach {
          case k if map.contains(k) => //Do nothing
          case k                    =>
            state.queryContext.relationshipOps.removeProperty(r, k)
        }
      }
    }

    /*Make the map expression look like a map*/
    mapExpression(context) match {
      case IsMap(createMapFrom) => setProperties(createMapFrom(state.queryContext))
      case x                    =>
        throw new CypherTypeException("Expected %s to be a map, but it was :`%s`".format(element, x))
    }

    Stream(context)
  }


  def identifiers = Seq.empty

  def children = Seq(element, mapExpression)

  def rewrite(f: (Expression) => Expression): UpdateAction = MapPropertySetAction(element.rewrite(f), mapExpression.rewrite(f))

  def throwIfSymbolsMissing(symbols: SymbolTable) {
    element.evaluateType(MapType(), symbols)
    mapExpression.evaluateType(MapType(), symbols)
  }

  def symbolTableDependencies = element.symbolTableDependencies ++ mapExpression.symbolTableDependencies
}

