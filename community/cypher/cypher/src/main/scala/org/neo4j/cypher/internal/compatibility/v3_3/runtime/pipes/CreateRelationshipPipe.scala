/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.Expression
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.helpers.IsMap
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.mutation.{GraphElementPropertyFunctions, makeValueNeoSafe}
import org.neo4j.cypher.internal.compiler.v3_3.helpers.ListSupport
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.Id
import org.neo4j.cypher.internal.frontend.v3_3.{CypherTypeException, InternalException, InvalidSemanticsException}
import org.neo4j.cypher.internal.spi.v3_3.QueryContext
import org.neo4j.graphdb.{Node, Relationship}

import scala.collection.Map

abstract class BaseRelationshipPipe(src: Pipe, key: String, startNode: String, typ: LazyType, endNode: String,
                                    properties: Option[Expression])
  extends PipeWithSource(src) with GraphElementPropertyFunctions with ListSupport {

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    input.map(createRelationship(_, state))

  private def createRelationship(context: ExecutionContext, state: QueryState): ExecutionContext = {
    val start = getNode(context, startNode)
    val end = getNode(context, endNode)
    val typeId = typ.typ(state.query)
    val relationship = state.query.createRelationship(start.getId, end.getId, typeId)
    relationship.getType // we do this to make sure the relationship is loaded from the store into this object
    setProperties(context, state, relationship.getId)
    context += key -> relationship
  }

  private def getNode(row: ExecutionContext, name: String): Node =
    row.get(name) match {
      case Some(n: Node) => n
      case x => throw new InternalException(s"Expected to find a node at $name but found nothing $x")
    }

  private def setProperties(context: ExecutionContext, state: QueryState, relId: Long) = {
    properties.foreach { expr =>
      expr(context)(state) match {
        case _: Node | _: Relationship =>
          throw new CypherTypeException("Parameter provided for relationship creation is not a Map")
        case IsMap(f) =>
          val propertiesMap: Map[String, Any] = f(state.query)
          propertiesMap.foreach {
            case (k, v) => setProperty(relId, k, v, state.query)
          }
        case _ =>
          throw new CypherTypeException("Parameter provided for relationship creation is not a Map")
      }
    }
  }

  private def setProperty(relId: Long, key: String, value: Any, qtx: QueryContext) {
    //do not set properties for null values
    if (value == null) {
      handleNull(key: String)
    } else {
      val propertyKeyId = qtx.getOrCreatePropertyKeyId(key)
      qtx.relationshipOps.setProperty(relId, propertyKeyId, makeValueNeoSafe(value))
    }
  }

  protected def handleNull(key: String): Unit
}

case class CreateRelationshipPipe(src: Pipe,
                                  key: String, startNode: String, typ: LazyType, endNode: String,
                                  properties: Option[Expression])
                                 (val id: Id = new Id)
  extends BaseRelationshipPipe(src, key, startNode, typ, endNode, properties) {
  override protected def handleNull(key: String) {
    //do nothing
  }
}

case class MergeCreateRelationshipPipe(src: Pipe, key: String, startNode: String, typ: LazyType, endNode: String,
                                       properties: Option[Expression])
                                      (val id: Id = new Id)
  extends BaseRelationshipPipe(src, key, startNode, typ, endNode, properties) {

  override protected def handleNull(key: String) {
    //merge cannot use null properties, since in that case the match part will not find the result of the create
    throw new InvalidSemanticsException(s"Cannot merge relationship using null property value for $key")
  }
}
