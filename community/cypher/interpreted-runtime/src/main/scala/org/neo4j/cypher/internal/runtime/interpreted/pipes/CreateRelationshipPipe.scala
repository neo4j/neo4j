/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted._
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.function.ThrowingBiConsumer
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.{NodeValue, RelationshipValue}
import org.opencypher.v9_0.util.attribution.Id
import org.opencypher.v9_0.util.{CypherTypeException, InternalException, InvalidSemanticsException}

abstract class BaseRelationshipPipe(src: Pipe, key: String, startNode: String, typ: LazyType, endNode: String,
                                    properties: Option[Expression])
  extends PipeWithSource(src) with GraphElementPropertyFunctions {

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    input.map(createRelationship(_, state))

  private def createRelationship(context: ExecutionContext, state: QueryState): ExecutionContext = {
    val start = getNode(context, startNode)
    val end = getNode(context, endNode)
    val typeId = typ.typ(state.query)
    val relationship = state.query.createRelationship(start.id(), end.id(), typeId)
    setProperties(context, state, relationship.id())
    context += key -> relationship
  }

  private def getNode(row: ExecutionContext, name: String): NodeValue =
    row.get(name) match {
      case Some(n: NodeValue) => n
      case x => throw new InternalException(s"Expected to find a node at $name but found nothing $x")
    }

  private def setProperties(context: ExecutionContext, state: QueryState, relId: Long) = {
    properties.foreach { expr =>
      expr(context, state) match {
        case _: NodeValue | _: RelationshipValue =>
          throw new CypherTypeException("Parameter provided for relationship creation is not a Map")
        case IsMap(map) =>
          map(state.query).foreach(new ThrowingBiConsumer[String, AnyValue, RuntimeException] {
            override def accept(k: String, v: AnyValue): Unit = setProperty(relId, k, v, state.query)
          })
        case _ =>
          throw new CypherTypeException("Parameter provided for relationship creation is not a Map")
      }
    }
  }

  private def setProperty(relId: Long, key: String, value: AnyValue, qtx: QueryContext) {
    //do not set properties for null values
    if (value == Values.NO_VALUE) {
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
                                 (val id: Id = Id.INVALID_ID)
  extends BaseRelationshipPipe(src, key, startNode, typ, endNode, properties) {
  override protected def handleNull(key: String) {
    //do nothing
  }
}

case class MergeCreateRelationshipPipe(src: Pipe, key: String, startNode: String, typ: LazyType, endNode: String,
                                       properties: Option[Expression])
                                      (val id: Id = Id.INVALID_ID)
  extends BaseRelationshipPipe(src, key, startNode, typ, endNode, properties) {

  override protected def handleNull(key: String) {
    //merge cannot use null properties, since in that case the match part will not find the result of the create
    throw new InvalidSemanticsException(s"Cannot merge relationship using null property value for $key")
  }
}
