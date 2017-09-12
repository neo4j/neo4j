/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.pipes

import java.util.function.BiConsumer

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.Expression
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.helpers.IsMap
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.mutation.makeValueNeoSafe
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.{LazyType, Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.Id
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ExecutionContext, PipelineInformation}
import org.neo4j.cypher.internal.frontend.v3_3.{CypherTypeException, InvalidSemanticsException}
import org.neo4j.cypher.internal.spi.v3_3.QueryContext
import org.neo4j.graphdb.{Node, Relationship}
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

abstract class BaseRelationshipSlottedPipe(src: Pipe, RelationshipKey: String, startNode: Int, typ: LazyType, endNode: Int,
                                           pipelineInformation: PipelineInformation,
                                           properties: Option[Expression])
  extends PipeWithSource(src) {

  private val offset = pipelineInformation.getLongOffsetFor(RelationshipKey)

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    input.map {
      row =>{
        val start = getNode(row, startNode)
        val end = getNode(row, endNode)
        val typeId = typ.typ(state.query)
        val relationship = state.query.createRelationship(start, end, typeId)

        relationship.getType // we do this to make sure the relationship is loaded from the store into this object

        setProperties(row, state, relationship.getId)

        row.setLongAt(offset, relationship.getId)
        row
      }
    }


  private def getNode(row: ExecutionContext, offset: Int): Long ={
    row.getLongAt(offset)
  }

  private def setProperties(context: ExecutionContext, state: QueryState, relId: Long) = {
    properties.foreach { expr =>
      expr(context, state) match {
        case _: Node | _: Relationship =>
          throw new CypherTypeException("Parameter provided for relationship creation is not a Map")
        case IsMap(f) =>
          val propertiesMap: MapValue = f(state.query)
          propertiesMap.foreach {
            new BiConsumer[String, AnyValue] {
              override def accept(k: String, v: AnyValue): Unit = setProperty(relId, k, v, state.query)
            }
          }
        case _ =>
          throw new CypherTypeException("Parameter provided for relationship creation is not a Map")
      }
    }
  }

  private def setProperty(relId: Long, key: String, value: AnyValue, qtx: QueryContext) {
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

case class CreateRelationshipSlottedPipe(src: Pipe, RelationshipKey: String, startNode: Int, typ: LazyType, endNode: Int,
                                         pipelineInformation: PipelineInformation,
                                         properties: Option[Expression])
                                        (val id: Id = new Id)
  extends BaseRelationshipSlottedPipe(src, RelationshipKey, startNode, typ: LazyType, endNode, pipelineInformation, properties) {
  override protected def handleNull(key: String) {
    //do nothing
  }
}

case class MergeCreateRelationshipSlottedPipe(src: Pipe, RelationshipKey: String, startNode: Int, typ: LazyType, endNode: Int,
                                              pipelineInformation: PipelineInformation,
                                              properties: Option[Expression])
                                             (val id: Id = new Id)
  extends BaseRelationshipSlottedPipe(src, RelationshipKey, startNode, typ: LazyType, endNode, pipelineInformation, properties) {

  override protected def handleNull(key: String) {
    //merge cannot use null properties, since in that case the match part will not find the result of the create
    throw new InvalidSemanticsException(s"Cannot merge relationship using null property value for $key")
  }
}
