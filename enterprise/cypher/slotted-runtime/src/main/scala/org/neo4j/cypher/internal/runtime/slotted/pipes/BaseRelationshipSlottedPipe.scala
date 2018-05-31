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
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.{Slot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{LazyType, Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, IsMap, makeValueNeoSafe}
import org.opencypher.v9_0.util.attribution.Id
import org.opencypher.v9_0.util.{CypherTypeException, InvalidSemanticsException}
import org.neo4j.cypher.internal.runtime.slotted.helpers.SlottedPipeBuilderUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.function.ThrowingBiConsumer
import org.neo4j.graphdb.{Node, Relationship}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue

abstract class BaseRelationshipSlottedPipe(src: Pipe,
                                           RelationshipKey: String,
                                           startNode: Slot,
                                           typ: LazyType,
                                           endNode: Slot,
                                           slots: SlotConfiguration,
                                           properties: Option[Expression]) extends PipeWithSource(src) {

  //===========================================================================
  // Compile-time initializations
  //===========================================================================
  private val getStartNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(startNode)
  private val getEndNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(endNode)
  private val offset = slots.getLongOffsetFor(RelationshipKey)

  //===========================================================================
  // Runtime code
  //===========================================================================

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    input.map {
      row =>{
        val start = getStartNodeFunction(row)
        val end = getEndNodeFunction(row)
        val typeId = typ.typ(state.query)
        val relationship = state.query.createRelationship(start, end, typeId)

        relationship.`type`() // we do this to make sure the relationship is loaded from the store into this object

        setProperties(row, state, relationship.id())

        row.setLongAt(offset, relationship.id())
        row
      }
    }

  private def setProperties(context: ExecutionContext, state: QueryState, relId: Long) = {
    properties.foreach { expr =>
      expr(context, state) match {
        case _: Node | _: Relationship =>
          throw new CypherTypeException("Parameter provided for relationship creation is not a Map")
        case IsMap(f) =>
          val propertiesMap: MapValue = f(state.query)
          propertiesMap.foreach {
            new ThrowingBiConsumer[String, AnyValue, RuntimeException] {
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
    if (value == Values.NO_VALUE) {
      handleNull(key: String)
    } else {
      val propertyKeyId = qtx.getOrCreatePropertyKeyId(key)
      qtx.relationshipOps.setProperty(relId, propertyKeyId, makeValueNeoSafe(value))
    }
  }

  protected def handleNull(key: String): Unit
}

case class CreateRelationshipSlottedPipe(src: Pipe,
                                         RelationshipKey: String,
                                         startNode: Slot,
                                         typ: LazyType,
                                         endNode: Slot,
                                         slots: SlotConfiguration,
                                         properties: Option[Expression])
                                        (val id: Id = Id.INVALID_ID)
  extends BaseRelationshipSlottedPipe(src, RelationshipKey, startNode, typ: LazyType, endNode, slots, properties) {
  override protected def handleNull(key: String) {
    //do nothing
  }
}

case class MergeCreateRelationshipSlottedPipe(src: Pipe,
                                              RelationshipKey: String,
                                              startNode: Slot,
                                              typ: LazyType,
                                              endNode: Slot,
                                              slots: SlotConfiguration,
                                              properties: Option[Expression])
                                             (val id: Id = Id.INVALID_ID)
  extends BaseRelationshipSlottedPipe(src, RelationshipKey, startNode, typ: LazyType, endNode, slots, properties) {

  override protected def handleNull(key: String) {
    //merge cannot use null properties, since in that case the match part will not find the result of the create
    throw new InvalidSemanticsException(s"Cannot merge relationship using null property value for $key")
  }
}
