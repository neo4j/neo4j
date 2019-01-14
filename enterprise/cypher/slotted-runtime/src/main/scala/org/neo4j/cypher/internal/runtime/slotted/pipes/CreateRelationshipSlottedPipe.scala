/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import java.util.function.BiConsumer

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{Slot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.slotted.helpers.SlottedPipeBuilderUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.runtime.{LenientCreateRelationship, QueryContext}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{LazyType, Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, IsMap, makeValueNeoSafe}
import org.neo4j.cypher.internal.util.v3_4.attribution.Id
import org.neo4j.cypher.internal.util.v3_4.{CypherTypeException, InternalException, InvalidSemanticsException}
import org.neo4j.graphdb.{Node, Relationship}
import org.neo4j.kernel.api.StatementConstants.{NO_SUCH_NODE, NO_SUCH_RELATIONSHIP}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue

abstract class BaseCreateRelationshipSlottedPipe(src: Pipe,
                                                 relationshipKey: String,
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
  private val offset = slots.getLongOffsetFor(relationshipKey)

  //===========================================================================
  // Runtime code
  //===========================================================================

  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    input.map {
      def handleMissingNode(nodeSlot: Slot) =
        if (state.lenientCreateRelationship) NO_SUCH_RELATIONSHIP
        else throw new InternalException(LenientCreateRelationship.errorMsg(relationshipKey, slots.getAliasOf(nodeSlot)))

      row => {
        val start = getStartNodeFunction(row)
        val end = getEndNodeFunction(row)
        val typeId = typ.typ(state.query)

        val relationshipId =
          if (start == NO_SUCH_NODE) handleMissingNode(startNode)
          else if (end == NO_SUCH_NODE) handleMissingNode(endNode)
          else {
            val relationship = state.query.createRelationship(start, end, typeId)
            relationship.`type`() // we do this to make sure the relationship is loaded from the store into this object
            setProperties(row, state, relationship.id())
            relationship.id()
          }

        row.setLongAt(offset, relationshipId)
        row
      }
    }

  private def setProperties(context: ExecutionContext, state: QueryState, relId: Long): Unit = {
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

  private def setProperty(relId: Long, key: String, value: AnyValue, qtx: QueryContext): Unit = {
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
  extends BaseCreateRelationshipSlottedPipe(src, RelationshipKey, startNode, typ: LazyType, endNode, slots, properties) {
  override protected def handleNull(key: String): Unit = {
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
  extends BaseCreateRelationshipSlottedPipe(src, RelationshipKey, startNode, typ: LazyType, endNode, slots, properties) {

  override protected def handleNull(key: String): Unit = {
    //merge cannot use null properties, since in that case the match part will not find the result of the create
    throw new InvalidSemanticsException(s"Cannot merge relationship using null property value for $key")
  }
}
