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

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{LazyLabel, Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, IsMap, makeValueNeoSafe}
import org.opencypher.v9_0.util.attribution.Id
import org.opencypher.v9_0.util.{CypherTypeException, InvalidSemanticsException}
import org.neo4j.function.ThrowingBiConsumer
import org.neo4j.graphdb.{Node, Relationship}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

abstract class BaseCreateNodeSlottedPipe(source: Pipe,
                                         ident: String,
                                         slots: SlotConfiguration,
                                         labels: Seq[LazyLabel],
                                         properties: Option[Expression])
  extends PipeWithSource(source) with Pipe {

  private val offset = slots.getLongOffsetFor(ident)

  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    input.map {
      row =>
        val nodeId = state.query.createNodeId()
        setProperties(row, state, nodeId)
        setLabels(row, state, nodeId)
        row.setLongAt(offset, nodeId)
        row
    }
  }

  private def setProperties(context: ExecutionContext, state: QueryState, nodeId: Long) = {
    properties.foreach { (expr) =>
      expr(context, state) match {
        case _: Node | _: Relationship =>
          throw new CypherTypeException("Parameter provided for node creation is not a Map")
        case IsMap(m) =>
          m(state.query).foreach(new ThrowingBiConsumer[String, AnyValue, RuntimeException] {
            override def accept(k: String, v: AnyValue): Unit = setProperty(nodeId, k, v, state.query)
          })
        case _ =>
          throw new CypherTypeException("Parameter provided for node creation is not a Map")
      }
    }
  }

  private def setProperty(nodeId: Long, key: String, value: AnyValue, qtx: QueryContext) {
    //do not set properties for null values
    if (value == Values.NO_VALUE) {
      handleNull(key)
    } else {
      val propertyKeyId = qtx.getOrCreatePropertyKeyId(key)
      qtx.nodeOps.setProperty(nodeId, propertyKeyId, makeValueNeoSafe(value))
    }
  }

  protected def handleNull(key: String): Unit

  private def setLabels(context: ExecutionContext, state: QueryState, nodeId: Long) = {
    val labelIds = labels.map(_.getOrCreateId(state.query).id)
    state.query.setLabelsOnNode(nodeId, labelIds.iterator)
  }
}

case class CreateNodeSlottedPipe(source: Pipe,
                                 ident: String,
                                 slots: SlotConfiguration,
                                 labels: Seq[LazyLabel],
                                 properties: Option[Expression])
                                (val id: Id = Id.INVALID_ID)
  extends BaseCreateNodeSlottedPipe(source, ident, slots, labels, properties) {

  override protected def handleNull(key: String) {
    // do nothing
  }
}

case class MergeCreateNodeSlottedPipe(source: Pipe,
                                      ident: String,
                                      slots: SlotConfiguration,
                                      labels: Seq[LazyLabel],
                                      properties: Option[Expression])
                                     (val id: Id = Id.INVALID_ID)
  extends BaseCreateNodeSlottedPipe(source, ident, slots, labels, properties) {

  override protected def handleNull(key: String) {
    //merge cannot use null properties, since in that case the match part will not find the result of the create
    throw new InvalidSemanticsException(s"Cannot merge node using null property value for $key")
  }
}
