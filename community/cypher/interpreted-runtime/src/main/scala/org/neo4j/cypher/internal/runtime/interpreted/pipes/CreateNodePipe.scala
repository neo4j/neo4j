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
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.{NodeValue, RelationshipValue}
import org.opencypher.v9_0.util.attribution.Id
import org.opencypher.v9_0.util.{CypherTypeException, InvalidSemanticsException}

abstract class BaseCreateNodePipe(src: Pipe, key: String, labels: Seq[LazyLabel], properties: Option[Expression])
  extends PipeWithSource(src) with GraphElementPropertyFunctions {

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    input.map(createNode(_, state))

  private def createNode(context: ExecutionContext, state: QueryState): ExecutionContext = {
    val node = state.query.createNode()
    setProperties(context, state, node.id())
    setLabels(context, state, node.id())
    context += key -> node
  }

  private def setProperties(context: ExecutionContext, state: QueryState, nodeId: Long) = {
    properties.foreach { expr =>
      expr(context, state) match {
        case _: NodeValue | _: RelationshipValue =>
          throw new CypherTypeException("Parameter provided for node creation is not a Map")
        case IsMap(map) =>
          map(state.query).foreach(new ThrowingBiConsumer[String, AnyValue, RuntimeException] {
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

case class CreateNodePipe(src: Pipe, key: String, labels: Seq[LazyLabel], properties: Option[Expression])
                         (val id: Id = Id.INVALID_ID) extends BaseCreateNodePipe(src, key, labels, properties) {

  override protected def handleNull(key: String) {
    // do nothing
  }
}

case class MergeCreateNodePipe(src: Pipe, key: String, labels: Seq[LazyLabel], properties: Option[Expression])
                              (val id: Id = Id.INVALID_ID) extends BaseCreateNodePipe(src, key, labels, properties) {
  override protected def handleNull(key: String) {
    //merge cannot use null properties, since in that case the match part will not find the result of the create
    throw new InvalidSemanticsException(s"Cannot merge node using null property value for $key")
  }
}
