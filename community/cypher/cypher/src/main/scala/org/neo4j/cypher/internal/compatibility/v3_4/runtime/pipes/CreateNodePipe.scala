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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes

import java.util.function.BiConsumer

import org.neo4j.cypher.internal.apa.v3_4.{CypherTypeException, InvalidSemanticsException}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions.Expression
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.helpers.IsMap
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.mutation.{GraphElementPropertyFunctions, makeValueNeoSafe}
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlanId
import org.neo4j.cypher.internal.spi.v3_4.QueryContext
import org.neo4j.helpers.ValueUtils
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.{EdgeValue, NodeValue}

abstract class BaseCreateNodePipe(src: Pipe, key: String, labels: Seq[LazyLabel], properties: Option[Expression])
  extends PipeWithSource(src) with GraphElementPropertyFunctions {

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    input.map(createNode(_, state))

  private def createNode(context: ExecutionContext, state: QueryState): ExecutionContext = {
    val node = state.query.createNode()
    setProperties(context, state, node.getId)
    setLabels(context, state, node.getId)
    context += key -> ValueUtils.fromNodeProxy(node)
  }

  private def setProperties(context: ExecutionContext, state: QueryState, nodeId: Long) = {
    properties.foreach { expr =>
      expr(context, state) match {
        case _: NodeValue | _: EdgeValue =>
          throw new CypherTypeException("Parameter provided for node creation is not a Map")
        case IsMap(map) =>
          map(state.query).foreach(new BiConsumer[String, AnyValue] {
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
                         (val id: LogicalPlanId = LogicalPlanId.DEFAULT) extends BaseCreateNodePipe(src, key, labels, properties) {

  override protected def handleNull(key: String) {
    // do nothing
  }
}

case class MergeCreateNodePipe(src: Pipe, key: String, labels: Seq[LazyLabel], properties: Option[Expression])
                              (val id: LogicalPlanId = LogicalPlanId.DEFAULT) extends BaseCreateNodePipe(src, key, labels, properties) {
  override protected def handleNull(key: String) {
    //merge cannot use null properties, since in that case the match part will not find the result of the create
    throw new InvalidSemanticsException(s"Cannot merge node using null property value for $key")
  }
}
