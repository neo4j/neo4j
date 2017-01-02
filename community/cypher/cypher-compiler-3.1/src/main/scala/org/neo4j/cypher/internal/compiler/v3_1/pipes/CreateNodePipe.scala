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
package org.neo4j.cypher.internal.compiler.v3_1.pipes

import org.neo4j.cypher.internal.compiler.v3_1.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_1.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v3_1.executionplan.{CreatesAnyNode, CreatesNodesWithLabels, Effects}
import org.neo4j.cypher.internal.compiler.v3_1.helpers.{IsMap, ListSupport}
import org.neo4j.cypher.internal.compiler.v3_1.mutation.{GraphElementPropertyFunctions, makeValueNeoSafe}
import org.neo4j.cypher.internal.compiler.v3_1.spi.QueryContext
import org.neo4j.cypher.internal.frontend.v3_1.symbols._
import org.neo4j.cypher.internal.frontend.v3_1.{CypherTypeException, InvalidSemanticsException}
import org.neo4j.graphdb.{Node, Relationship}

import scala.collection.Map

abstract class BaseCreateNodePipe(src: Pipe, key: String, labels: Seq[LazyLabel], properties: Option[Expression], pipeMonitor: PipeMonitor)
  extends PipeWithSource(src, pipeMonitor) with RonjaPipe with GraphElementPropertyFunctions with ListSupport {

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    input.map(createNode(_, state))

  private def createNode(context: ExecutionContext, state: QueryState): ExecutionContext = {
    val node = state.query.createNode()
    setProperties(context, state, node.getId)
    setLabels(context, state, node.getId)
    context += key -> node
  }

  private def setProperties(context: ExecutionContext, state: QueryState, nodeId: Long) = {
    properties.foreach { expr =>
      expr(context)(state) match {
        case _: Node | _: Relationship =>
          throw new CypherTypeException("Parameter provided for node creation is not a Map")
        case IsMap(f) =>
          val propertiesMap: Map[String, Any] = f(state.query)
          propertiesMap.foreach {
            case (k, v) => setProperty(nodeId, k, v, state.query)
          }
        case _ =>
          throw new CypherTypeException("Parameter provided for node creation is not a Map")
      }
    }
  }

  private def setProperty(nodeId: Long, key: String, value: Any, qtx: QueryContext) {
    //do not set properties for null values
    if (value == null) {
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

  def symbols = src.symbols.add(key, CTNode)

  override def localEffects = if (labels.isEmpty)
    Effects(CreatesAnyNode)
  else
    Effects(CreatesNodesWithLabels(labels.map(_.name).toSet))
}

case class CreateNodePipe(src: Pipe, key: String, labels: Seq[LazyLabel], properties: Option[Expression])(val estimatedCardinality: Option[Double] = None)
                         (implicit pipeMonitor: PipeMonitor) extends BaseCreateNodePipe(src, key, labels, properties, pipeMonitor) {

  override def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))

  override def planDescriptionWithoutCardinality = src.planDescription.andThen(this.id, "CreateNode", variables)

  override def dup(sources: List[Pipe]): Pipe = {
    val (onlySource :: Nil) = sources
    CreateNodePipe(onlySource, key, labels, properties)(estimatedCardinality)
  }

  override protected def handleNull(key: String) {
    // do nothing
  }
}

case class MergeCreateNodePipe(src: Pipe, key: String, labels: Seq[LazyLabel], properties: Option[Expression])(val estimatedCardinality: Option[Double] = None)
                         (implicit pipeMonitor: PipeMonitor) extends BaseCreateNodePipe(src, key, labels, properties, pipeMonitor) {

  override def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))

  override def planDescriptionWithoutCardinality = src.planDescription.andThen(this.id, "MergeCreateNode", variables)

  override def dup(sources: List[Pipe]): Pipe = {
    val (onlySource :: Nil) = sources
    MergeCreateNodePipe(onlySource, key, labels, properties)(estimatedCardinality)
  }

  override protected def handleNull(key: String) {
    //merge cannot use null properties, since in that case the match part will not find the result of the create
    throw new InvalidSemanticsException(s"Cannot merge node using null property value for $key")
  }
}
