/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.pipes

import org.neo4j.cypher.internal.compiler.v3_0.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_0.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.{CreatesAnyNode, CreatesNodesWithLabels, Effects}
import org.neo4j.cypher.internal.compiler.v3_0.helpers.CollectionSupport
import org.neo4j.cypher.internal.compiler.v3_0.mutation.{GraphElementPropertyFunctions, makeValueNeoSafe}
import org.neo4j.cypher.internal.frontend.v3_0.InvalidSemanticsException
import org.neo4j.cypher.internal.frontend.v3_0.symbols._

import scala.collection.Map

case class MergeNodePipe(src: Pipe, key: String, labels: Seq[LazyLabel],
                         properties: Map[LazyPropertyKey, Expression],
                         onCreate: Seq[SetOperation],
                         onMatch: Seq[SetOperation])
                        (val estimatedCardinality: Option[Double] = None)
                        (implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(src, pipeMonitor) with RonjaPipe with GraphElementPropertyFunctions with CollectionSupport {

  protected def internalCreateResults(input: Iterator[ExecutionContext],
                                      state: QueryState): Iterator[ExecutionContext] = {
    input.map { row =>
      val value = row.get(key).get
      if (value == null) {
        createNode(row, state)
        onCreate.foreach(_.set(row, state))
      } else onMatch.foreach(_.set(row, state))

      row
    }
  }

  private def createNode(context: ExecutionContext, state: QueryState): ExecutionContext = {
    val node = state.query.createNode()
    setProperties(context, state, node.getId)
    setLabels(context, state, node.getId)

    context += key -> node
  }

  private def setProperties(context: ExecutionContext, state: QueryState, nodeId: Long) = {
    val queryContext = state.query
    properties.foreach {
      case (propertyKey, expr) =>
        val maybePropertyKey = propertyKey.id(queryContext).map(_.id) // if the key was already looked up
        val propertyId = maybePropertyKey
          .getOrElse(queryContext.getOrCreatePropertyKeyId(propertyKey.name)) // otherwise create it
        val v = expr(context)(state)
        if (v == null) throw new InvalidSemanticsException(s"Cannot merge node using null property value for ${propertyKey.name}")

        //set the actual property
        state.query.nodeOps.setProperty(nodeId, propertyId, makeValueNeoSafe(v))
    }
  }

  private def setLabels(context: ExecutionContext, state: QueryState, nodeId: Long) = {
    val labelIds = labels.map(l => {
      val maybeLabelId = l.id(state.query).map(_.id)
      maybeLabelId getOrElse state.query.getOrCreateLabelId(l.name)
    })
    state.query.setLabelsOnNode(nodeId, labelIds.iterator)
  }

  def planDescriptionWithoutCardinality = src.planDescription.andThen(this.id, "MergeNode", variables)

  def symbols = src.symbols.add(key, CTNode)

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))

  override def dup(sources: List[Pipe]): Pipe = {
    val (onlySource :: Nil) = sources
    MergeNodePipe(onlySource, key, labels, properties, onCreate, onMatch)(estimatedCardinality)
  }

  override def localEffects = if (labels.isEmpty)
    Effects(CreatesAnyNode)
  else
    Effects(CreatesNodesWithLabels(labels.map(_.name).toSet))
}
