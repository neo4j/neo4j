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
import org.neo4j.cypher.internal.compiler.v3_0.helpers.{CollectionSupport, IsMap}
import org.neo4j.cypher.internal.compiler.v3_0.mutation.{makeValueNeoSafe, GraphElementPropertyFunctions}
import org.neo4j.cypher.internal.frontend.v3_0.CypherTypeException
import org.neo4j.cypher.internal.frontend.v3_0.symbols._
import org.neo4j.graphdb.{Node, Relationship}

import scala.collection.Map

case class CreateNodePipe(src: Pipe, key: String, labels: Seq[LazyLabel], properties: Option[Expression])(val estimatedCardinality: Option[Double] = None)
                           (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(src, pipeMonitor) with RonjaPipe with GraphElementPropertyFunctions with CollectionSupport {

  protected def internalCreateResults(input: Iterator[ExecutionContext],
                                      state: QueryState): Iterator[ExecutionContext] = {
    input.map { row =>
      createNode(row, state)
    }
  }

  private def createNode(context: ExecutionContext, state: QueryState): ExecutionContext = {
    val node = state.query.createNode()
    setProperties(context, state, node.getId)
    setLabels(context, state, node.getId)

    context += key -> node
  }

 private def setProperties(context: ExecutionContext, state: QueryState, nodeId: Long) = {
   properties.foreach { expr =>
     expr(context)(state) match {
       case _: Node | _: Relationship => throw new
           CypherTypeException("Parameter provided for node creation is not a Map")
       case IsMap(f) =>
         val propertiesMap: Map[String, Any] = f(state.query)
         propertiesMap.foreach {
           case (k, v) =>
             //do not set properties for null values
             if (v != null) {
               val propertyKeyId = state.query.getOrCreatePropertyKeyId(k)
               state.query.nodeOps.setProperty(nodeId, propertyKeyId, makeValueNeoSafe(v))
             }
         }
       case _ =>
         throw new CypherTypeException("Parameter provided for node creation is not a Map")
     }
   }
 }

  private def setLabels(context: ExecutionContext, state: QueryState, nodeId: Long) = {
    val labelIds = labels.map(l => {
      val maybeLabelId = l.id(state.query).map(_.id)
      maybeLabelId getOrElse state.query.getOrCreateLabelId(l.name)
    })
    state.query.setLabelsOnNode(nodeId, labelIds.iterator)
  }

  def planDescriptionWithoutCardinality = src.planDescription.andThen(this.id, "CreateNode", identifiers)

  def symbols = src.symbols.add(key, CTNode)

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))

  override def dup(sources: List[Pipe]): Pipe = {
    val (onlySource :: Nil) = sources
    CreateNodePipe(onlySource, key, labels, properties)(estimatedCardinality)
  }

  override def localEffects = if (labels.isEmpty)
    Effects(CreatesAnyNode)
  else
    Effects(CreatesNodesWithLabels(labels.map(_.name).toSet))
}
