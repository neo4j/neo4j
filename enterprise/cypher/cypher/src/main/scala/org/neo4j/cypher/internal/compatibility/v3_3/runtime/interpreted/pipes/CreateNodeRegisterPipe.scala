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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.pipes

import org.neo4j.collection.primitive.PrimitiveLongCollections
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.Expression
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.helpers.{IsMap, PrimitiveLongHelper}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.PrimitiveExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.mutation.makeValueNeoSafe
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.{LazyLabel, Pipe, QueryState}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ExecutionContext, PipelineInformation}
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.Id
import org.neo4j.cypher.internal.frontend.v3_3.CypherTypeException
import org.neo4j.cypher.internal.spi.v3_3.QueryContext
import org.neo4j.graphdb.{Node, Relationship}

import scala.collection.Map

case class CreateNodeRegisterPipe(ident: String, pipelineInformation: PipelineInformation,
                                  labels: Seq[LazyLabel], properties: Option[Expression])
                                   (val id: Id = new Id) extends Pipe {

  private val offset = pipelineInformation.getLongOffsetFor(ident)

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val nodeId = state.query.createNodeId()
    val context = PrimitiveExecutionContext(pipelineInformation)
    setProperties(context, state, nodeId)
    setLabels(context, state, nodeId)

    state.copyArgumentStateTo(context)
    context.setLongAt(offset, nodeId)
    PrimitiveLongHelper.map(PrimitiveLongCollections.singleton(nodeId), nodeId => context)
  }

  private def setProperties(context: ExecutionContext, state: QueryState, nodeId: Long) = {
    properties.foreach { (expr) =>
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

  protected def handleNull(key: String) {
    // do nothing
  }

  private def setLabels(context: ExecutionContext, state: QueryState, nodeId: Long) = {
    val labelIds = labels.map(_.getOrCreateId(state.query).id)
    state.query.setLabelsOnNode(nodeId, labelIds.iterator)
  }
}
