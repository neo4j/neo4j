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
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.{Effects, WriteAnyNodeProperty, WriteAnyRelationshipProperty}
import org.neo4j.cypher.internal.compiler.v3_0.helpers.{CastSupport, IsMap, MapSupport}
import org.neo4j.cypher.internal.compiler.v3_0.mutation.makeValueNeoSafe
import org.neo4j.cypher.internal.compiler.v3_0.spi.{Operations, QueryContext}
import org.neo4j.cypher.internal.frontend.v3_0.CypherTypeException
import org.neo4j.graphdb.{Node, PropertyContainer, Relationship}

import scala.collection.Map

abstract class SetPropertiesFromMapPipe[T <: PropertyContainer](src: Pipe, name: String,  expression: Expression, removeOtherProps: Boolean, pipeMonitor: PipeMonitor)
  extends PipeWithSource(src, pipeMonitor) with RonjaPipe with MapSupport {

  private val needsExclusiveLock = Expression.mapExpressionHasPropertyReadDependency(name, expression)

  override protected def internalCreateResults(input: Iterator[ExecutionContext],
                                               state: QueryState): Iterator[ExecutionContext] = {
    val qtx = state.query
    val ops = operations(qtx)

    input.map { row =>

      val item = row.get(name).get
      if (item != null) {
        val itemId = getId(item)
        if (needsExclusiveLock) ops.acquireExclusiveLock(itemId)

        /* Make the map expression look like a map */
        val map = expression(row)(state) match {
          case IsMap(createMapFrom) => {
            propertyKeyMap(qtx, createMapFrom(qtx))
          }
          case x => throw new CypherTypeException(s"Expected $expression to be a map, but it was :`$x`")
        }

        /*Find the property container we'll be working on*/
        setProperties(qtx, ops, itemId, map)

        if (needsExclusiveLock) ops.releaseExclusiveLock(itemId)
      }
      row
    }
  }

  private def propertyKeyMap(qtx: QueryContext, map: Map[String, Any]): Map[Int, Any] = {
    var builder = Map.newBuilder[Int, Any]

    for ((k, v) <- map) {
      if (v == null) {
        val optPropertyKeyId = qtx.getOptPropertyKeyId(k)
        if (optPropertyKeyId.isDefined) {
          builder += optPropertyKeyId.get -> v
        }
      }
      else {
        builder += qtx.getOrCreatePropertyKeyId(k) -> v
      }
    }

    builder.result()
  }

  def getId(value: Any): Long
  def operations(query: QueryContext): Operations[T]
  def operatorName: String

  private def setProperties(qtx: QueryContext, ops: Operations[T], itemId: Long, map: Map[Int, Any]) {
    /*Set all map values on the property container*/
    for ((k, v) <- map) {
      if (v == null)
        ops.removeProperty(itemId, k)
      else
        ops.setProperty(itemId, k, makeValueNeoSafe(v))
    }

    val properties = ops.propertyKeyIds(itemId).filterNot(map.contains).toSet

    /*Remove all other properties from the property container ( SET n = {prop1: ...})*/
    if (removeOtherProps) {
      for (propertyKeyId <- properties) {
        ops.removeProperty(itemId, propertyKeyId)
      }
    }
  }

  override def planDescriptionWithoutCardinality = src.planDescription.andThen(this.id, operatorName, variables)

  override def symbols = src.symbols
}

case class SetNodePropertiesFromMapPipe(src: Pipe, name: String, expression: Expression, removeOtherProps: Boolean)
                                       (val estimatedCardinality: Option[Double] = None)
                                       (implicit pipeMonitor: PipeMonitor)
  extends SetPropertiesFromMapPipe[Node](src, name, expression, removeOtherProps, pipeMonitor) {

  override def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))

  override def dup(sources: List[Pipe]): Pipe = {
    val (onlySource :: Nil) = sources
    SetNodePropertiesFromMapPipe(onlySource, name, expression, removeOtherProps)(estimatedCardinality)
  }

  override def localEffects = Effects(WriteAnyNodeProperty)

  override def getId(value: Any) = CastSupport.castOrFail[Node](value).getId

  override def operations(query: QueryContext) = query.nodeOps

  override def operatorName: String = "SetNodePropertiesFromMap"
}

case class SetRelationshipPropertiesFromMapPipe(src: Pipe, name: String, expression: Expression, removeOtherProps: Boolean)
                                               (val estimatedCardinality: Option[Double] = None)
                                               (implicit pipeMonitor: PipeMonitor)
  extends SetPropertiesFromMapPipe[Relationship](src, name, expression, removeOtherProps, pipeMonitor) {

  override def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))

  override def dup(sources: List[Pipe]): Pipe = {
    val (onlySource :: Nil) = sources
    SetRelationshipPropertiesFromMapPipe(onlySource, name, expression, removeOtherProps)(estimatedCardinality)
  }

  override def localEffects = Effects(WriteAnyRelationshipProperty)

  override def getId(value: Any) = CastSupport.castOrFail[Relationship](value).getId

  override def operations(query: QueryContext) = query.relationshipOps

  override def operatorName: String = "SetRelationshipPropertiesFromMap"
}




