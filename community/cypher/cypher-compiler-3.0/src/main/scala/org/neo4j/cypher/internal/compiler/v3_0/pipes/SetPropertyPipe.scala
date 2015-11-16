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
import org.neo4j.cypher.internal.compiler.v3_0.commands.expressions.{Property, Expression}
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.{SetGivenRelationshipProperty, SetGivenNodeProperty, Effects}
import org.neo4j.cypher.internal.compiler.v3_0.helpers.{CastSupport, CollectionSupport}
import org.neo4j.cypher.internal.compiler.v3_0.mutation.{GraphElementPropertyFunctions, makeValueNeoSafe}
import org.neo4j.cypher.internal.compiler.v3_0.spi.{Operations, QueryContext}
import org.neo4j.graphdb.{Relationship, PropertyContainer, Node}


object SetPropertyPipe {
  val PROPERTY_MISSING = -1L
}

abstract class SetPropertyPipe[T <: PropertyContainer](src: Pipe, name: String, propertyKey: LazyPropertyKey, expression: Expression, pipeMonitor: PipeMonitor)
  extends PipeWithSource(src, pipeMonitor) with RonjaPipe with GraphElementPropertyFunctions with CollectionSupport {

  private val needsExclusiveLock = Expression.hasPropertyReadDependency(name, expression, propertyKey.name)

  override protected def internalCreateResults(input: Iterator[ExecutionContext],
                                               state: QueryState): Iterator[ExecutionContext] = {
    input.map { row =>
      val value = row.get(name).get
      if (value != null) setProperty(row, state, getId(value))
      row
    }
  }

  def getId(value: Any): Long
  def operations(query: QueryContext): Operations[T]
  def operatorName: String

  private def setProperty(context: ExecutionContext, state: QueryState, entityId: Long) = {
    val queryContext = state.query
    val maybePropertyKey = propertyKey.id(queryContext).map(_.id) // if the key was already looked up
    val propertyId = maybePropertyKey
        .getOrElse(queryContext.getOrCreatePropertyKeyId(propertyKey.name)) // otherwise create it
    val ops = operations(queryContext)

    if (needsExclusiveLock) ops.acquireExclusiveLock(entityId)

    val value = makeValueNeoSafe(expression(context)(state))
    if (value == null) ops.removeProperty(entityId, propertyId)
    else ops.setProperty(entityId, propertyId, value)

    if (needsExclusiveLock) ops.releaseExclusiveLock(entityId)
  }

  override def planDescriptionWithoutCardinality = src.planDescription.andThen(this.id, operatorName, variables)

  override def symbols = src.symbols
}

case class SetNodePropertyPipe(src: Pipe, name: String, propertyKey: LazyPropertyKey, expression: Expression)
                              (val estimatedCardinality: Option[Double] = None)
                              (implicit pipeMonitor: PipeMonitor)
  extends SetPropertyPipe[Node](src, name, propertyKey, expression, pipeMonitor){

  override def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))

  override def dup(sources: List[Pipe]): Pipe = {
    val (onlySource :: Nil) = sources
    SetNodePropertyPipe(onlySource, name, propertyKey,  expression)(estimatedCardinality)
  }

  override def localEffects = Effects(SetGivenNodeProperty(propertyKey.name))

  override def getId(value: Any) = CastSupport.castOrFail[Node](value).getId

  override def operations(query: QueryContext) = query.nodeOps

  override def operatorName: String = "SetNodeProperty"
}

case class SetRelationshipPropertyPipe(src: Pipe, name: String, propertyKey: LazyPropertyKey, expression: Expression)
                              (val estimatedCardinality: Option[Double] = None)
                              (implicit pipeMonitor: PipeMonitor)
  extends SetPropertyPipe[Relationship](src, name, propertyKey, expression, pipeMonitor){

  override def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))

  override def dup(sources: List[Pipe]): Pipe = {
    val (onlySource :: Nil) = sources
    SetRelationshipPropertyPipe(onlySource, name, propertyKey,  expression)(estimatedCardinality)
  }

  override def localEffects = Effects(SetGivenRelationshipProperty(propertyKey.name))

  override def getId(value: Any) = CastSupport.castOrFail[Relationship](value).getId

  override def operations(query: QueryContext) = query.relationshipOps

  override def operatorName: String = "SetRelationshipProperty"
}
