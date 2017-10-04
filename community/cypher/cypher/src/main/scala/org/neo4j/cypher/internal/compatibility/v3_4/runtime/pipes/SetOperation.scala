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

import org.neo4j.cypher.internal.util.v3_4.{CypherTypeException, InvalidArgumentException}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions.Expression
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.helpers.{CastSupport, IsMap}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.mutation.makeValueNeoSafe
import org.neo4j.cypher.internal.spi.v3_4.{Operations, QueryContext}
import org.neo4j.graphdb.{Node, PropertyContainer, Relationship}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.{EdgeValue, MapValue, NodeValue}

import scala.collection.Map

sealed trait SetOperation {

  def set(executionContext: ExecutionContext, state: QueryState): Unit

  def name: String
}

object SetOperation {

  private[pipes] def toMap(executionContext: ExecutionContext, state: QueryState, expression: Expression) = {
    /* Make the map expression look like a map */
    val qtx = state.query
    expression(executionContext, state) match {
      case IsMap(map) => propertyKeyMap(qtx, map(qtx))
      case x => throw new CypherTypeException(s"Expected $expression to be a map, but it was :`$x`")
    }
  }

  private def propertyKeyMap(qtx: QueryContext, map: MapValue): Map[Int, AnyValue] = {
    var builder = Map.newBuilder[Int, AnyValue]
    map.foreach(new BiConsumer[String, AnyValue] {
      override def accept(k: String, v: AnyValue): Unit = {
        if (v == Values.NO_VALUE) {
          val optPropertyKeyId = qtx.getOptPropertyKeyId(k)
          if (optPropertyKeyId.isDefined) {
            builder += optPropertyKeyId.get -> v
          }
        }
        else {
          builder += qtx.getOrCreatePropertyKeyId(k) -> v
        }
      }
    })

    builder.result()
  }
}

abstract class AbstractSetPropertyOperation extends SetOperation {

  protected def setProperty[T <: PropertyContainer](context: ExecutionContext, state: QueryState, ops: Operations[T],
                                                    itemId: Long, propertyKey: LazyPropertyKey,
                                                    expression: Expression) = {
    val queryContext = state.query
    val maybePropertyKey = propertyKey.id(queryContext).map(_.id) // if the key was already looked up
    val propertyId = maybePropertyKey
      .getOrElse(queryContext.getOrCreatePropertyKeyId(propertyKey.name)) // otherwise create it

    val value = makeValueNeoSafe(expression(context, state))

    if (value == Values.NO_VALUE) {
      if (ops.hasProperty(itemId, propertyId)) ops.removeProperty(itemId, propertyId)
    }
    else ops.setProperty(itemId, propertyId, value)
  }
}

abstract class SetEntityPropertyOperation[T <: PropertyContainer](itemName: String, propertyKey: LazyPropertyKey,
                                                                  expression: Expression)
  extends AbstractSetPropertyOperation {

  private val needsExclusiveLock = Expression.hasPropertyReadDependency(itemName, expression, propertyKey.name)

  override def set(executionContext: ExecutionContext, state: QueryState) = {
    val item = executionContext.get(itemName).get
    if (item != Values.NO_VALUE) {
      val itemId = id(item)
      val ops = operations(state.query)
      if (needsExclusiveLock) ops.acquireExclusiveLock(itemId)

      try {
        setProperty[T](executionContext, state, ops, itemId, propertyKey, expression)
      } finally if (needsExclusiveLock) ops.releaseExclusiveLock(itemId)
    }
  }

  protected def id(item: Any): Long

  protected def operations(qtx: QueryContext): Operations[T]
}

case class SetNodePropertyOperation(nodeName: String, propertyKey: LazyPropertyKey,
                                    expression: Expression)
  extends SetEntityPropertyOperation[Node](nodeName, propertyKey, expression) {

  override def name = "SetNodeProperty"

  override protected def id(item: Any) = CastSupport.castOrFail[NodeValue](item).id()

  override protected def operations(qtx: QueryContext) = qtx.nodeOps
}

case class SetRelationshipPropertyOperation(relName: String, propertyKey: LazyPropertyKey,
                                            expression: Expression)
  extends SetEntityPropertyOperation[Relationship](relName, propertyKey, expression) {

  override def name = "SetRelationshipProperty"

  override protected def id(item: Any) = CastSupport.castOrFail[EdgeValue](item).id()

  override protected def operations(qtx: QueryContext) = qtx.relationshipOps
}

case class SetPropertyOperation(entityExpr: Expression, propertyKey: LazyPropertyKey, expression: Expression)
  extends AbstractSetPropertyOperation {

  override def name: String = "SetProperty"

  override def set(executionContext: ExecutionContext, state: QueryState) = {
    val resolvedEntity = entityExpr(executionContext, state)
    if (resolvedEntity != Values.NO_VALUE) {
      val (entityId, ops) = resolvedEntity match {
        case node: NodeValue => (node.id(), state.query.nodeOps)
        case rel: EdgeValue => (rel.id(), state.query.relationshipOps)
        case _ => throw new InvalidArgumentException(
          s"The expression $entityExpr should have been a node or a relationship, but got $resolvedEntity")
      }
      // better safe than sorry let's lock the entity
      ops.acquireExclusiveLock(entityId)

      try {
        setProperty(executionContext, state, ops, entityId, propertyKey, expression)
      } finally ops.releaseExclusiveLock(entityId)
    }
  }
}

abstract class SetPropertyFromMapOperation[T <: PropertyContainer](itemName: String, expression: Expression,
                                                                   removeOtherProps: Boolean) extends SetOperation {

  private val needsExclusiveLock = Expression.mapExpressionHasPropertyReadDependency(itemName, expression)

  override def set(executionContext: ExecutionContext, state: QueryState) = {
    val item = executionContext.get(itemName).get
    if (item != Values.NO_VALUE) {
      val ops = operations(state.query)
      val itemId = id(item)
      if (needsExclusiveLock) ops.acquireExclusiveLock(itemId)

      try {
        val map = SetOperation.toMap(executionContext, state, expression)

        setPropertiesFromMap(state.query, ops, itemId, map, removeOtherProps)
      } finally if (needsExclusiveLock) ops.releaseExclusiveLock(itemId)
    }
  }

  protected def id(item: Any): Long

  protected def operations(qtx: QueryContext): Operations[T]

  private def setPropertiesFromMap(qtx: QueryContext, ops: Operations[T], itemId: Long,
                                   map: Map[Int, AnyValue], removeOtherProps: Boolean) {

    /*Set all map values on the property container*/
    for ((k, v) <- map) {
      if (v == Values.NO_VALUE)
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
}

case class SetNodePropertyFromMapOperation(nodeName: String, expression: Expression,
                                           removeOtherProps: Boolean)
  extends SetPropertyFromMapOperation[Node](nodeName, expression, removeOtherProps) {

  override def name = "SetNodePropertyFromMap"

  override protected def id(item: Any) = CastSupport.castOrFail[NodeValue](item).id()

  override protected def operations(qtx: QueryContext) = qtx.nodeOps
}

case class SetRelationshipPropertyFromMapOperation(relName: String, expression: Expression,
                                                   removeOtherProps: Boolean)
  extends SetPropertyFromMapOperation[Relationship](relName, expression, removeOtherProps) {

  override def name = "SetRelationshipPropertyFromMap"

  override protected def id(item: Any) = CastSupport.castOrFail[EdgeValue](item).id()

  override protected def operations(qtx: QueryContext) = qtx.relationshipOps
}

case class SetLabelsOperation(nodeName: String, labels: Seq[LazyLabel]) extends SetOperation {

  override def set(executionContext: ExecutionContext, state: QueryState) = {
    val value: AnyValue = executionContext.get(nodeName).get
    if (value != Values.NO_VALUE) {
      val nodeId = CastSupport.castOrFail[NodeValue](value).id()
      val labelIds = labels.map(_.getOrCreateId(state.query).id)
      state.query.setLabelsOnNode(nodeId, labelIds.iterator)
    }
  }

  override def name = "SetLabels"
}
