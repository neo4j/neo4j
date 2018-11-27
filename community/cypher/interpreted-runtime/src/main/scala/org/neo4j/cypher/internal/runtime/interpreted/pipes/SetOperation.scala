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

import org.neo4j.cypher.internal.runtime.interpreted._
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.{ExpressionCursors, Operations, QueryContext}
import org.neo4j.cypher.internal.v3_5.util.{CypherTypeException, InvalidArgumentException}
import org.neo4j.function.ThrowingBiConsumer
import org.neo4j.internal.kernel.api.{NodeCursor, RelationshipScanCursor}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual._

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

sealed trait SetOperation {

  def set(executionContext: ExecutionContext, state: QueryState): Unit

  def name: String

  def needsExclusiveLock: Boolean
}

object SetOperation {

  private[pipes] def toMap(executionContext: ExecutionContext, state: QueryState, expression: Expression) = {
    /* Make the map expression look like a map */
    expression(executionContext, state) match {
      case IsMap(map) => propertyKeyMap(state.query, map(state))
      case x => throw new CypherTypeException(s"Expected $expression to be a map, but it was :`$x`")
    }
  }

  private def propertyKeyMap(qtx: QueryContext, map: MapValue): Map[Int, AnyValue] = {
    val builder = Map.newBuilder[Int, AnyValue]
    val setKeys = new ArrayBuffer[String]()
    val setValues = new ArrayBuffer[AnyValue]()

    map.foreach(new ThrowingBiConsumer[String, AnyValue, RuntimeException] {
      override def accept(k: String, v: AnyValue): Unit = {
        if (v == Values.NO_VALUE) {
          val optPropertyKeyId = qtx.getOptPropertyKeyId(k)
          if (optPropertyKeyId.isDefined) {
            builder += optPropertyKeyId.get -> v
          }
        }
        else {
          setKeys += k
          setValues += v
        }
      }
    })

    // Adding property keys is O(|totalPropertyKeyIds|^2) per call, so
    // batch creation is way faster is graphs with many propertyKeyIds
    val propertyIds = qtx.getOrCreatePropertyKeyIds(setKeys.toArray)
    for (i <- propertyIds.indices)
      builder += (propertyIds(i) -> setValues(i))

    builder.result()
  }
}

abstract class AbstractSetPropertyOperation extends SetOperation {

  protected def setProperty[T, CURSOR](context: ExecutionContext,
                                       state: QueryState,
                                       cursor: CURSOR,
                                       ops: Operations[T, CURSOR],
                                       itemId: Long,
                                       propertyKey: LazyPropertyKey,
                                       expression: Expression): Unit = {

    val queryContext = state.query
    val maybePropertyKey = propertyKey.id(queryContext).map(_.id) // if the key was already looked up
    val propertyId = maybePropertyKey
      .getOrElse(queryContext.getOrCreatePropertyKeyId(propertyKey.name)) // otherwise create it

    val value = makeValueNeoSafe(expression(context, state))

    if (value == Values.NO_VALUE) {
      if (ops.hasProperty(itemId, propertyId, cursor, state.cursors.propertyCursor))
        ops.removeProperty(itemId, propertyId)
    }
    else ops.setProperty(itemId, propertyId, value)
  }
}

abstract class SetEntityPropertyOperation[T, CURSOR](itemName: String,
                                                     propertyKey: LazyPropertyKey,
                                                     expression: Expression)
  extends AbstractSetPropertyOperation {

  override def set(executionContext: ExecutionContext, state: QueryState): Unit = {
    val item = executionContext(itemName)
    if (item != Values.NO_VALUE) {
      val itemId = id(item)
      val ops = operations(state.query)
      val cursor = entityCursor(state.cursors)
      if (needsExclusiveLock) ops.acquireExclusiveLock(itemId)

      try {
        setProperty[T, CURSOR](executionContext, state, cursor, ops, itemId, propertyKey, expression)
      } finally if (needsExclusiveLock) ops.releaseExclusiveLock(itemId)
    }
  }

  protected def id(item: Any): Long

  protected def operations(qtx: QueryContext): Operations[T, CURSOR]

  protected def entityCursor(cursors: ExpressionCursors): CURSOR
}

case class SetNodePropertyOperation(nodeName: String, propertyKey: LazyPropertyKey,
                                    expression: Expression, needsExclusiveLock: Boolean = true)
  extends SetEntityPropertyOperation[NodeValue, NodeCursor](nodeName, propertyKey, expression) {

  override def name = "SetNodeProperty"

  override protected def id(item: Any) = CastSupport.castOrFail[VirtualNodeValue](item).id()

  override protected def operations(qtx: QueryContext) = qtx.nodeOps

  override protected def entityCursor(cursors: ExpressionCursors): NodeCursor = cursors.nodeCursor
}

case class SetRelationshipPropertyOperation(relName: String, propertyKey: LazyPropertyKey,
                                            expression: Expression, needsExclusiveLock: Boolean = true)
  extends SetEntityPropertyOperation[RelationshipValue, RelationshipScanCursor](relName, propertyKey, expression) {

  override def name = "SetRelationshipProperty"

  override protected def id(item: Any) = CastSupport.castOrFail[VirtualRelationshipValue](item).id()

  override protected def operations(qtx: QueryContext) = qtx.relationshipOps

  override protected def entityCursor(cursors: ExpressionCursors): RelationshipScanCursor = cursors.relationshipScanCursor
}

case class SetPropertyOperation(entityExpr: Expression, propertyKey: LazyPropertyKey, expression: Expression)
  extends AbstractSetPropertyOperation {

  override def name: String = "SetProperty"

  override def set(executionContext: ExecutionContext, state: QueryState) = {
    val resolvedEntity = entityExpr(executionContext, state)
    if (resolvedEntity != Values.NO_VALUE) {
      def setIt[T, CURSOR](entityId: Long, ops: Operations[T, CURSOR], cursor: CURSOR): Unit = {
        // better safe than sorry let's lock the entity
        ops.acquireExclusiveLock(entityId)

        try {
          setProperty(executionContext, state, cursor, ops, entityId, propertyKey, expression)
        } finally ops.releaseExclusiveLock(entityId)
      }

      resolvedEntity match {
        case node: VirtualNodeValue => setIt(node.id(), state.query.nodeOps, state.cursors.nodeCursor)
        case rel: VirtualRelationshipValue => setIt(rel.id(), state.query.relationshipOps, state.cursors.relationshipScanCursor)
        case _ => throw new InvalidArgumentException(
          s"The expression $entityExpr should have been a node or a relationship, but got $resolvedEntity")
      }
    }
  }

  override def needsExclusiveLock = true
}

abstract class SetPropertyFromMapOperation[T, CURSOR](itemName: String,
                                                      expression: Expression,
                                                      removeOtherProps: Boolean) extends SetOperation {

  override def set(executionContext: ExecutionContext, state: QueryState): Unit = {
    val item = executionContext(itemName)
    if (item != Values.NO_VALUE) {
      val ops = operations(state.query)
      val itemId = id(item)
      if (needsExclusiveLock) ops.acquireExclusiveLock(itemId)

      try {
        val map = SetOperation.toMap(executionContext, state, expression)

        setPropertiesFromMap(state.cursors, ops, itemId, map, removeOtherProps)
      } finally if (needsExclusiveLock) ops.releaseExclusiveLock(itemId)
    }
  }

  protected def id(item: Any): Long

  protected def operations(qtx: QueryContext): Operations[T, CURSOR]

  protected def entityCursor(cursors: ExpressionCursors): CURSOR

  private def setPropertiesFromMap(cursors: ExpressionCursors,
                                   ops: Operations[T, CURSOR],
                                   itemId: Long,
                                   map: Map[Int, AnyValue],
                                   removeOtherProps: Boolean): Unit = {

    /*Set all map values on the property container*/
    for ((k, v) <- map) {
      if (v == Values.NO_VALUE)
        ops.removeProperty(itemId, k)
      else
        ops.setProperty(itemId, k, makeValueNeoSafe(v))
    }

    val properties = ops.propertyKeyIds(itemId, entityCursor(cursors), cursors.propertyCursor).filterNot(map.contains).toSet

    /*Remove all other properties from the property container ( SET n = {prop1: ...})*/
    if (removeOtherProps) {
      for (propertyKeyId <- properties) {
        ops.removeProperty(itemId, propertyKeyId)
      }
    }
  }
}

case class SetNodePropertyFromMapOperation(nodeName: String, expression: Expression,
                                           removeOtherProps: Boolean, needsExclusiveLock: Boolean = true)
  extends SetPropertyFromMapOperation[NodeValue, NodeCursor](nodeName, expression, removeOtherProps) {

  override def name = "SetNodePropertyFromMap"

  override protected def id(item: Any) = CastSupport.castOrFail[VirtualNodeValue](item).id()

  override protected def operations(qtx: QueryContext) = qtx.nodeOps

  override protected def entityCursor(cursors: ExpressionCursors): NodeCursor = cursors.nodeCursor
}

case class SetRelationshipPropertyFromMapOperation(relName: String, expression: Expression,
                                                   removeOtherProps: Boolean, needsExclusiveLock: Boolean = true)
  extends SetPropertyFromMapOperation[RelationshipValue, RelationshipScanCursor](relName, expression, removeOtherProps) {

  override def name = "SetRelationshipPropertyFromMap"

  override protected def id(item: Any) = CastSupport.castOrFail[VirtualRelationshipValue](item).id()

  override protected def operations(qtx: QueryContext) = qtx.relationshipOps

  override protected def entityCursor(cursors: ExpressionCursors): RelationshipScanCursor = cursors.relationshipScanCursor
}

case class SetLabelsOperation(nodeName: String, labels: Seq[LazyLabel]) extends SetOperation {

  override def set(executionContext: ExecutionContext, state: QueryState) = {
    val value: AnyValue = executionContext.get(nodeName).get
    if (value != Values.NO_VALUE) {
      val nodeId = CastSupport.castOrFail[VirtualNodeValue](value).id()
      val labelIds = labels.map(_.getOrCreateId(state.query).id)
      state.query.setLabelsOnNode(nodeId, labelIds.iterator)
    }
  }

  override def name = "SetLabels"

  override def needsExclusiveLock = false
}
