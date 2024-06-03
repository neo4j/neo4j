/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveRelationshipFromSlotFunctionFor
import org.neo4j.cypher.internal.runtime.CastSupport
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.cypher.internal.runtime.NodeOperations
import org.neo4j.cypher.internal.runtime.Operations
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.RelationshipOperations
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AbstractSetPropertyFromMapOperation
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AbstractSetPropertyOperation
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyLabel
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyPropertyKey
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SetOperation
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.kernel.api.StatementConstants
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue

case class SlottedSetLabelsOperation(nodeSlot: Slot, labels: Seq[LazyLabel]) extends SetOperation {
  private val getFromNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(nodeSlot)

  override def set(executionContext: CypherRow, state: QueryState): Long = {
    val node = getFromNodeFunction.applyAsLong(executionContext)
    if (node != StatementConstants.NO_SUCH_NODE) {
      val labelIds = labels.map(_.getOrCreateId(state.query))
      state.query.setLabelsOnNode(node, labelIds.iterator).toLong
    } else {
      0L
    }
  }

  override def name = "SetLabels"

  override def needsExclusiveLock = false
}

abstract class SlottedSetEntityPropertyOperation[T](propertyKey: LazyPropertyKey, expression: Expression)
    extends AbstractSetPropertyOperation {

  override def set(executionContext: CypherRow, state: QueryState): Long = {
    val itemId = getItemId(executionContext)
    if (itemId != StatementConstants.NO_SUCH_ENTITY) {
      val ops = operations(state.query)
      if (needsExclusiveLock) ops.acquireExclusiveLock(itemId)

      invalidateCachedProperties(executionContext, itemId)

      val wasSet =
        try {
          setProperty[T](
            executionContext,
            state,
            ops,
            itemId,
            propertyKey.name,
            propertyKey.id(state.query),
            expression
          )
        } finally if (needsExclusiveLock) ops.releaseExclusiveLock(itemId)

      if (wasSet) 1L else 0L
    } else {
      0L
    }
  }
  protected def getItemId(executionContext: CypherRow): Long
  protected def operations(qtx: QueryContext): Operations[T, _]
  protected def invalidateCachedProperties(executionContext: CypherRow, id: Long): Unit
}

abstract class SlottedSetEntityPropertiesOperation[T](keys: Array[LazyPropertyKey], values: Array[Expression])
    extends AbstractSetPropertyOperation {

  override def set(executionContext: CypherRow, state: QueryState): Long = {
    val itemId = getItemId(executionContext)
    if (itemId != StatementConstants.NO_SUCH_ENTITY) {
      val ops = operations(state.query)
      if (needsExclusiveLock) ops.acquireExclusiveLock(itemId)

      invalidateCachedProperties(executionContext, itemId)

      try {
        setProperties[T](executionContext, state, ops, itemId, keys, values)
      } finally {
        if (needsExclusiveLock) ops.releaseExclusiveLock(itemId)
      }
    } else {
      0L
    }
  }

  protected def getItemId(executionContext: CypherRow): Long

  protected def operations(qtx: QueryContext): Operations[T, _]

  protected def invalidateCachedProperties(executionContext: CypherRow, id: Long): Unit
}

abstract class SlottedSetNodeOrRelPropertyFromMapOperation[T, CURSOR](expression: Expression, removeOtherProps: Boolean)
    extends AbstractSetPropertyFromMapOperation {

  override def set(executionContext: CypherRow, state: QueryState): Long = {
    val itemId = getItemId(executionContext)
    if (itemId != StatementConstants.NO_SUCH_ENTITY) {
      val ops = operations(state.query)
      if (needsExclusiveLock) ops.acquireExclusiveLock(itemId)

      invalidateCachedProperties(executionContext, itemId)

      try {
        val map = SetOperation.toMap(executionContext, state, expression)

        setPropertiesFromMap(
          state.cursors.propertyCursor,
          state.query,
          entityCursor(state.cursors),
          ops,
          itemId,
          map,
          removeOtherProps
        )
      } finally if (needsExclusiveLock) ops.releaseExclusiveLock(itemId)
    } else {
      0L
    }
  }

  protected def getItemId(executionContext: CypherRow): Long

  protected def id(item: Any): Long

  protected def operations(qtx: QueryContext): Operations[T, CURSOR]

  protected def invalidateCachedProperties(executionContext: CypherRow, id: Long): Unit

  protected def entityCursor(cursors: ExpressionCursors): CURSOR
}

case class SlottedSetNodePropertyOperation(
  nodeSlot: Slot,
  propertyKey: LazyPropertyKey,
  expression: Expression,
  needsExclusiveLock: Boolean = true
) extends SlottedSetEntityPropertyOperation[VirtualNodeValue](propertyKey, expression) {
  private val getFromNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(nodeSlot)

  override def name = "SetNodeProperty"
  override protected def operations(qtx: QueryContext): NodeOperations = qtx.nodeWriteOps

  override protected def invalidateCachedProperties(executionContext: CypherRow, id: Long): Unit =
    executionContext.invalidateCachedNodeProperties(id)

  override protected def getItemId(executionContext: CypherRow): Long =
    getFromNodeFunction.applyAsLong(executionContext)
}

case class SlottedSetNodePropertiesOperation(
  nodeSlot: Slot,
  keys: Array[LazyPropertyKey],
  values: Array[Expression],
  needsExclusiveLock: Boolean = true
) extends SlottedSetEntityPropertiesOperation[VirtualNodeValue](keys, values) {
  private val getFromNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(nodeSlot)

  override def name = "SetNodeProperties"

  override protected def operations(qtx: QueryContext): NodeOperations = qtx.nodeWriteOps

  override protected def invalidateCachedProperties(executionContext: CypherRow, id: Long): Unit =
    executionContext.invalidateCachedNodeProperties(id)

  override protected def getItemId(executionContext: CypherRow): Long =
    getFromNodeFunction.applyAsLong(executionContext)
}

case class SlottedSetRelationshipPropertyOperation(
  relSlot: Slot,
  propertyKey: LazyPropertyKey,
  expression: Expression,
  needsExclusiveLock: Boolean = true
) extends SlottedSetEntityPropertyOperation[VirtualRelationshipValue](propertyKey, expression) {
  private val getFromRelFunction = makeGetPrimitiveRelationshipFromSlotFunctionFor(relSlot)

  override def name = "SetRelationshipProperty"

  override protected def operations(qtx: QueryContext): RelationshipOperations = qtx.relationshipWriteOps

  override protected def invalidateCachedProperties(executionContext: CypherRow, id: Long): Unit =
    executionContext.invalidateCachedRelationshipProperties(id)

  override protected def getItemId(executionContext: CypherRow): Long = getFromRelFunction.applyAsLong(executionContext)
}

case class SlottedSetRelationshipPropertiesOperation(
  relSlot: Slot,
  keys: Array[LazyPropertyKey],
  values: Array[Expression],
  needsExclusiveLock: Boolean = true
) extends SlottedSetEntityPropertiesOperation[VirtualRelationshipValue](keys, values) {
  private val getFromRelFunction = makeGetPrimitiveRelationshipFromSlotFunctionFor(relSlot)

  override def name = "SetRelationshipProperties"

  override protected def operations(qtx: QueryContext): RelationshipOperations = qtx.relationshipWriteOps

  override protected def invalidateCachedProperties(executionContext: CypherRow, id: Long): Unit =
    executionContext.invalidateCachedRelationshipProperties(id)

  override protected def getItemId(executionContext: CypherRow): Long = getFromRelFunction.applyAsLong(executionContext)
}

case class SlottedSetNodePropertyFromMapOperation(
  nodeSlot: Slot,
  expression: Expression,
  removeOtherProps: Boolean,
  needsExclusiveLock: Boolean = true
) extends SlottedSetNodeOrRelPropertyFromMapOperation[VirtualNodeValue, NodeCursor](expression, removeOtherProps) {
  private val getFromNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(nodeSlot)

  override def name = "SetNodePropertyFromMap"

  override protected def id(item: Any): Long = CastSupport.castOrFail[VirtualNodeValue](item).id()

  override protected def operations(qtx: QueryContext): NodeOperations = qtx.nodeWriteOps

  override protected def entityCursor(cursors: ExpressionCursors): NodeCursor = cursors.nodeCursor

  override protected def invalidateCachedProperties(executionContext: CypherRow, id: Long): Unit =
    executionContext.invalidateCachedNodeProperties(id)

  override protected def getItemId(executionContext: CypherRow): Long =
    getFromNodeFunction.applyAsLong(executionContext)
}

case class SlottedSetRelationshipPropertyFromMapOperation(
  relSlot: Slot,
  expression: Expression,
  removeOtherProps: Boolean,
  needsExclusiveLock: Boolean = true
) extends SlottedSetNodeOrRelPropertyFromMapOperation[VirtualRelationshipValue, RelationshipScanCursor](
      expression,
      removeOtherProps
    ) {
  private val getFromRelFunction = makeGetPrimitiveRelationshipFromSlotFunctionFor(relSlot)

  override def name = "SetRelationshipPropertyFromMap"

  override protected def id(item: Any): Long = CastSupport.castOrFail[VirtualRelationshipValue](item).id()

  override protected def operations(qtx: QueryContext): RelationshipOperations = qtx.relationshipWriteOps

  override protected def entityCursor(cursors: ExpressionCursors): RelationshipScanCursor =
    cursors.relationshipScanCursor

  override protected def invalidateCachedProperties(executionContext: CypherRow, id: Long): Unit =
    executionContext.invalidateCachedRelationshipProperties(id)

  override protected def getItemId(executionContext: CypherRow): Long = getFromRelFunction.applyAsLong(executionContext)
}
