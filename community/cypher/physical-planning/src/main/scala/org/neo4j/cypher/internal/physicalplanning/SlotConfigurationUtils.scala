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
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.SlotWithKeyAndAliases
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.VariableSlotKey
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.EntityById
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.WritableRow
import org.neo4j.cypher.internal.util.AssertionRunner
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.exceptions.InternalException
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue
import org.neo4j.values.virtual.VirtualValues

import java.util.function.ToLongFunction

object SlotConfigurationUtils {
  // TODO: Check if having try/catch blocks inside some of these generated functions prevents inlining or other JIT optimizations
  //       If so we may want to consider moving the handling and responsibility out to the pipes that use them

  val PRIMITIVE_NULL: Long = -1L

  /**
   * Use this to make a specialized getter function for a slot,
   * that given an ExecutionContext returns an AnyValue.
   */
  def makeGetValueFromSlotFunctionFor(slot: Slot): ReadableRow => AnyValue =
    slot match {
      case LongSlot(offset, false, CTNode) =>
        (context: ReadableRow) =>
          VirtualValues.node(context.getLongAt(offset))

      case LongSlot(offset, false, CTRelationship) =>
        (context: ReadableRow) =>
          VirtualValues.relationship(context.getLongAt(offset))

      case LongSlot(offset, true, CTNode) =>
        (context: ReadableRow) => {
          val nodeId = context.getLongAt(offset)
          if (nodeId == PRIMITIVE_NULL)
            Values.NO_VALUE
          else
            VirtualValues.node(nodeId)
        }
      case LongSlot(offset, true, CTRelationship) =>
        (context: ReadableRow) => {
          val relId = context.getLongAt(offset)
          if (relId == PRIMITIVE_NULL)
            Values.NO_VALUE
          else
            VirtualValues.relationship(relId)
        }
      case RefSlot(offset, _, _) =>
        (context: ReadableRow) =>
          context.getRefAt(offset)

      case _ =>
        throw new InternalException(s"Do not know how to make getter for slot $slot")
    }

  /**
   * Use this to make a specialized getter function for a slot and a primitive return type (i.e. CTNode or CTRelationship),
   * that given an ExecutionContext returns a long.
   */
  def makeGetPrimitiveFromSlotFunctionFor(
    slot: Slot,
    returnType: CypherType,
    throwOfTypeError: Boolean = true
  ): ToLongFunction[ReadableRow] =
    (slot, returnType, throwOfTypeError) match {
      case (LongSlot(offset, _, _), CTNode | CTRelationship, _) =>
        (context: ReadableRow) =>
          context.getLongAt(offset)

      case (RefSlot(offset, false, _), CTNode, true) =>
        (context: ReadableRow) =>
          val value = context.getRefAt(offset)
          try {
            value.asInstanceOf[VirtualNodeValue].id()
          } catch {
            case _: java.lang.ClassCastException =>
              throw new ParameterWrongTypeException(
                s"Expected to find a node at ref slot $offset but found $value instead"
              )
          }

      case (RefSlot(offset, false, _), CTRelationship, true) =>
        (context: ReadableRow) =>
          val value = context.getRefAt(offset)
          try {
            value.asInstanceOf[VirtualRelationshipValue].id()
          } catch {
            case _: java.lang.ClassCastException =>
              throw new ParameterWrongTypeException(
                s"Expected to find a relationship at ref slot $offset but found $value instead"
              )
          }

      case (RefSlot(offset, true, _), CTNode, true) =>
        (context: ReadableRow) =>
          val value = context.getRefAt(offset)
          try {
            if (value eq Values.NO_VALUE)
              PRIMITIVE_NULL
            else
              value.asInstanceOf[VirtualNodeValue].id()
          } catch {
            case _: java.lang.ClassCastException =>
              throw new ParameterWrongTypeException(
                s"Expected to find a node at ref slot $offset but found $value instead"
              )
          }

      case (RefSlot(offset, true, _), CTRelationship, true) =>
        (context: ReadableRow) =>
          val value = context.getRefAt(offset)
          try {
            if (value eq Values.NO_VALUE)
              PRIMITIVE_NULL
            else
              value.asInstanceOf[VirtualRelationshipValue].id()
          } catch {
            case _: java.lang.ClassCastException =>
              throw new ParameterWrongTypeException(
                s"Expected to find a relationship at ref slot $offset but found $value instead"
              )
          }

      case (RefSlot(offset, _, _), CTNode, false) =>
        (context: ReadableRow) =>
          context.getRefAt(offset) match {
            case n: VirtualNodeValue => n.id()
            case _                   => PRIMITIVE_NULL
          }

      case (RefSlot(offset, _, _), CTRelationship, false) =>
        (context: ReadableRow) =>
          context.getRefAt(offset) match {
            case r: VirtualRelationshipValue => r.id()
            case _                           => PRIMITIVE_NULL
          }

      case _ =>
        throw new InternalException(s"Do not know how to make a primitive getter for slot $slot with type $returnType")
    }

  /**
   * Use this to make a specialized getter function for a slot that is expected to contain a node
   * that given an ExecutionContext returns a long with the node id.
   */
  def makeGetPrimitiveNodeFromSlotFunctionFor(
    slot: Slot,
    throwOnTypeError: Boolean = true
  ): ToLongFunction[ReadableRow] =
    makeGetPrimitiveFromSlotFunctionFor(slot, CTNode, throwOnTypeError)

  /**
   * Use this to make a specialized getter function for a slot that is expected to contain a relationship
   * that given an ExecutionContext returns a long with the relationship id.
   */
  def makeGetPrimitiveRelationshipFromSlotFunctionFor(
    slot: Slot,
    throwOfTypeError: Boolean = true
  ): ToLongFunction[ReadableRow] =
    makeGetPrimitiveFromSlotFunctionFor(slot, CTRelationship, throwOfTypeError)

  val NO_ENTITY_FUNCTION: ToLongFunction[ReadableRow] = (value: ReadableRow) => PRIMITIVE_NULL

  /**
   * Use this to make a specialized setter function for a slot,
   * that takes as input an ExecutionContext and an AnyValue.
   */
  def makeSetValueInSlotFunctionFor(slot: Slot): (WritableRow, AnyValue) => Unit =
    slot match {
      case LongSlot(offset, false, CTNode) =>
        (context: WritableRow, value: AnyValue) =>
          try {
            context.setLongAt(offset, value.asInstanceOf[VirtualNodeValue].id())
          } catch {
            case _: java.lang.ClassCastException =>
              throw new ParameterWrongTypeException(
                s"Expected to find a node at long slot $offset but found $value instead"
              )
          }

      case LongSlot(offset, false, CTRelationship) =>
        (context: WritableRow, value: AnyValue) =>
          try {
            context.setLongAt(offset, value.asInstanceOf[VirtualRelationshipValue].id())
          } catch {
            case _: java.lang.ClassCastException =>
              throw new ParameterWrongTypeException(
                s"Expected to find a relationship at long slot $offset but found $value instead"
              )
          }

      case LongSlot(offset, true, CTNode) =>
        (context: WritableRow, value: AnyValue) =>
          if (value eq Values.NO_VALUE)
            context.setLongAt(offset, PRIMITIVE_NULL)
          else {
            try {
              context.setLongAt(offset, value.asInstanceOf[VirtualNodeValue].id())
            } catch {
              case _: java.lang.ClassCastException =>
                throw new ParameterWrongTypeException(
                  s"Expected to find a node at long slot $offset but found $value instead"
                )
            }
          }

      case LongSlot(offset, true, CTRelationship) =>
        (context: WritableRow, value: AnyValue) =>
          if (value eq Values.NO_VALUE)
            context.setLongAt(offset, PRIMITIVE_NULL)
          else {
            try {
              context.setLongAt(offset, value.asInstanceOf[VirtualRelationshipValue].id())
            } catch {
              case _: java.lang.ClassCastException =>
                throw new ParameterWrongTypeException(
                  s"Expected to find a relationship at long slot $offset but found $value instead"
                )
            }
          }

      case RefSlot(offset, _, _) =>
        (context: WritableRow, value: AnyValue) =>
          context.setRefAt(offset, value)

      case _ =>
        throw new InternalException(s"Do not know how to make setter for slot $slot")
    }

  /**
   * Use this to make a specialized setter function for a slot,
   * that takes as input an ExecutionContext and a primitive long value.
   */
  def makeSetPrimitiveInSlotFunctionFor(slot: Slot, valueType: CypherType): (CypherRow, Long, EntityById) => Unit =
    (slot, valueType) match {
      case (LongSlot(offset, nullable, CTNode), CTNode) =>
        if (AssertionRunner.isAssertionsEnabled && !nullable) {
          (context: CypherRow, value: Long, _: EntityById) =>
            if (value == PRIMITIVE_NULL)
              throw new ParameterWrongTypeException(s"Cannot assign null to a non-nullable slot")
            context.setLongAt(offset, value)
        } else {
          (context: CypherRow, value: Long, _: EntityById) =>
            context.setLongAt(offset, value)
        }

      case (LongSlot(offset, nullable, CTRelationship), CTRelationship) =>
        if (AssertionRunner.isAssertionsEnabled && !nullable) {
          (context: CypherRow, value: Long, _: EntityById) =>
            if (value == PRIMITIVE_NULL)
              throw new ParameterWrongTypeException(s"Cannot assign null to a non-nullable slot")
            context.setLongAt(offset, value)
        } else {
          (context: CypherRow, value: Long, _: EntityById) =>
            context.setLongAt(offset, value)
        }

      case (RefSlot(offset, false, typ), CTNode) if typ.isAssignableFrom(CTNode) =>
        if (AssertionRunner.isAssertionsEnabled) {
          (context: CypherRow, value: Long, entityById: EntityById) =>
            if (value == PRIMITIVE_NULL)
              throw new ParameterWrongTypeException(s"Cannot assign null to a non-nullable slot")
            context.setRefAt(offset, entityById.nodeById(value))
        } else {
          (context: CypherRow, value: Long, entityById: EntityById) =>
            // NOTE: Slot allocation needs to guarantee that we can never get nulls in here
            context.setRefAt(offset, entityById.nodeById(value))
        }

      case (RefSlot(offset, false, typ), CTRelationship) if typ.isAssignableFrom(CTRelationship) =>
        if (AssertionRunner.isAssertionsEnabled) {
          (context: CypherRow, value: Long, entityById: EntityById) =>
            if (value == -1L)
              if (value == PRIMITIVE_NULL)
                throw new ParameterWrongTypeException(s"Cannot assign null to a non-nullable slot")
            context.setRefAt(offset, entityById.relationshipById(value))
        } else {
          (context: CypherRow, value: Long, entityById: EntityById) =>
            // NOTE: Slot allocation needs to guarantee that we can never get nulls in here
            context.setRefAt(offset, entityById.relationshipById(value))
        }

      case (RefSlot(offset, true, typ), CTNode) if typ.isAssignableFrom(CTNode) =>
        (context: CypherRow, value: Long, entityById: EntityById) =>
          if (value == PRIMITIVE_NULL)
            context.setRefAt(offset, Values.NO_VALUE)
          else
            context.setRefAt(offset, entityById.nodeById(value))

      case (RefSlot(offset, true, typ), CTRelationship) if typ.isAssignableFrom(CTRelationship) =>
        (context: CypherRow, value: Long, entityById: EntityById) =>
          if (value == PRIMITIVE_NULL)
            context.setRefAt(offset, Values.NO_VALUE)
          else
            context.setRefAt(offset, entityById.relationshipById(value))

      case _ =>
        throw new InternalException(s"Do not know how to make a primitive $valueType setter for slot $slot")
    }

  /**
   * Use this to make a specialized getter function for a slot that is expected to contain a node
   * that given an ExecutionContext returns a long with the node id.
   */
  def makeSetPrimitiveNodeInSlotFunctionFor(slot: Slot): (CypherRow, Long, EntityById) => Unit =
    makeSetPrimitiveInSlotFunctionFor(slot, CTNode)

  /**
   * Use this to make a specialized getter function for a slot that is expected to contain a node
   * that given an ExecutionContext returns a long with the relationship id.
   */
  def makeSetPrimitiveRelationshipInSlotFunctionFor(slot: Slot): (CypherRow, Long, EntityById) => Unit =
    makeSetPrimitiveInSlotFunctionFor(slot, CTRelationship)

  /**
   * Prepare the given [[SlotConfiguration]] for use by runtime operators.
   * After this is called we do not expect any further mutations to happen to the slot configuration.
   */
  def finalizeSlotConfiguration(slots: SlotConfiguration): Unit = {
    if (!slots.finalized) {
      generateSlotAccessorFunctions(slots)
      slots.updateHasCachedProperties
      slots.finalized = true
    }
  }

  /**
   * Generate and update accessors for all slots in a SlotConfiguration
   */
  def generateSlotAccessorFunctions(slots: SlotConfiguration): Unit = {
    slots.foreachSlotAndAliases({
      case SlotWithKeyAndAliases(VariableSlotKey(key), slot, aliases) =>
        val getter = SlotConfigurationUtils.makeGetValueFromSlotFunctionFor(slot)
        val setter = SlotConfigurationUtils.makeSetValueInSlotFunctionFor(slot)
        val primitiveNodeSetter =
          if (slot.typ.isAssignableFrom(CTNode))
            Some(SlotConfigurationUtils.makeSetPrimitiveNodeInSlotFunctionFor(slot))
          else
            None
        val primitiveRelationshipSetter =
          if (slot.typ.isAssignableFrom(CTRelationship))
            Some(SlotConfigurationUtils.makeSetPrimitiveRelationshipInSlotFunctionFor(slot))
          else
            None
        slots.updateAccessorFunctions(key, getter, setter, primitiveNodeSetter, primitiveRelationshipSetter)
        aliases.foreach(alias =>
          slots.updateAccessorFunctions(alias, getter, setter, primitiveNodeSetter, primitiveRelationshipSetter)
        )

      case _ => // do nothing
    })
  }
}
