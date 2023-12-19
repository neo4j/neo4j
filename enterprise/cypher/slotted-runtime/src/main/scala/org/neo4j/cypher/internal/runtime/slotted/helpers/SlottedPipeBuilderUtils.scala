/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cypher.internal.runtime.slotted.helpers

import NullChecker.entityIsNull
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{LongSlot, RefSlot, Slot}
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.util.v3_4.{AssertionUtils, InternalException, ParameterWrongTypeException}
import org.neo4j.cypher.internal.util.v3_4.symbols.{CTNode, CTRelationship, CypherType}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.{VirtualNodeValue, VirtualRelationshipValue, VirtualValues}

object SlottedPipeBuilderUtils {
  // TODO: Check if having try/catch blocks inside some of these generated functions prevents inlining or other JIT optimizations
  //       If so we may want to consider moving the handling and responsibility out to the pipes that use them

  val UNDEFINED_NODE: Long = -1L
  val UNDEFINED_REL: Long = -1L

  /**
    * Use this to make a specialized getter function for a slot,
    * that given an ExecutionContext returns an AnyValue.
    */
  def makeGetValueFromSlotFunctionFor(slot: Slot): ExecutionContext => AnyValue =
    slot match {
      case LongSlot(offset, false, CTNode) =>
        (context: ExecutionContext) =>
          VirtualValues.node(context.getLongAt(offset))

      case LongSlot(offset, false, CTRelationship) =>
        (context: ExecutionContext) =>
          VirtualValues.relationship(context.getLongAt(offset))

      case LongSlot(offset, true, CTNode) =>
        (context: ExecutionContext) => {
          val nodeId = context.getLongAt(offset)
          if (entityIsNull(nodeId))
            Values.NO_VALUE
          else
            VirtualValues.node(nodeId)
        }
      case LongSlot(offset, true, CTRelationship) =>
        (context: ExecutionContext) => {
          val relId = context.getLongAt(offset)
          if (entityIsNull(relId))
            Values.NO_VALUE
          else
            VirtualValues.relationship(relId)
        }
      case RefSlot(offset, _, _) =>
        (context: ExecutionContext) =>
          context.getRefAt(offset)

      case _ =>
        throw new InternalException(s"Do not know how to make getter for slot $slot")
    }

  /**
    * Use this to make a specialized getter function for a slot and a primitive return type (i.e. CTNode or CTRelationship),
    * that given an ExecutionContext returns a long.
    */
  def makeGetPrimitiveFromSlotFunctionFor(slot: Slot, returnType: CypherType): ExecutionContext => Long =
    (slot, returnType) match {
      case (LongSlot(offset, _, _), CTNode | CTRelationship) =>
        (context: ExecutionContext) =>
          context.getLongAt(offset)

      case (RefSlot(offset, false, _), CTNode) =>
        (context: ExecutionContext) =>
          val value = context.getRefAt(offset)
          try {
            value.asInstanceOf[VirtualNodeValue].id()
          } catch {
            case _: java.lang.ClassCastException =>
              throw new ParameterWrongTypeException(s"Expected to find a node at ref slot $offset but found $value instead")
          }

      case (RefSlot(offset, false, _), CTRelationship) =>
        (context: ExecutionContext) =>
          val value = context.getRefAt(offset)
          try {
            value.asInstanceOf[VirtualRelationshipValue].id()
          } catch {
            case _: java.lang.ClassCastException =>
              throw new ParameterWrongTypeException(s"Expected to find a relationship at ref slot $offset but found $value instead")
          }

      case (RefSlot(offset, true, _), CTNode) =>
        (context: ExecutionContext) =>
          val value = context.getRefAt(offset)
          try {
            if (value == Values.NO_VALUE)
              UNDEFINED_NODE
            else
              value.asInstanceOf[VirtualNodeValue].id()
          } catch {
            case _: java.lang.ClassCastException =>
              throw new ParameterWrongTypeException(s"Expected to find a node at ref slot $offset but found $value instead")
          }

      case (RefSlot(offset, true, _), CTRelationship) =>
        (context: ExecutionContext) =>
          val value = context.getRefAt(offset)
          try {
            if (value == Values.NO_VALUE)
              UNDEFINED_REL
            else
              value.asInstanceOf[VirtualRelationshipValue].id()
          } catch {
            case _: java.lang.ClassCastException =>
              throw new ParameterWrongTypeException(s"Expected to find a relationship at ref slot $offset but found $value instead")
          }

      case _ =>
        throw new InternalException(s"Do not know how to make a primitive getter for slot $slot with type $returnType")
    }

  /**
    * Use this to make a specialized getter function for a slot that is expected to contain a node
    * that given an ExecutionContext returns a long with the node id.
    */
  def makeGetPrimitiveNodeFromSlotFunctionFor(slot: Slot): ExecutionContext => Long =
    makeGetPrimitiveFromSlotFunctionFor(slot, CTNode)

  /**
    * Use this to make a specialized getter function for a slot that is expected to contain a node
    * that given an ExecutionContext returns a long with the relationship id.
    */
  def makeGetPrimitiveRelationshipFromSlotFunctionFor(slot: Slot): ExecutionContext => Long =
    makeGetPrimitiveFromSlotFunctionFor(slot, CTRelationship)

  /**
    * Use this to make a specialized setter function for a slot,
    * that takes as input an ExecutionContext and an AnyValue.
    */
  def makeSetValueInSlotFunctionFor(slot: Slot): (ExecutionContext, AnyValue) => Unit =
    slot match {
      case LongSlot(offset, false, CTNode) =>
        (context: ExecutionContext, value: AnyValue) =>
          try {
            context.setLongAt(offset, value.asInstanceOf[VirtualNodeValue].id())
          } catch {
            case _: java.lang.ClassCastException =>
              throw new ParameterWrongTypeException(s"Expected to find a node at long slot $offset but found $value instead")
          }

      case LongSlot(offset, false, CTRelationship) =>
        (context: ExecutionContext, value: AnyValue) =>
          try {
            context.setLongAt(offset, value.asInstanceOf[VirtualRelationshipValue].id())
          } catch {
            case _: java.lang.ClassCastException =>
              throw new ParameterWrongTypeException(s"Expected to find a relationship at long slot $offset but found $value instead")
          }

      case LongSlot(offset, true, CTNode) =>
        (context: ExecutionContext, value: AnyValue) =>
          if (value == Values.NO_VALUE)
            context.setLongAt(offset, -1L)
          else {
            try {
              context.setLongAt(offset, value.asInstanceOf[VirtualNodeValue].id())
            } catch {
              case _: java.lang.ClassCastException =>
                throw new ParameterWrongTypeException(s"Expected to find a node at long slot $offset but found $value instead")
            }
          }

      case LongSlot(offset, true, CTRelationship) =>
        (context: ExecutionContext, value: AnyValue) =>
          if (value == Values.NO_VALUE)
            context.setLongAt(offset, -1L)
          else {
            try {
              context.setLongAt(offset, value.asInstanceOf[VirtualRelationshipValue].id())
            } catch {
              case _: java.lang.ClassCastException =>
                throw new ParameterWrongTypeException(s"Expected to find a relationship at long slot $offset but found $value instead")
            }
          }

      case RefSlot(offset, _, _) =>
        (context: ExecutionContext, value: AnyValue) =>
          context.setRefAt(offset, value)

      case _ =>
        throw new InternalException(s"Do not know how to make setter for slot $slot")
    }

  /**
    * Use this to make a specialized setter function for a slot,
    * that takes as input an ExecutionContext and a primitive long value.
    */
  def makeSetPrimitiveInSlotFunctionFor(slot: Slot, valueType: CypherType): (ExecutionContext, Long) => Unit =
    (slot, valueType) match {
      case (LongSlot(offset, nullable, CTNode), CTNode) =>
        if (AssertionUtils.assertionsEnabled && !nullable) {
          (context: ExecutionContext, value: Long) =>
            if (value == -1L)
              throw new ParameterWrongTypeException(s"Cannot assign null to a non-nullable slot")
            context.setLongAt(offset, value)
        }
        else {
          (context: ExecutionContext, value: Long) =>
            context.setLongAt(offset, value)
        }

      case (LongSlot(offset, nullable, CTRelationship), CTRelationship) =>
        if (AssertionUtils.assertionsEnabled && !nullable) {
          (context: ExecutionContext, value: Long) =>
            if (value == -1L)
              throw new ParameterWrongTypeException(s"Cannot assign null to a non-nullable slot")
            context.setLongAt(offset, value)
        }
        else {
          (context: ExecutionContext, value: Long) =>
            context.setLongAt(offset, value)
        }

      case (RefSlot(offset, false, typ), CTNode) if typ.isAssignableFrom(CTNode) =>
        if (AssertionUtils.assertionsEnabled) {
          (context: ExecutionContext, value: Long) =>
            if (value == -1L)
              throw new ParameterWrongTypeException(s"Cannot assign null to a non-nullable slot")
            context.setRefAt(offset, VirtualValues.node(value))
        }
        else {
          (context: ExecutionContext, value: Long) =>
            // NOTE: Slot allocation needs to guarantee that we can never get nulls in here
            context.setRefAt(offset, VirtualValues.node(value))
        }

      case (RefSlot(offset, false, typ), CTRelationship) if typ.isAssignableFrom(CTRelationship) =>
        if (AssertionUtils.assertionsEnabled) {
          (context: ExecutionContext, value: Long) =>
            if (value == -1L)
              throw new ParameterWrongTypeException(s"Cannot assign null to a non-nullable slot")
            context.setRefAt(offset, VirtualValues.relationship(value))
        }
        else {
          (context: ExecutionContext, value: Long) =>
            // NOTE: Slot allocation needs to guarantee that we can never get nulls in here
            context.setRefAt(offset, VirtualValues.relationship(value))
        }

      case (RefSlot(offset, true, typ), CTNode) if typ.isAssignableFrom(CTNode) =>
        (context: ExecutionContext, value: Long) =>
          if (value == -1L)
            context.setRefAt(offset, Values.NO_VALUE)
          else
            context.setRefAt(offset, VirtualValues.node(value))

      case (RefSlot(offset, true, typ), CTRelationship) if typ.isAssignableFrom(CTRelationship) =>
        (context: ExecutionContext, value: Long) =>
          if (value == -1L)
            context.setRefAt(offset, Values.NO_VALUE)
          else
            context.setRefAt(offset, VirtualValues.relationship(value))

      case _ =>
        throw new InternalException(s"Do not know how to make a primitive $valueType setter for slot $slot")
    }

  /**
    * Use this to make a specialized getter function for a slot that is expected to contain a node
    * that given an ExecutionContext returns a long with the node id.
    */
  def makeSetPrimitiveNodeInSlotFunctionFor(slot: Slot): (ExecutionContext, Long) => Unit =
    makeSetPrimitiveInSlotFunctionFor(slot, CTNode)

  /**
    * Use this to make a specialized getter function for a slot that is expected to contain a node
    * that given an ExecutionContext returns a long with the relationship id.
    */
  def makeSetPrimitiveRelationshipInSlotFunctionFor(slot: Slot): (ExecutionContext, Long) => Unit =
    makeSetPrimitiveInSlotFunctionFor(slot, CTRelationship)


}
