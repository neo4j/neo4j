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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.slotted.helpers

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{LongSlot, RefSlot, Slot}
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.util.v3_4.{InternalException, ParameterWrongTypeException}
import org.neo4j.cypher.internal.util.v3_4.symbols.{CTNode, CTRelationship}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.{VirtualEdgeValue, VirtualNodeValue, VirtualValues}

object SlottedPipeBuilderUtils {
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
          VirtualValues.edge(context.getLongAt(offset))

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
            VirtualValues.edge(relId)
        }
      case RefSlot(offset, _, _) =>
        (context: ExecutionContext) =>
          context.getRefAt(offset)

      case _ =>
        throw new InternalException(s"Do not know how to make getter for slot $slot")
    }

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
            context.setLongAt(offset, value.asInstanceOf[VirtualEdgeValue].id())
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
              context.setLongAt(offset, value.asInstanceOf[VirtualEdgeValue].id())
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
}
