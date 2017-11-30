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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.slotted

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{LongSlot, RefSlot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.util.v3_4.InternalException
import org.neo4j.cypher.internal.util.v3_4.symbols.{CTNode, CTRelationship}
import org.neo4j.kernel.impl.util.{NodeProxyWrappingNodeValue, RelationshipProxyWrappingEdgeValue}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

object PrimitiveExecutionContext {
  def empty = new PrimitiveExecutionContext(SlotConfiguration.empty)
}

/**
  * Execution context which uses a slot configuration to store values in two arrays.
  *
  * @param slots the slot configuration to use.
  */
case class PrimitiveExecutionContext(slots: SlotConfiguration) extends ExecutionContext {

  override val longs = new Array[Long](slots.numberOfLongs)
  override val refs = new Array[AnyValue](slots.numberOfReferences)

  override def toString(): String = {
    val iter = this.iterator
    val s: StringBuilder = StringBuilder.newBuilder
    s ++= s"\nPrimitiveExecutionContext {\n    $slots"
    while(iter.hasNext) {
      val slotValue = iter.next
      s ++= f"\n    ${slotValue._1}%-40s = ${slotValue._2}"
    }
    s ++= "\n}\n"
    s.result
  }

  override def copyTo(target: ExecutionContext, fromLongOffset: Int = 0, fromRefOffset: Int = 0, toLongOffset: Int = 0, toRefOffset: Int = 0): Unit =
    target match {
      case other@PrimitiveExecutionContext(otherPipeline) =>
        if (slots.numberOfLongs > otherPipeline.numberOfLongs ||
          slots.numberOfReferences > otherPipeline.numberOfReferences)
          throw new InternalException("Tried to copy more data into less.")
        else {
          System.arraycopy(longs, fromLongOffset, other.longs, toLongOffset, slots.numberOfLongs - fromLongOffset)
          System.arraycopy(refs, fromRefOffset, other.refs, toRefOffset, slots.numberOfReferences - fromRefOffset)
        }
      case _ => fail()
    }

  override def copyFrom(input: ExecutionContext, nLongs: Int, nRefs: Int): Unit = input match {
    case other@PrimitiveExecutionContext(otherPipeline) =>
      if (nLongs > slots.numberOfLongs || nRefs > slots.numberOfReferences)
        throw new InternalException("Tried to copy more data into less.")
      else {
        System.arraycopy(other.longs, 0, longs, 0, nLongs)
        System.arraycopy(other.refs, 0, refs, 0, nRefs)
      }
    case _ => fail()
  }

  override def setLongAt(offset: Int, value: Long): Unit = longs(offset) = value

  override def getLongAt(offset: Int): Long = longs(offset)

  override def setRefAt(offset: Int, value: AnyValue): Unit = refs(offset) = value

  override def getRefAt(offset: Int): AnyValue = {
    val value = refs(offset)
    if (value == null)
      throw new InternalException("Value not initialised")
    value
  }

  override def +=(kv: (String, AnyValue)): Nothing = fail()

  override def -=(key: String): Nothing = fail()

  override def get(key: String): Nothing = fail()

  override def iterator: Iterator[(String, AnyValue)] = {
    // This method implementation is for debug usage only (the debugger will invoke it when stepping).
    // Please do not use in production code.
    val longSlots = slots.getLongSlots
    val refSlots = slots.getRefSlots
    val longSlotValues = for { i <- 0 until longs.length }
      yield (longSlots(i).toString, Values.longValue(longs(i)))
    val refSlotValues = for { i <- 0 until refs.length }
      yield (refSlots(i).toString, refs(i))
    (longSlotValues ++ refSlotValues).iterator
  }

  private def fail(): Nothing = throw new InternalException("Tried using a primitive context as a map")

  // This method is called from ScopeExpressions. We should already have allocated a slot for the given
  // key, so we just set the value in the existing slot instead of creating a new context like in
  // the MapExecutionContext.
  override def newWith1(key1: String, value1: AnyValue): ExecutionContext = {
    setValue(key1, value1)
    this
  }

  // This method is used instead of newWith1 from a ScopeExpression where the key in the new scope is overriding an existing key
  // and it has to have the same name due to syntactic constraints. In this case we actually make a copy in order to not corrupt the existing slot.
  override def newScopeWith1(key1: String, value1: AnyValue): ExecutionContext = {
    val scopeContext = PrimitiveExecutionContext(slots)
    copyTo(scopeContext)
    scopeContext.asInstanceOf[PrimitiveExecutionContext].setValue(key1, value1)
    scopeContext
  }

  override def newWith2(key1: String, value1: AnyValue, key2: String, value2: AnyValue): ExecutionContext = fail()

  override def newWith3(key1: String, value1: AnyValue, key2: String, value2: AnyValue, key3: String, value3: AnyValue): ExecutionContext = fail()

  override def mergeWith(other: ExecutionContext): ExecutionContext = fail()

  override def createClone(): ExecutionContext = fail()

  override def newWith(newEntries: Seq[(String, AnyValue)]): ExecutionContext = fail()

  private def setValue(key1: String, value1: AnyValue): Unit = {
    (slots.get(key1), value1) match {
      case (Some(RefSlot(offset, _, _)), _) =>
        setRefAt(offset, value1)

      case (Some(LongSlot(offset, false, CTNode)),
            nodeVal: NodeProxyWrappingNodeValue) =>
        setLongAt(offset, nodeVal.id())

      case (Some(LongSlot(offset, false, CTRelationship)),
            relVal: RelationshipProxyWrappingEdgeValue) =>
        setLongAt(offset, relVal.id())

      case (Some(LongSlot(offset, true, CTNode)), nodeVal) if nodeVal == Values.NO_VALUE =>
        setLongAt(offset, -1L)

      case (Some(LongSlot(offset, true, CTRelationship)), relVal) if relVal == Values.NO_VALUE =>
        setLongAt(offset, -1L)

      case (Some(LongSlot(offset, true, CTNode)),
            nodeVal: NodeProxyWrappingNodeValue) =>
        setLongAt(offset, nodeVal.id())

      case (Some(LongSlot(offset, true, CTRelationship)),
            relVal: RelationshipProxyWrappingEdgeValue) =>
        setLongAt(offset, relVal.id())

      case _ =>
        throw new InternalException(s"Ouch, no suitable slot for key $key1 = $value1\nSlots: ${slots}")
    }
  }
}
