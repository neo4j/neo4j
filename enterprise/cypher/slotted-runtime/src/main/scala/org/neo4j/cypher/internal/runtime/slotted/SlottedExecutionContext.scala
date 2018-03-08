/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.runtime.slotted

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{ExecutionContextInstanceCache, LongSlot, RefSlot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.util.v3_4.InternalException
import org.neo4j.cypher.internal.util.v3_4.symbols.{CTNode, CTRelationship}
import org.neo4j.cypher.result.QueryResult
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual._

import scala.collection.mutable

object SlottedExecutionContext {
  def empty = new SlottedExecutionContext(SlotConfiguration.empty)
}

/**
  * Execution context which uses a slot configuration to store values in two arrays.
  *
  * @param slots the slot configuration to use.
  */
case class SlottedExecutionContext(slots: SlotConfiguration) extends ExecutionContext {

  override val longs = new Array[Long](slots.numberOfLongs)
  //java.util.Arrays.fill(longs, -2L) // When debugging long slot issues you can uncomment this to check for uninitialized long slots (also in getLongAt below)
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

  override def release(): Unit = slots.releaseExecutionContext(this)

  override def reset(): Unit = {
    // Clear refs
    // TODO: We could try to skip this (e.g. if assertions are not enabled)
    // when we don't have code that relies on isRefInitialized() for correctness
    java.util.Arrays.fill(refs.asInstanceOf[Array[Object]], null)
  }

  override def copyTo(target: ExecutionContext, fromLongOffset: Int = 0, fromRefOffset: Int = 0, toLongOffset: Int = 0, toRefOffset: Int = 0): Unit =
    target match {
      case other@SlottedExecutionContext(otherPipeline) =>
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
    case other@SlottedExecutionContext(otherPipeline) =>
      if (nLongs > slots.numberOfLongs || nRefs > slots.numberOfReferences)
        throw new InternalException("Tried to copy more data into less.")
      else {
        System.arraycopy(other.longs, 0, longs, 0, nLongs)
        System.arraycopy(other.refs, 0, refs, 0, nRefs)
      }
    case _ => fail()
  }

  override def setLongAt(offset: Int, value: Long): Unit =
    longs(offset) = value

  override def getLongAt(offset: Int): Long =
    longs(offset)
    // When debugging long slot issues you can uncomment and replace with this to check for uninitialized long slots
    //  {
    //    val value = longs(offset)
    //    if (value == -2L)
    //      throw new InternalException(s"Long value not initialised at offset $offset in $this")
    //    value
    //  }

  override def setRefAt(offset: Int, value: AnyValue): Unit = refs(offset) = value

  override def getRefAt(offset: Int): AnyValue = {
    val value = refs(offset)
    if (value == null)
      throw new InternalException(s"Reference value not initialised at offset $offset in $this")
    value
  }

  override def -=(key: String): Nothing = fail() // We do not expect this to be used

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

  private def fail(): Nothing = throw new InternalException("Tried using a slotted context as a map")

  //-----------------------------------------------------------------------------------------------------------
  // Compatibility implementations of the old ExecutionContext API used by Community interpreted runtime pipes
  //-----------------------------------------------------------------------------------------------------------
  override def get(key: String): Option[AnyValue] = {
    slots.maybeGetter(key).map(g => g(this))
  }

  override def +=(kv: (String, AnyValue)): this.type = {
    setValue(kv._1, kv._2)
    this
  }

  override def getOrElse[B1 >: AnyValue](key: String, default: => B1): B1 = get(key) match {
    case Some(v) => v
    case None => default
  }

  // The newWith methods are called from Community pipes. We should already have allocated slots for the given keys,
  // so we just set the values in the existing slots instead of creating a new context like in the MapExecutionContext.
  override def set(newEntries: Seq[(String, AnyValue)]): ExecutionContext = {
    newEntries.foreach {
      case (k, v) =>
        setValue(k, v)
    }
    this
  }

  override def set(key1: String, value1: AnyValue): ExecutionContext = {
    setValue(key1, value1)
    this
  }

  override def set(key1: String, value1: AnyValue, key2: String, value2: AnyValue): ExecutionContext = {
    setValue(key1, value1)
    setValue(key2, value2)
    this
  }

  override def set(key1: String, value1: AnyValue, key2: String, value2: AnyValue, key3: String, value3: AnyValue): ExecutionContext = {
    setValue(key1, value1)
    setValue(key2, value2)
    setValue(key3, value3)
    this
  }

  override def copyWith(key1: String, value1: AnyValue): ExecutionContext = {
    // This method should throw like its siblings below as soon as reduce is changed to not use it.
    val newCopy = SlottedExecutionContext(slots)
    copyTo(newCopy)
    newCopy.setValue(key1, value1)
    newCopy
  }

  override def copyWith(key1: String, value1: AnyValue, key2: String, value2: AnyValue): ExecutionContext = {
    throw new UnsupportedOperationException(
      "Use ExecutionContextFactory.copyWith instead to get the correct slot configuration"
    )
  }

  override def copyWith(key1: String, value1: AnyValue,
                        key2: String, value2: AnyValue,
                        key3: String, value3: AnyValue): ExecutionContext = {
    throw new UnsupportedOperationException(
      "Use ExecutionContextFactory.copyWith instead to get the correct slot configuration"
    )
  }

  override def copyWith(newEntries: Seq[(String, AnyValue)]): ExecutionContext = {
    throw new UnsupportedOperationException(
      "Use ExecutionContextFactory.copyWith instead to get the correct slot configuration"
    )
  }

  private def setValue(key1: String, value1: AnyValue): Unit = {
    slots.maybeSetter(key1)
      .getOrElse(throw new InternalException(s"Ouch, no suitable slot for key $key1 = $value1\nSlots: ${slots}"))
      .apply(this, value1)
 }

  def isRefInitialized(offset: Int): Boolean = {
    refs(offset) != null
  }

  def getRefAtWithoutCheckingInitialized(offset: Int): AnyValue =
    refs(offset)

  override def mergeWith(other: ExecutionContext): ExecutionContext = other match {
    case primitiveOther: SlottedExecutionContext =>
      primitiveOther.slots.foreachSlot {
        case (key, otherSlot) =>
          val thisSlot = slots.get(key).getOrElse(
            throw new InternalException(s"Tried to merge slot $otherSlot from $other but it is missing from $this." +
              "Looks like something needs to be fixed in slot allocation.")
          )
          assert(thisSlot.isTypeCompatibleWith(otherSlot))
          otherSlot match {
            case LongSlot(offset, _, _) =>
              setLongAt(thisSlot.offset, other.getLongAt(offset))
            case RefSlot(offset, _, _) if primitiveOther.isRefInitialized(offset) =>
              setRefAt(thisSlot.offset, primitiveOther.getRefAtWithoutCheckingInitialized(offset))
            case _ =>
          }
      }
      this

    case _ =>
      throw new InternalException("Well well, isn't this a delicate situation?")
  }

  override def createClone(): ExecutionContext = {
    val clone = SlottedExecutionContext(slots)
    copyTo(clone)
    clone
  }

  // TODO: If we save currently utilized slot size per logical plan this could be simplified to checking
  // if the slot offset is less than the current size.
  // This is also the only way that we could detect if a LongSlot was not initialized
  override def boundEntities(materializeNode: Long => AnyValue, materializeRelationship: Long => AnyValue): Map[String, AnyValue] = {
    var entities = mutable.Map.empty[String, AnyValue]
    slots.foreachSlot {
      case (key, RefSlot(offset, _, _)) =>
        if (isRefInitialized(offset)) {
          val entity = getRefAtWithoutCheckingInitialized(offset)
          entity match {
            case _: NodeValue | _: RelationshipValue =>
              entities += key -> entity
            case nodeRef: NodeReference =>
              entities += key -> materializeNode(nodeRef.id())
            case relRef: RelationshipReference =>
              entities += key -> materializeRelationship(relRef.id())
            case _ => // Do nothing
          }
        }
      case (key, LongSlot(offset, false, CTNode)) =>
        entities += key -> materializeNode(getLongAt(offset))
      case (key, LongSlot(offset, false, CTRelationship)) =>
        entities += key -> materializeRelationship(getLongAt(offset))
      case (key, LongSlot(offset, true, CTNode)) =>
        val entityId = getLongAt(offset)
        if (entityId >= 0)
          entities += key -> materializeNode(getLongAt(offset))
      case (key, LongSlot(offset, true, CTRelationship)) =>
        val entityId = getLongAt(offset)
        if (entityId >= 0)
          entities += key -> materializeRelationship(getLongAt(offset))
      case _ => // Do nothing
    }
    entities.toMap
  }

  override def isNull(key: String): Boolean =
    slots.get(key) match {
      case Some(RefSlot(offset, true, _)) if isRefInitialized(offset) =>
        getRefAtWithoutCheckingInitialized(offset) == Values.NO_VALUE
      case Some(LongSlot(offset, true, CTNode)) =>
        entityIsNull(getLongAt(offset))
      case Some(LongSlot(offset, true, CTRelationship)) =>
        entityIsNull(getLongAt(offset))
      case _ =>
        false
    }
}

case class ArrayResultExecutionContextFactory(columns: Seq[(String, Expression)])
  extends ExecutionContextInstanceCache[ArrayResultExecutionContextFactory, ArrayResultExecutionContext] {
  val columnExpressionArray = columns.map(_._2).toArray
  val columnArraySize = columnExpressionArray.size
  val columnIndexMap = {
    val m = new mutable.OpenHashMap[String, Int](columns.length)
    var index = 0
    columns.foreach {
      case (name, exp) => m.put(name, index)
      index += 1
    }
    m
  }

  setExecutionContextConstructor((factory) => {
    val resultArray = new Array[AnyValue](columnArraySize)
    ArrayResultExecutionContext(resultArray, columnIndexMap, factory)
  })

  def newResult(context: ExecutionContext, state: QueryState): ArrayResultExecutionContext = {
    val result = allocateExecutionContext

    // Apply the expressions that materializes the result values and fill in the result array
    val resultArray = result.resultArray
    0 until columnArraySize foreach {
      index =>
        resultArray(index) = columnExpressionArray(index).apply(context, state)
    }
    result
  }
}

case class ArrayResultExecutionContext(resultArray: Array[AnyValue],
                                       columnIndexMap: scala.collection.Map[String, Int],
                                       factory: ArrayResultExecutionContextFactory)
  extends ExecutionContext with QueryResult.Record {

  override def releaseMe(): Unit = factory.releaseExecutionContext(this)
  override def release(): Unit = factory.releaseExecutionContext(this)

  override def reset(): Unit = ()

  override def get(key: String): Option[AnyValue] = {
    columnIndexMap.get(key) match {
      case Some(index) => Some(resultArray(index))
      case _=> None
    }
  }

  override def iterator: Iterator[(String, AnyValue)] = {
    columnIndexMap.iterator.map {
      case (name, index) =>
        (name, resultArray(index))
    }
  }

  override def fields(): Array[AnyValue] = {
    resultArray
  }

  override def size: Int = resultArray.size

  //---------------------------------------------------------------------------
  // This is an ExecutionContext by name only and does not support the full API
  // The methods below should never be called on a produced result
  private def fail(): Nothing = throw new InternalException("Tried using a result context as an execution context")

  override def copyTo(target: ExecutionContext, fromLongOffset: Int, fromRefOffset: Int, toLongOffset: Int, toRefOffset: Int): Unit = fail()

  override def copyFrom(input: ExecutionContext, nLongs: Int, nRefs: Int): Unit = fail()

  override def setLongAt(offset: Int, value: Long): Unit = fail()

  override def getLongAt(offset: Int): Long = fail()

  override def longs(): Array[Long] = fail()

  override def setRefAt(offset: Int, value: AnyValue): Unit = fail()

  override def getRefAt(offset: Int): AnyValue = fail()

  override def refs(): Array[AnyValue] = fail()

  override def set(newEntries: Seq[(String, AnyValue)]): ExecutionContext = fail()

  override def set(key: String, value: AnyValue): ExecutionContext = fail()

  override def set(key1: String, value1: AnyValue, key2: String, value2: AnyValue): ExecutionContext = fail()

  override def set(key1: String, value1: AnyValue, key2: String, value2: AnyValue, key3: String, value3: AnyValue): ExecutionContext = fail()

  override def mergeWith(other: ExecutionContext): ExecutionContext = fail()

  override def createClone(): ExecutionContext = fail()

  override def copyWith(key: String, value: AnyValue): ExecutionContext = fail()

  override def copyWith(key1: String, value1: AnyValue, key2: String, value2: AnyValue): ExecutionContext = fail()

  override def copyWith(key1: String, value1: AnyValue, key2: String, value2: AnyValue, key3: String, value3: AnyValue): ExecutionContext = fail()

  override def copyWith(newEntries: Seq[(String, AnyValue)]): ExecutionContext = fail()

  override def boundEntities(materializeNode: Long => AnyValue, materializeRelationship: Long => AnyValue): Map[String, AnyValue] = fail()

  override def isNull(key: String): Boolean = fail()

  override def +=(kv: (String, AnyValue)): ArrayResultExecutionContext.this.type = fail()

  override def -=(key: String): ArrayResultExecutionContext.this.type = fail()
}