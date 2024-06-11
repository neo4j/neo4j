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
package org.neo4j.cypher.internal.runtime.slotted

import org.neo4j.cypher.internal.expressions.ASTCachedProperty
import org.neo4j.cypher.internal.macros.AssertMacros.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.internal.physicalplanning.LongSlot
import org.neo4j.cypher.internal.physicalplanning.RefSlot
import org.neo4j.cypher.internal.physicalplanning.SlotAllocation.LOAD_CSV_METADATA_KEY
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.ApplyPlanSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.CachedPropertySlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.DuplicatedSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.MetaDataSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.OuterNestedApplyPlanSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.SlotWithKeyAndAliases
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.VariableSlotKey
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.EntityById
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.ResourceLinenumber
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.exceptions.InternalException
import org.neo4j.graphdb.NotFoundException
import org.neo4j.kernel.api.StatementConstants
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.HeapEstimator.shallowSizeOfInstance
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue

object SlottedRow {
  final val INSTANCE_SIZE = shallowSizeOfInstance(classOf[SlottedRow])

  def empty = new SlottedRow(SlotConfiguration.empty)
  final val DEBUG = false

  def getNodeId(row: ReadableRow, offset: Int, isReference: Boolean): Long =
    if (isReference) {
      row.getRefAt(offset) match {
        case node: VirtualNodeValue => node.id()
        case Values.NO_VALUE        => StatementConstants.NO_SUCH_NODE
        case null                   => throw new CypherTypeException(s"Expected a node, but got null ")
        case other                  => throw new CypherTypeException(s"Expected a node, but got ${other.getTypeName} ")
      }
    } else {
      row.getLongAt(offset)
    }

  def getLinenumber(row: ReadableRow, slots: SlotConfiguration): Option[ResourceLinenumber] = {
    slots.getMetaDataSlot(LOAD_CSV_METADATA_KEY) match {
      case Some(RefSlot(offset, _, _)) =>
        row.getRefAt(offset) match {
          case l: ResourceLinenumber =>
            Some(l)
          case Values.NO_VALUE =>
            None
          case null =>
            None
          case _ =>
            throw new InternalException("Wrong type of linenumber")
        }

      case _ =>
        None
    }
  }
}

trait SlottedCompatible {
  def copyAllToSlottedRow(target: SlottedRow): Unit
  def copyLongsToSlottedRow(target: SlottedRow, fromOffset: Int, toOffset: Int, length: Int): Unit
  def copyRefsToSlottedRow(target: SlottedRow, fromOffset: Int, toOffset: Int, length: Int): Unit
}

/**
 * Execution context which uses a slot configuration to store values in two arrays.
 *
 * @param slots the slot configuration to use.
 */
//noinspection NameBooleanParameters
case class SlottedRow(slots: SlotConfiguration) extends CypherRow {

  val longs = new Array[Long](slots.numberOfLongs)
  // java.util.Arrays.fill(longs, -2L) // When debugging long slot issues you can uncomment this to check for uninitialized long slots (also in getLongAt below)
  val refs = new Array[AnyValue](slots.numberOfReferences)

  override def toString: String = {
    val iter = this.iterator
    val s: StringBuilder = StringBuilder.newBuilder
    s ++= s"\nSlottedExecutionContext {\n    $slots"
    while (iter.hasNext) {
      val slotValue = iter.next
      s ++= f"\n    ${slotValue._1}%-40s = ${slotValue._2}"
    }
    s ++= "\n}\n"
    s.result
  }

  override def copyAllFrom(input: ReadableRow): Unit = input match {
    case other: SlottedRow        => copyFromSlotted(other, 0, 0, 0, 0)
    case other: SlottedCompatible => other.copyAllToSlottedRow(this)
    case _                        => fail()
  }

  override def copyFrom(input: ReadableRow, nLongs: Int, nRefs: Int): Unit =
    if (nLongs > slots.numberOfLongs || nRefs > slots.numberOfReferences)
      throw new InternalException(
        "A bug has occurred in the slotted runtime: The target slotted execution context cannot hold the data to copy."
      )
    else input match {
      case other @ SlottedRow(_) =>
        System.arraycopy(other.longs, 0, longs, 0, nLongs)
        System.arraycopy(other.refs, 0, refs, 0, nRefs)

      case other: SlottedCompatible =>
        other.copyLongsToSlottedRow(this, 0, 0, nLongs)
        other.copyRefsToSlottedRow(this, 0, 0, nRefs)

      case _ => fail()
    }

  override def copyLongsFrom(input: ReadableRow, fromOffset: Int, toOffset: Int, length: Int): Unit = {
    input match {
      case from: SlottedRow        => System.arraycopy(from.longs, fromOffset, longs, toOffset, length)
      case from: SlottedCompatible => from.copyLongsToSlottedRow(this, fromOffset, toOffset, length)
      case _                       => fail()
    }
  }

  override def copyRefsFrom(input: ReadableRow, fromOffset: Int, toOffset: Int, length: Int): Unit = {
    input match {
      case from: SlottedRow        => System.arraycopy(from.refs, fromOffset, refs, toOffset, length)
      case from: SlottedCompatible => from.copyRefsToSlottedRow(this, fromOffset, toOffset, length)
      case _                       => fail()
    }
  }

  override def copyMapped(func: AnyValue => AnyValue): CypherRow = {
    val clone = SlottedRow(slots)
    clone.copyAllFrom(this)
    clone.transformRefs(func)
    clone
  }

  private def transformRefs(func: AnyValue => AnyValue): Unit = {
    var i = 0
    while (i < refs.length) {
      val refValue = getRefAt(i)
      val newRefValue = func(refValue)
      setRefAt(i, newRefValue)
      i += 1
    }
  }

  override def valueExists(p: AnyValue => Boolean): Boolean = {
    var i = 0
    while (i < refs.length) {
      if (refs(i) != null && p(refs(i))) {
        return true
      }
      i += 1
    }
    false
  }

  override def copyFromOffset(
    input: ReadableRow,
    sourceLongOffset: Int,
    sourceRefOffset: Int,
    targetLongOffset: Int,
    targetRefOffset: Int
  ): Unit =
    input match {
      case other: SlottedRow =>
        copyFromSlotted(other, sourceLongOffset, sourceRefOffset, targetLongOffset, targetRefOffset)
      case _ => fail()
    }

  private def copyFromSlotted(
    other: SlottedRow,
    sourceLongOffset: Int,
    sourceRefOffset: Int,
    targetLongOffset: Int,
    targetRefOffset: Int
  ): Unit = {
    val otherPipeline = other.slots
    if (
      otherPipeline.numberOfLongs > slots.numberOfLongs ||
      otherPipeline.numberOfReferences > slots.numberOfReferences
    ) {
      throw new InternalException(
        s"""A bug has occurred in the slotted runtime: The target slotted execution context cannot hold the data to copy
           |From : $otherPipeline
           |To :   $slots""".stripMargin
      )
    } else {
      System.arraycopy(
        other.longs,
        sourceLongOffset,
        longs,
        targetLongOffset,
        other.slots.numberOfLongs - sourceLongOffset
      )
      System.arraycopy(
        other.refs,
        sourceRefOffset,
        refs,
        targetRefOffset,
        other.slots.numberOfReferences - sourceRefOffset
      )
    }
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
    if (SlottedRow.DEBUG && value == null)
      throw new InternalException(s"Reference value not initialised at offset $offset in $this")
    value
  }

  override def getByName(name: String): AnyValue = {
    slots.maybeGetter(name).map(g => g(this)).getOrElse(throw new NotFoundException(s"Unknown variable `$name`."))
  }

  override def numberOfColumns: Int = refs.length + longs.length

  override def containsName(name: String): Boolean = slots.maybeGetter(name).map(g => g(this)).isDefined

  override def setCachedPropertyAt(offset: Int, value: Value): Unit = refs(offset) = value

  override def invalidateCachedProperties(): Unit = {
    slots.foreachCachedSlot {
      case (_, propertyRefSLot) => setCachedPropertyAt(propertyRefSLot.offset, null)
    }
  }

  override def invalidateCachedNodeProperties(node: Long): Unit = {
    slots.foreachCachedSlot {
      case (cnp, propertyRefSLot) =>
        slots.get(cnp.entityName) match {
          case Some(longSlot: LongSlot) =>
            if (longSlot.typ == CTNode && getLongAt(longSlot.offset) == node) {
              setCachedPropertyAt(propertyRefSLot.offset, null)
            }
          case Some(refSlot: RefSlot) =>
            if (refSlot.typ == CTNode && getRefAt(refSlot.offset).asInstanceOf[VirtualNodeValue].id == node) {
              setCachedPropertyAt(propertyRefSLot.offset, null)
            }
          case None =>
          // This case is possible to reach, when we allocate a cached property before a pipeline break and before the variable it is referencing.
          // We will never evaluate that cached property in this row, and we could improve SlotAllocation to allocate it only on the next pipeline
          // instead, but that is difficult. It is harmless if we get here, we will simply not do anything.
        }
    }
  }

  override def invalidateCachedRelationshipProperties(rel: Long): Unit = {
    slots.foreachCachedSlot {
      case (crp, propertyRefSLot) =>
        slots.get(crp.entityName) match {
          case Some(longSlot: LongSlot) =>
            if (longSlot.typ == CTRelationship && getLongAt(longSlot.offset) == rel) {
              setCachedPropertyAt(propertyRefSLot.offset, null)
            }
          case Some(refSlot: RefSlot) =>
            if (
              refSlot.typ == CTRelationship && getRefAt(refSlot.offset).asInstanceOf[VirtualRelationshipValue].id == rel
            ) {
              setCachedPropertyAt(propertyRefSLot.offset, null)
            }
          case None =>
          // This case is possible to reach, when we allocate a cached property before a pipeline break and before the variable it is referencing.
          // We will never evaluate that cached property in this row, and we could improve SlotAllocation to allocate it only on the next pipeline
          // instead, but that is difficult. It is harmless if we get here, we will simply not do anything.
        }
    }
  }

  override def setCachedProperty(key: ASTCachedProperty.RuntimeKey, value: Value): Unit =
    setCachedPropertyAt(slots.getCachedPropertyOffsetFor(key), value)

  override def getCachedPropertyAt(offset: Int): Value = refs(offset).asInstanceOf[Value]

  override def getCachedProperty(key: ASTCachedProperty.RuntimeKey): Value = fail()

  override def estimatedHeapUsage: Long = {
    var usage = SlottedRow.INSTANCE_SIZE + HeapEstimator.sizeOf(longs) + HeapEstimator.shallowSizeOf(
      refs.asInstanceOf[Array[Object]]
    )
    var i = 0
    while (i < refs.length) {
      val ref = refs(i)
      if (ref != null) {
        usage += ref.estimatedHeapUsage()
      }
      i += 1
    }
    usage
  }

  override def deduplicatedEstimatedHeapUsage(previous: CypherRow): Long = {
    if (previous eq null) {
      estimatedHeapUsage
    } else {
      var usage = SlottedRow.INSTANCE_SIZE + HeapEstimator.sizeOf(longs) + HeapEstimator.shallowSizeOf(
        refs.asInstanceOf[Array[Object]]
      )
      var i = 0
      while (i < refs.length) {
        val ref = refs(i)
        if ((ref ne null) && (ref ne previous.getRefAt(i))) {
          usage += ref.estimatedHeapUsage()
        }
        i += 1
      }
      usage
    }
  }

  private def fail(): Nothing = throw new InternalException("Tried using a slotted context as a map")

  // -----------------------------------------------------------------------------------------------------------
  // Compatibility implementations of the old ExecutionContext API used by Community interpreted runtime pipes
  // -----------------------------------------------------------------------------------------------------------

  override def setLinenumber(line: Option[ResourceLinenumber]): Unit = fail()

  override def getLinenumber: Option[ResourceLinenumber] = {
    SlottedRow.getLinenumber(this, slots)
  }

  // The newWith methods are called from Community pipes. We should already have allocated slots for the given keys,
  // so we just set the values in the existing slots instead of creating a new context like in the MapExecutionContext.
  override def set(newEntries: collection.Seq[(String, AnyValue)]): Unit =
    newEntries.foreach {
      case (k, v) =>
        setValue(k, v)
    }

  override def set(key1: String, value1: AnyValue): Unit =
    setValue(key1, value1)

  override def set(key1: String, value1: AnyValue, key2: String, value2: AnyValue): Unit = {
    setValue(key1, value1)
    setValue(key2, value2)
  }

  override def set(
    key1: String,
    value1: AnyValue,
    key2: String,
    value2: AnyValue,
    key3: String,
    value3: AnyValue
  ): Unit = {
    setValue(key1, value1)
    setValue(key2, value2)
    setValue(key3, value3)
  }

  override def copyWith(key1: String, value1: AnyValue): CypherRow = {
    // This method should throw like its siblings below as soon as reduce is changed to not use it.
    val newCopy = SlottedRow(slots)
    newCopy.copyAllFrom(this)
    newCopy.setValue(key1, value1)
    newCopy
  }

  override def copyWith(key1: String, value1: AnyValue, key2: String, value2: AnyValue): CypherRow = {
    throw new UnsupportedOperationException(
      "Use ExecutionContextFactory.copyWith instead to get the correct slot configuration"
    )
  }

  override def copyWith(
    key1: String,
    value1: AnyValue,
    key2: String,
    value2: AnyValue,
    key3: String,
    value3: AnyValue
  ): CypherRow = {
    throw new UnsupportedOperationException(
      "Use ExecutionContextFactory.copyWith instead to get the correct slot configuration"
    )
  }

  override def copyWith(newEntries: collection.Seq[(String, AnyValue)]): CypherRow = {
    throw new UnsupportedOperationException(
      "Use ExecutionContextFactory.copyWith instead to get the correct slot configuration"
    )
  }

  private def setValue(key1: String, value1: AnyValue): Unit = {
    slots.maybeSetter(key1)
      .getOrElse(throw new InternalException(s"Ouch, no suitable slot for key $key1 = $value1\nSlots: $slots"))
      .apply(this, value1)
  }

  def isRefInitialized(offset: Int): Boolean = {
    refs(offset) != null
  }

  def getRefAtWithoutCheckingInitialized(offset: Int): AnyValue =
    refs(offset)

  override def mergeWith(other: ReadableRow, entityById: EntityById, checkNullability: Boolean = true): Unit =
    other match {
      case slottedOther: SlottedRow =>
        slottedOther.slots.foreachSlot({
          case (VariableSlotKey(key), otherSlot @ LongSlot(offset, _, CTNode)) =>
            val thisSlotSetter = slots.maybePrimitiveNodeSetter(key).getOrElse(
              throw new InternalException(
                s"Tried to merge primitive node slot $otherSlot from $other but it is missing from $this." +
                  "Looks like something needs to be fixed in slot allocation."
              )
            )
            thisSlotSetter.apply(this, other.getLongAt(offset), entityById)

          case (VariableSlotKey(key), otherSlot @ LongSlot(offset, _, CTRelationship)) =>
            val thisSlotSetter = slots.maybePrimitiveRelationshipSetter(key).getOrElse(
              throw new InternalException(
                s"Tried to merge primitive relationship slot $otherSlot from $other but it is missing from $this." +
                  "Looks like something needs to be fixed in slot allocation."
              )
            )
            thisSlotSetter.apply(this, other.getLongAt(offset), entityById)

          case (VariableSlotKey(key), otherSlot @ RefSlot(offset, _, _)) if slottedOther.isRefInitialized(offset) =>
            val thisSlotSetter = slots.maybeSetter(key).getOrElse(
              throw new InternalException(s"Tried to merge slot $otherSlot from $other but it is missing from $this." +
                "Looks like something needs to be fixed in slot allocation.")
            )
            checkOnlyWhenAssertionsAreEnabled(!checkNullability || checkCompatibleNullablility(key, otherSlot))

            val otherValue = slottedOther.getRefAtWithoutCheckingInitialized(offset)
            thisSlotSetter.apply(this, otherValue)

          case (_: VariableSlotKey, _) =>
          // a slot which is not initialized(=null). This means it is allocated, but will only be used later in the pipeline.
          // Therefore, this is a no-op.

          case (CachedPropertySlotKey(property), refSlot) =>
            setCachedProperty(property, other.getCachedPropertyAt(refSlot.offset))

          case (key: MetaDataSlotKey, refSlot) =>
            val thisOffset = slots.getMetaDataOffsetFor(key)
            // Do not overwrite existing meta data here (preserves backwards compatibility with MapCypherRow)
            if (!isRefInitialized(thisOffset) || (getRefAtWithoutCheckingInitialized(thisOffset) eq Values.NO_VALUE)) {
              setRefAt(thisOffset, other.getRefAt(refSlot.offset))
            }

          case (DuplicatedSlotKey(_, _), _) =>
          // no op
        })

      case _ =>
        throw new InternalException("Well well, isn't this a delicate situation?")
    }

  private def checkCompatibleNullablility(key: String, otherSlot: RefSlot): Boolean = {
    val thisSlot = slots.get(key).get
    // This should be guaranteed by slot allocation or else we could get incorrect results
    if (!thisSlot.nullable && otherSlot.nullable)
      throw new InternalException(s"Tried to merge slot $otherSlot into $thisSlot but its nullability is incompatible")
    true
  }

  override def createClone(): CypherRow = {
    val clone = SlottedRow(slots)
    clone.copyAllFrom(this)
    clone
  }

  override def isNull(key: String): Boolean =
    slots.get(key) match {
      case Some(RefSlot(offset, true, _)) if isRefInitialized(offset) =>
        getRefAtWithoutCheckingInitialized(offset) eq Values.NO_VALUE
      case Some(LongSlot(offset, true, CTNode)) =>
        entityIsNull(getLongAt(offset))
      case Some(LongSlot(offset, true, CTRelationship)) =>
        entityIsNull(getLongAt(offset))
      case _ =>
        false
    }

  private def iterator: Iterator[(String, AnyValue)] = {
    // This method implementation is for debug usage only.
    // Please do not use in production code.
    var tuples: List[(String, AnyValue)] = Nil
    def prettyKey(key: String, aliases: collection.Set[String]): String = (key +: aliases.toSeq).mkString(",")
    slots.foreachSlotAndAliasesOrdered({
      case SlotWithKeyAndAliases(VariableSlotKey(key), RefSlot(offset, _, _), aliases) =>
        tuples ::= ((prettyKey(key, aliases), refs(offset)))
      case SlotWithKeyAndAliases(VariableSlotKey(key), LongSlot(offset, _, _), aliases) =>
        tuples ::= ((prettyKey(key, aliases), Values.longValue(longs(offset))))
      case SlotWithKeyAndAliases(CachedPropertySlotKey(cachedProperty), slot, _) =>
        tuples ::= ((cachedProperty.asCanonicalStringVal, refs(slot.offset)))
      case SlotWithKeyAndAliases(MetaDataSlotKey(key, id), slot, _) =>
        tuples ::= ((s"MetaData($key, $id)", refs(slot.offset)))
      case SlotWithKeyAndAliases(ApplyPlanSlotKey(id), slot, _) =>
        tuples ::= ((s"Apply-Plan($id)", Values.longValue(longs(slot.offset))))
      case SlotWithKeyAndAliases(OuterNestedApplyPlanSlotKey(id), slot, _) =>
        tuples ::= ((s"Nested-Apply-Plan($id)", Values.longValue(longs(slot.offset))))
      case SlotWithKeyAndAliases(DuplicatedSlotKey(key, id), slot, _) =>
        tuples ::= ((
          s"DuplicatedSlot($key, $id)",
          if (slot.isLongSlot) Values.longValue(longs(slot.offset))
          else refs(slot.offset)
        ))
    })
    tuples.iterator
  }

  /**
   * Removes slots that have been marked as "discarded" in the slot configuration.
   *
   * Caution!! Only safe to call when the current "pipeline" is done processing this row.
   * In slotted this is before a "break", where rows are copied
   * (see [[SlottedPipelineBreakingPolicy]]).
   */
  // Note, consider adding a test case in SlottedPipelineBreakingPolicyTest
  // if you add calls to this method
  override def compact(): Unit = {
    if (refs.length > 0 && slots.discardedRefSlotOffsets().nonEmpty) {
      val discard = slots.discardedRefSlotOffsets()
      var i = 0
      while (i < discard.length) {
        refs(discard(i)) = null
        i += 1
      }
    }
  }
}
