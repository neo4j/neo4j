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

import org.eclipse.collections.api.list.primitive.MutableIntList
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList
import org.neo4j.cypher.internal.expressions.ASTCachedProperty
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.ApplyPlanSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.CachedPropertySlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.MetaDataSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.OuterNestedApplyPlanSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.SlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.SlotWithKeyAndAliases
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.VariableSlotKey
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.EntityById
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.exceptions.InternalException
import org.neo4j.values.AnyValue

import java.util.Objects

import scala.collection.mutable

object SlotConfiguration {
  def empty = new SlotConfiguration(mutable.Map.empty, 0, 0, new mutable.BitSet(0))

  case class Size(nLongs: Int, nReferences: Int) {

    def contains(slot: Slot): Boolean =
      slot match {
        case s: LongSlot => s.offset < nLongs
        case s: RefSlot  => s.offset < nReferences
      }
  }

  object Size {
    val zero: Size = Size(nLongs = 0, nReferences = 0)
  }

  final def isRefSlotAndNotAlias(slots: SlotConfiguration, k: LogicalVariable): Boolean = {
    !slots.isAlias(k.name) &&
    slots.get(k.name).forall(_.isInstanceOf[RefSlot])
  }

  final def isRefSlotAndNotAlias(slots: SlotConfiguration, k: String): Boolean = {
    !slots.isAlias(k) &&
    slots.get(k).forall(_.isInstanceOf[RefSlot])
  }

  sealed trait SlotKey
  case class VariableSlotKey(name: String) extends SlotKey
  case class CachedPropertySlotKey(property: ASTCachedProperty.RuntimeKey) extends SlotKey
  case class ApplyPlanSlotKey(applyPlanId: Id) extends SlotKey
  case class OuterNestedApplyPlanSlotKey(applyPlanId: Id) extends SlotKey
  case class MetaDataSlotKey(name: String, planId: Id) extends SlotKey

  case class SlotWithKeyAndAliases(key: SlotKey, slot: Slot, aliases: collection.Set[String])
}

/**
 * A configuration which maps variables to slots. Two types of slot exists: LongSlot and RefSlot. In LongSlots we
 * store nodes and relationships, represented by their ids, and in RefSlots everything else, represented as AnyValues.
 *
 * @param slots the slots of the configuration.
 * @param numberOfLongs the number of long slots.
 * @param numberOfReferences the number of ref slots.
 * @param markedDiscarded ref slot offsets that have been marked as can be discarded on next copy
 */
class SlotConfiguration private (
  // Note, make sure to sync cachedPropertyOffsets when adding mutating calls to this map
  private val slots: mutable.Map[SlotConfiguration.SlotKey, Slot],
  var numberOfLongs: Int,
  var numberOfReferences: Int,
  private val markedDiscarded: mutable.BitSet
) {

  // Contains all slot offsets of cached property slots, for fast access.
  // NOTE! This needs to stay in sync with the content of `slots` (for entries with a CachedPropertySlotKey)
  private val cachedPropertyOffsets: MutableIntList = {
    val offsets = slots.iterator
      .collect { case (CachedPropertySlotKey(_), slot) => slot.offset }
      .toArray
    IntArrayList.newListWith(offsets: _*)
  }

  // For each existing variable key, a mapping to all aliases.
  // If x is added first, and y and z are aliases of x, the mapping will look like "x" -> Set("y", "z")
  // Contains only information about VariableSlotKeys
  private val slotAliases = new mutable.HashMap[String, mutable.Set[String]] with mutable.MultiMap[String, String]

  var finalized: Boolean = false // Is this SlotConfiguration still open for mutations?
  private val getters: mutable.Map[String, CypherRow => AnyValue] = new mutable.HashMap[String, CypherRow => AnyValue]()

  private val setters: mutable.Map[String, (CypherRow, AnyValue) => Unit] =
    new mutable.HashMap[String, (CypherRow, AnyValue) => Unit]()

  private val primitiveNodeSetters: mutable.Map[String, (CypherRow, Long, EntityById) => Unit] =
    new mutable.HashMap[String, (CypherRow, Long, EntityById) => Unit]()

  private val primitiveRelationshipSetters: mutable.Map[String, (CypherRow, Long, EntityById) => Unit] =
    new mutable.HashMap[String, (CypherRow, Long, EntityById) => Unit]()

  def size(): SlotConfiguration.Size = SlotConfiguration.Size(numberOfLongs, numberOfReferences)

  def addAlias(newKey: LogicalVariable, existingKey: String): SlotConfiguration = {
    addAlias(newKey.name, existingKey)
  }

  def addAlias(newKey: String, existingKey: String): SlotConfiguration = {
    require(!finalized)
    val slot = slots.getOrElse(
      VariableSlotKey(existingKey),
      throw new SlotAllocationFailed(s"Tried to alias non-existing slot '$existingKey'  with alias '$newKey'")
    )
    markNotDiscarded(slot)
    slots.put(VariableSlotKey(newKey), slot)
    slotAliases.addBinding(rootKey(existingKey), newKey)

    this
  }

  private def rootKey(key: String): String = {
    slotAliases
      .collectFirst { case (rootKey, aliases) if aliases.contains(key) => rootKey }
      .getOrElse(key)
  }

  /**
   * Test if a slot key refers to an alias.
   * NOTE: method can only test keys that are either 'original key' or alias, MUST NOT be called on keys that are neither (i.e., do not exist in the configuration).
   */
  private def isAlias(key: String): Boolean = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      get(key).isDefined,
      s"Ran `isAlias` on $key which is not part of the slot configuration."
    )
    !slotAliases.contains(key)
  }

  def getAliasesFor(key: String): Set[String] = slotAliases.get(key).map(_.toSet).getOrElse(Set.empty)

  def apply(key: LogicalVariable): Slot = slots.apply(VariableSlotKey(key.name))
  def apply(key: String): Slot = slots.apply(VariableSlotKey(key))

  def nameOfSlot(offset: Int, longSlot: Boolean): Option[String] = slots.collectFirst {
    case (VariableSlotKey(name), LongSlot(o, _, _)) if longSlot && o == offset && !isAlias(name) => name
    case (VariableSlotKey(name), RefSlot(o, _, _)) if !longSlot && o == offset && !isAlias(name) => name
  }

  def filterSlots(f: ((SlotKey, Slot)) => Boolean): Iterable[Slot] = {
    slots.filter(f).values
  }

  def get(key: LogicalVariable): Option[Slot] = slots.get(VariableSlotKey(key.name))
  def get(key: String): Option[Slot] = slots.get(VariableSlotKey(key))

  def add(key: String, slot: Slot): Unit = {
    require(!finalized)
    slot match {
      case LongSlot(_, nullable, typ) => newLong(key, nullable, typ)
      case RefSlot(_, nullable, typ)  => newReference(key, nullable, typ)
    }
  }

  /**
   * Make a copy of this slot configuration.
   */
  def copy(): SlotConfiguration = {
    val newPipeline = new SlotConfiguration(
      slots = this.slots.clone(),
      numberOfLongs,
      numberOfReferences,
      this.markedDiscarded.clone()
    )
    slotAliases.foreach {
      case (key, aliases) =>
        newPipeline.slotAliases.put(key, mutable.Set.empty[String])
        aliases.foreach(alias => newPipeline.slotAliases.addBinding(key, alias))
    }
    newPipeline
  }

  @scala.annotation.tailrec
  final def replaceExistingSlot(key: String, existingSlot: Slot, modifiedSlot: Slot): Unit = {
    require(!finalized)
    if (slotAliases.contains(key)) {
      val existingAliases = slotAliases(key)
      // Propagate changes to all corresponding entries in the slots map
      slots.put(VariableSlotKey(key), modifiedSlot)
      existingAliases.foreach(alias => slots.put(VariableSlotKey(alias), modifiedSlot))
    } else {
      // Find original key
      val originalKey = slotAliases.collectFirst {
        case (slotKey, aliases) if aliases.contains(key) => slotKey
      }.getOrElse(throw new InternalException(s"No original key found for alias $key"))
      replaceExistingSlot(originalKey, existingSlot, modifiedSlot)
    }
  }

  private def unifyTypeAndNullability(key: String, existingSlot: Slot, newSlot: Slot): Unit = {
    val updateNullable = !existingSlot.nullable && newSlot.nullable
    val updateTyp = existingSlot.typ != newSlot.typ && !existingSlot.typ.isAssignableFrom(newSlot.typ)
    require(!updateTyp || newSlot.typ.isAssignableFrom(existingSlot.typ))
    if (updateNullable || updateTyp) {
      val modifiedSlot = (existingSlot, updateNullable, updateTyp) match {
        // We are conservative about nullability and increase it to true
        case (LongSlot(offset, _, _), true, true) =>
          LongSlot(offset, nullable = true, newSlot.typ)
        case (RefSlot(offset, _, _), true, true) =>
          RefSlot(offset, nullable = true, newSlot.typ)
        case (LongSlot(offset, _, typ), true, false) =>
          LongSlot(offset, nullable = true, typ)
        case (RefSlot(offset, _, typ), true, false) =>
          RefSlot(offset, nullable = true, typ)
        case (LongSlot(offset, nullable, _), false, true) =>
          LongSlot(offset, nullable, newSlot.typ)
        case (RefSlot(offset, nullable, _), false, true) =>
          RefSlot(offset, nullable, newSlot.typ)
        case config => throw new InternalException(s"Unexpected slot configuration: $config")
      }
      replaceExistingSlot(key, existingSlot, modifiedSlot)
    }
  }

  def newLong(key: LogicalVariable, nullable: Boolean, typ: CypherType): SlotConfiguration = {
    newLong(key.name, nullable, typ)
  }

  def newLong(key: String, nullable: Boolean, typ: CypherType): SlotConfiguration = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      typ == CTNode || typ == CTRelationship,
      s"Invalid type: $typ. Part of the runtime implementation depends on this, for example pipelined ForEach"
    )
    require(!finalized)
    val slot = LongSlot(numberOfLongs, nullable, typ)
    slots.get(VariableSlotKey(key)) match {
      case Some(existingSlot) =>
        if (!existingSlot.isTypeCompatibleWith(slot)) {
          throw new InternalException(
            s"Tried overwriting already taken variable name '$key' as $slot (was: $existingSlot)"
          )
        }
        // Reuse the existing (compatible) slot
        unifyTypeAndNullability(key, existingSlot, slot)

      case None =>
        slots.put(VariableSlotKey(key), slot)
        slotAliases.put(key, mutable.Set.empty[String])
        numberOfLongs = numberOfLongs + 1
    }
    this
  }

  def newArgument(applyPlanId: Id): SlotConfiguration = {
    require(!finalized)
    if (slots.contains(ApplyPlanSlotKey(applyPlanId))) {
      throw new IllegalStateException(s"Should only add argument once per plan, got plan with $applyPlanId twice")
    }
    if (applyPlanId != Id.INVALID_ID) { // Top level argument is not allocated
      slots.put(ApplyPlanSlotKey(applyPlanId), LongSlot(numberOfLongs, nullable = false, CTAny))
      numberOfLongs += 1
    }
    this
  }

  /**
   * Use for [[ApplyPlan]]s that have two levels of arguments, e.g., [[Trail]].
   */
  def newNestedArgument(applyPlanId: Id): SlotConfiguration = {
    require(!finalized)
    if (slots.contains(OuterNestedApplyPlanSlotKey(applyPlanId))) {
      throw new IllegalStateException(s"Should only add argument once per plan, got plan with $applyPlanId twice")
    }
    if (applyPlanId == Id.INVALID_ID) {
      throw new IllegalStateException(s"Nested argument can not be a Top Level argument")
    }
    slots.put(OuterNestedApplyPlanSlotKey(applyPlanId), LongSlot(numberOfLongs, nullable = false, CTAny))
    numberOfLongs += 1
    this
  }

  def newReference(key: LogicalVariable, nullable: Boolean, typ: CypherType): SlotConfiguration = {
    newReference(key.name, nullable, typ)
  }

  def newReference(key: String, nullable: Boolean, typ: CypherType): SlotConfiguration = {
    require(!finalized)
    val slot = RefSlot(numberOfReferences, nullable, typ)
    val slotKey = VariableSlotKey(key)
    slots.get(slotKey) match {
      case Some(existingSlot) =>
        markNotDiscarded(existingSlot)
        if (!existingSlot.isTypeCompatibleWith(slot)) {
          throw new InternalException(
            s"Tried overwriting already taken variable name '$key' as $slot (was: $existingSlot)"
          )
        }
        // Reuse the existing (compatible) slot
        unifyTypeAndNullability(key, existingSlot, slot)

      case None =>
        slots.put(slotKey, slot)
        slotAliases.put(key, mutable.Set.empty[String])
        numberOfReferences = numberOfReferences + 1
    }
    this
  }

  def newCachedProperty(key: ASTCachedProperty.RuntimeKey, shouldDuplicate: Boolean = false): SlotConfiguration = {
    require(!finalized)
    val slotKey = CachedPropertySlotKey(key)
    slots.get(slotKey) match {
      case Some(_) =>
        // RefSlots for cached node properties are always compatible and identical in nullability and type. We can therefore reuse the existing slot.
        if (shouldDuplicate) {
          // In case we want to copy a whole bunch of slots at runtime using Arraycopy, we dont want to exclude same cached property slot,
          // even if it already exists in the row. To make that possible, we increase the number of references here, which will mean that there will be no
          // assigned slot for that array index in the references array. We can then simply copy the cached property into that position together with all
          // the other slots. We won't read it ever again from that array position, we will rather read the duplicate that exists at some other position
          // in the row.
          numberOfReferences += 1
        }

      case None =>
        slots.put(slotKey, RefSlot(numberOfReferences, nullable = false, CTAny))
        cachedPropertyOffsets.add(numberOfReferences)
        numberOfReferences = numberOfReferences + 1
    }
    this
  }

  def newMetaData(key: String, planId: Id = Id.INVALID_ID): SlotConfiguration = {
    require(!finalized)
    val slotKey = MetaDataSlotKey(key, planId)
    slots.get(slotKey) match {
      case Some(_) =>
      // For LoadCSV we only support meta data from one clause at a time, so we allow the same key to be
      // used multiple times mapping to the same slot, and the runtime value to be overwritten by the latest clause.

      case None =>
        slots.put(slotKey, RefSlot(numberOfReferences, nullable = false, CTAny))
        numberOfReferences += 1
    }
    this
  }

  def getReferenceOffsetFor(variable: LogicalVariable): Int = getReferenceOffsetFor(variable.name)

  def getReferenceOffsetFor(name: String): Int = slots.get(VariableSlotKey(name)) match {
    case Some(s: RefSlot) => s.offset
    case Some(s) => throw new InternalException(s"Uh oh... There was no reference slot for `$name`. It was a $s")
    case _       => throw new InternalException(s"Uh oh... There was no slot for `$name`")
  }

  def getLongOffsetFor(variable: LogicalVariable): Int = getLongOffsetFor(variable.name)

  def getLongOffsetFor(name: String): Int = slots.get(VariableSlotKey(name)) match {
    case Some(s: LongSlot) => s.offset
    case Some(s)           => throw new InternalException(s"Uh oh... There was no long slot for `$name`. It was a $s")
    case _                 => throw new InternalException(s"Uh oh... There was no slot for `$name`")
  }

  def getArgumentLongOffsetFor(applyPlanId: Id): Int = {
    if (applyPlanId == Id.INVALID_ID) {
      TopLevelArgument.SLOT_OFFSET
    } else {
      slots.getOrElse(
        ApplyPlanSlotKey(applyPlanId),
        throw new InternalException(s"No argument slot allocated for plan with $applyPlanId")
      ).offset
    }
  }

  /**
   * Returns the outer-level argument, for an [[ApplyPlan]] that has two-level/nested arguments.
   * E.g., the LHS argument for [[Trail]].
   */
  def getOuterNestedArgumentLongOffsetFor(applyPlanId: Id): Int = {
    if (applyPlanId == Id.INVALID_ID) {
      TopLevelArgument.SLOT_OFFSET
    } else {
      slots.getOrElse(
        OuterNestedApplyPlanSlotKey(applyPlanId),
        throw new InternalException(s"No nested argument slot allocated for plan with $applyPlanId")
      ).offset
    }
  }

  def getCachedPropertyOffsetFor(prop: ASTCachedProperty): Int = slots(CachedPropertySlotKey(prop.runtimeKey)).offset

  def getCachedPropertyOffsetFor(key: ASTCachedProperty.RuntimeKey): Int = slots(CachedPropertySlotKey(key)).offset

  def getMetaDataOffsetFor(key: String, id: Id = Id.INVALID_ID): Int = slots(MetaDataSlotKey(key, id)).offset

  def getMetaDataOffsetFor(key: MetaDataSlotKey): Int = slots(key).offset

  def updateAccessorFunctions(
    key: String,
    getter: CypherRow => AnyValue,
    setter: (CypherRow, AnyValue) => Unit,
    primitiveNodeSetter: Option[(CypherRow, Long, EntityById) => Unit],
    primitiveRelationshipSetter: Option[(CypherRow, Long, EntityById) => Unit]
  ): Unit = {
    require(!finalized)
    getters += key -> getter
    setters += key -> setter
    primitiveNodeSetter.foreach(primitiveNodeSetters += key -> _)
    primitiveRelationshipSetter.foreach(primitiveRelationshipSetters += key -> _)
  }

  def getter(key: String): CypherRow => AnyValue = {
    getters(key)
  }

  def setter(key: String): (CypherRow, AnyValue) => Unit = {
    setters(key)
  }

  def maybeGetter(key: String): Option[CypherRow => AnyValue] = {
    getters.get(key)
  }

  def maybeSetter(key: String): Option[(CypherRow, AnyValue) => Unit] = {
    setters.get(key)
  }

  def maybePrimitiveNodeSetter(key: String): Option[(CypherRow, Long, EntityById) => Unit] = {
    primitiveNodeSetters.get(key)
  }

  def maybePrimitiveRelationshipSetter(key: String): Option[(CypherRow, Long, EntityById) => Unit] = {
    primitiveRelationshipSetters.get(key)
  }

  // Helper to filter the slots map by aliases
  private def slotsAliasesFilter: mutable.Map[SlotKey, Slot] = slots.filter {
    case (VariableSlotKey(name), _) => !isAlias(name)
    case _                          => true
  }

  // Helper to map tuples of (SlotKey, Slot) to SlotWithKeyAndAliases
  private val slotKeySlotTupleAliasesMapper: ((SlotKey, Slot)) => SlotWithKeyAndAliases = {
    case (slotKey @ VariableSlotKey(name), slot) => SlotWithKeyAndAliases(slotKey, slot, slotAliases(name))
    case (otherKey, slot)                        => SlotWithKeyAndAliases(otherKey, slot, Set.empty)
  }

  /**
   * Apply a function to all SlotKeys.
   * SlotKeys that are aliases will be skipped.
   */
  def foreachSlot[U](f: ((SlotKey, Slot)) => U): Unit = {
    slotsAliasesFilter.foreach(f)
  }

  /**
   * Map all SlotKeys with the provided function.
   * SlotKeys that are aliases will NOT be skipped.
   */
  def mapSlotsDoNotSkipAliases[U](f: ((SlotKey, Slot)) => U): Iterable[U] = {
    slots.map(f)
  }

  /**
   * Apply a function to all SlotKeys.
   * SlotKeys that are aliases will be skipped. But, all aliases of a SlotKey are given together with the original SlotKey,
   * in case the caller wants to do something with the aliases.
   */
  def foreachSlotAndAliases(f: SlotWithKeyAndAliases => Unit): Unit = {
    foreachSlot(slotKeySlotTupleAliasesMapper.andThen(f))
  }

  /**
   * Apply a function to all SlotKeys.
   * The function will be applied in increasing slot offset order, first for the long slots and then for the ref slots.
   * SlotKeys that are aliases will be skipped. But, all aliases of a SlotKey are given together with the original SlotKey,
   * in case the caller wants to do something with the aliases.
   *
   * @param skipFirst the amount of longs and refs to be skipped in the beginning
   */
  def foreachSlotAndAliasesOrdered(
    f: SlotWithKeyAndAliases => Unit,
    skipFirst: SlotConfiguration.Size = SlotConfiguration.Size.zero
  ): Unit = {
    val (longs, refs) = slots.toSeq.partition(_._2.isLongSlot)

    def shouldApplyFunction(slotKey: SlotKey): Boolean = slotKey match {
      case VariableSlotKey(name) => !isAlias(name)
      case _                     => true
    }

    longs.filter {
      case (slotkey, slot) =>
        slot.offset >= skipFirst.nLongs && shouldApplyFunction(slotkey)
    }.sortBy(_._2.offset).map {
      case (slotKey @ VariableSlotKey(name), slot) => SlotWithKeyAndAliases(slotKey, slot, slotAliases(name))
      case (otherKey, slot)                        => SlotWithKeyAndAliases(otherKey, slot, Set.empty)
    }.foreach(f)

    refs.filter {
      case (slotkey, slot) => slot.offset >= skipFirst.nReferences && shouldApplyFunction(slotkey)
    }.sortBy(_._2.offset).map {
      case (slotKey @ VariableSlotKey(name), slot) => SlotWithKeyAndAliases(slotKey, slot, slotAliases(name))
      case (otherKey, slot)                        => SlotWithKeyAndAliases(otherKey, slot, Set.empty)
    }.foreach(f)
  }

  /**
   * Add all slots to another slot configuration. Also add aliases.
   *
   * @param other     the slots will be added here
   * @param skipFirst the amount of longs and refs to be skipped in the beginning
   */
  def addAllSlotsInOrderTo(
    other: SlotConfiguration,
    skipFirst: SlotConfiguration.Size = SlotConfiguration.Size.zero
  ): Unit = {
    require(!finalized)
    foreachSlotAndAliasesOrdered(
      {
        case SlotWithKeyAndAliases(VariableSlotKey(key), slot, aliases) =>
          other.add(key, slot)
          aliases.foreach { alias =>
            other.addAlias(alias, key)
          }
        case SlotWithKeyAndAliases(CachedPropertySlotKey(key), _, _) =>
          other.newCachedProperty(key, shouldDuplicate = true)
        case SlotWithKeyAndAliases(MetaDataSlotKey(key, id), _, _)      => other.newMetaData(key, id)
        case SlotWithKeyAndAliases(ApplyPlanSlotKey(applyPlanId), _, _) => other.newArgument(applyPlanId)
        case SlotWithKeyAndAliases(OuterNestedApplyPlanSlotKey(applyPlanId), _, _) =>
          other.newNestedArgument(applyPlanId)
      },
      skipFirst
    )
  }

  def addArgumentAliasesTo(other: SlotConfiguration, argumentSize: SlotConfiguration.Size): Unit = {
    slots.foreach {
      case (VariableSlotKey(original), slot) =>
        if (argumentSize.contains(slot)) {
          slotAliases.get(original).foreach(_.foreach(alias => other.addAlias(alias, original)))
        }
      case _ => ()
    }
  }

  def foreachCachedPropertySlotOffset(onOffset: Int => Unit): Unit = {
    cachedPropertyOffsets.forEach(offset => onOffset(offset))
  }

  def foreachCachedSlot(onCachedProperty: ((ASTCachedProperty.RuntimeKey, RefSlot)) => Unit): Unit = {
    slots.iterator.foreach {
      case (CachedPropertySlotKey(key), slot: RefSlot) => onCachedProperty(key -> slot)
      case _                                           => // do nothing
    }
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[SlotConfiguration]

  override def equals(other: Any): Boolean = other match {
    case that: SlotConfiguration =>
      (that canEqual this) &&
      slots == that.slots &&
      numberOfLongs == that.numberOfLongs &&
      numberOfReferences == that.numberOfReferences &&
      markedDiscarded == that.markedDiscarded
    case _ => false
  }

  override def hashCode(): Int = {
    Objects.hash(slots, numberOfLongs, numberOfReferences, markedDiscarded)
  }

  override def toString: String = s"SlotConfiguration${System.identityHashCode(this)}(" +
    s"longs=$numberOfLongs, " +
    s"refs=$numberOfReferences, " +
    s"discarded=${discardedRefSlotOffsets().mkString(",")}, " +
    s"markedDiscarded=${markedDiscarded.mkString(",")}, " +
    s"slots=$slots)"

  def hasCachedPropertySlot(key: ASTCachedProperty.RuntimeKey): Boolean = slots.contains(CachedPropertySlotKey(key))

  def getCachedPropertySlot(key: ASTCachedProperty.RuntimeKey): Option[RefSlot] =
    slots.get(CachedPropertySlotKey(key)).asInstanceOf[Option[RefSlot]]

  def hasArgumentSlot(applyPlanId: Id): Boolean = slots.contains(ApplyPlanSlotKey(applyPlanId))

  def hasNestedArgumentSlot(applyPlanId: Id): Boolean = slots.contains(OuterNestedApplyPlanSlotKey(applyPlanId))

  def getArgumentSlot(applyPlanId: Id): Option[LongSlot] =
    slots.get(ApplyPlanSlotKey(applyPlanId)).asInstanceOf[Option[LongSlot]]

  def getNestedArgumentSlot(applyPlanId: Id): Option[LongSlot] =
    slots.get(OuterNestedApplyPlanSlotKey(applyPlanId)).asInstanceOf[Option[LongSlot]]

  def hasMetaDataSlot(key: String, id: Id): Boolean = slots.contains(MetaDataSlotKey(key, id))

  def hasMetaDataSlot(key: MetaDataSlotKey): Boolean = slots.contains(key)

  def getMetaDataSlot(key: String, id: Id = Id.INVALID_ID): Option[RefSlot] =
    slots.get(MetaDataSlotKey(key, id)).asInstanceOf[Option[RefSlot]]

  def getMetaDataSlot(key: MetaDataSlotKey): Option[RefSlot] = slots.get(key).asInstanceOf[Option[RefSlot]]

  def hasCachedProperties: Boolean = _hasCachedProperties

  /**
   * Marks that a slot can be discarded in next copy.
   */
  def markDiscarded(key: String): Unit = {
    // We only discard ref slots that are not an alias and has ano aliases
    if (slotAliases.get(key).exists(_.isEmpty)) {
      get(key) match {
        case Some(RefSlot(offset, _, _)) => markedDiscarded += offset
        case _                           =>
      }
    }
  }

  def markDiscarded(key: LogicalVariable): Unit = markDiscarded(key.name)

  private def markNotDiscarded(slot: Slot): Unit = {
    if (!slot.isLongSlot && markedDiscarded.contains(slot.offset)) {
      markedDiscarded.remove(slot.offset)
    }
  }

  private[this] var discardedRefSlotOffsetsSorted: Array[Int] = _

  def discardedRefSlotOffsets(): Array[Int] = {
    if (!finalized) {
      markedDiscarded.toArray
    } else if (discardedRefSlotOffsetsSorted != null) {
      discardedRefSlotOffsetsSorted
    } else {
      discardedRefSlotOffsetsSorted = markedDiscarded.toArray
      discardedRefSlotOffsetsSorted
    }
  }

  private var _hasCachedProperties = false // Cache this once for fast lookups during runtime

  def updateHasCachedProperties(): Unit = {
    require(!finalized)
    _hasCachedProperties = slots.exists {
      case (_: CachedPropertySlotKey, _) =>
        true
      case _ =>
        false
    }
  }
}
