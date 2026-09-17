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

import org.neo4j.cypher.internal.expressions.ASTCachedProperty
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.macros.AssertMacros3
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.ApplyPlanSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.CachedPropertySlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.DuplicatedSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.MetaDataSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.OuterNestedApplyPlanSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.SlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.SlotWithKeyAndAliases
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.VariableSlotKey
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.exceptions.InternalException

import java.util.Objects

import scala.collection.mutable

object SlotConfigurationBuilder {
  def empty = new SlotConfigurationBuilder(mutable.Map.empty, 0, 0, new mutable.BitSet(0))
}

/**
 * Mutable slot configuration, used to build [[SlotConfiguration]].
 * Contains array offsets and type information of the driving table columns.
 *
 * @param slots the slots of the configuration.
 * @param numberOfLongs the number of long slots.
 * @param numberOfReferences the number of ref slots.
 * @param markedDiscarded ref slot offsets that have been marked as can be discarded on next copy
 * @see [[SlotConfiguration]]
 */
final class SlotConfigurationBuilder private (
  private val slots: mutable.Map[SlotKey, Slot],
  var numberOfLongs: Int,
  var numberOfReferences: Int,
  private val markedDiscarded: mutable.BitSet
) extends SlotView[Slot] with LegacySlotView {

  override def apply(key: SlotKey): Slot = slots(key)
  override def get(key: SlotKey): Option[Slot] = slots.get(key)
  override def contains(key: SlotKey): Boolean = slots.contains(key)
  override def getSlot(key: SlotKey): Option[Slot] = get(key)

  override def longOffset(key: SlotKey): Int = {
    val slot = apply(key)
    require(slot.isLongSlot)
    slot.offset
  }

  override def refOffset(key: SlotKey): Int = {
    val slot = apply(key)
    require(!slot.isLongSlot)
    slot.offset
  }

  override def iterable: Iterable[(SlotKey, Slot)] = slots.view.filter {
    case (VariableSlotKey(variable), _) => !isAlias(variable)
    case _                              => true
  }

  override def iterableWithAliases: Iterable[(SlotKey, Slot)] = slots.view

  override def keyedSlots: Iterable[SlotWithKeyAndAliases] = iterable.map {
    case (key @ VariableSlotKey(variable), slot) =>
      SlotWithKeyAndAliases(key, slot, slotAliases.get(variable).map(_.toSet).getOrElse(Set.empty))
    case (key, slot) =>
      SlotWithKeyAndAliases(key, slot, Set.empty)
  }

  // For each existing variable key, a mapping to all aliases.
  // If x is added first, and y and z are aliases of x, the mapping will look like "x" -> Set("y", "z")
  // Contains only information about VariableSlotKeys
  private val slotAliases = new mutable.HashMap[String, mutable.Set[String]] with mutable.MultiMap[String, String]

  var finalized: Boolean = false // Is this  SlotConfiguration still open for mutations?

  def size(): SlotConfiguration.Size = SlotConfiguration.Size(numberOfLongs, numberOfReferences)

  def addAlias(newKey: LogicalVariable, existingKey: String): SlotConfigurationBuilder = {
    addAlias(newKey.name, existingKey)
  }

  def addAlias(newKey: String, existingKey: String): SlotConfigurationBuilder = {
    require(!finalized)
    val slot = slots.getOrElse(
      VariableSlotKey(existingKey),
      throw SlotAllocationFailed.internalError(
        this.getClass.getSimpleName,
        s"Tried to alias non-existing slot '$existingKey' with alias '$newKey'"
      )
    )
    markNotDiscarded(slot)
    val maybeOldSlot = slots.put(VariableSlotKey(newKey), slot)
    slotAliases.addBinding(rootKey(existingKey), newKey)

    // In case the new alias replaces an existing slot and moves it to a new slot offset
    // we also need to preserve the old slot offset to make sure we can use Arraycopy in CartesianProduct.
    // The old slot will never be read again but the slot offsets must be a continuous series to
    // avoid off-by-one or ArrayIndexOutOfBoundsException errors.
    // See {@link  SlotConfiguration.newCachedProperty()} for more details.
    maybeOldSlot.foreach(oldSlot => {
      if (!slots.exists { case (_, slot) => slot.offset == oldSlot.offset && slot.isLongSlot == oldSlot.isLongSlot }) {
        slots.put(DuplicatedSlotKey(newKey, oldSlot.offset), oldSlot.asNullable)
        slotAliases.remove(newKey)
      }
    })

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
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
      get(key).isDefined,
      s"Ran `isAlias` on $key which is not part of the slot configuration."
    )
    !slotAliases.contains(key)
  }

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
  def copy(): SlotConfigurationBuilder = {
    val newPipeline = new SlotConfigurationBuilder(
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
      }.getOrElse(throw InternalException.internalError(
        this.getClass.getSimpleName,
        s"No original key found for alias $key"
      ))
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
        case config =>
          throw InternalException.internalError(this.getClass.getSimpleName, s"Unexpected slot configuration: $config")
      }
      replaceExistingSlot(key, existingSlot, modifiedSlot)
    }
  }

  def newLong(key: LogicalVariable, nullable: Boolean, typ: CypherType): SlotConfigurationBuilder = {
    newLong(key.name, nullable, typ)
  }

  def newLong(key: String, nullable: Boolean, typ: CypherType): SlotConfigurationBuilder = {
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
      typ == CTNode || typ == CTRelationship,
      s"Invalid type: $typ. Part of the runtime implementation depends on this, for example pipelined ForEach"
    )
    require(!finalized)
    val slot = LongSlot(numberOfLongs, nullable, typ)
    slots.get(VariableSlotKey(key)) match {
      case Some(existingSlot) =>
        if (!existingSlot.isTypeCompatibleWith(slot)) {
          throw InternalException.internalError(
            this.getClass.getSimpleName,
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

  def newArgument(applyPlanId: Id): SlotConfigurationBuilder = {
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
  def newNestedArgument(applyPlanId: Id): SlotConfigurationBuilder = {
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

  def newReference(key: LogicalVariable, nullable: Boolean, typ: CypherType): SlotConfigurationBuilder = {
    newReference(key.name, nullable, typ)
  }

  def newReference(key: String, nullable: Boolean, typ: CypherType): SlotConfigurationBuilder = {
    require(!finalized)
    val slot = RefSlot(numberOfReferences, nullable, typ)
    val slotKey = VariableSlotKey(key)
    slots.get(slotKey) match {
      case Some(existingSlot) =>
        markNotDiscarded(existingSlot)
        if (!existingSlot.isTypeCompatibleWith(slot)) {
          throw InternalException.internalError(
            this.getClass.getSimpleName,
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

  def newCachedProperty(
    key: ASTCachedProperty.RuntimeKey,
    shouldDuplicate: Boolean = false
  ): SlotConfigurationBuilder = {
    require(!finalized)
    val slotKey = CachedPropertySlotKey(key)
    slots.get(slotKey) match {
      case Some(_) =>
        // RefSlots for cached node properties are always compatible and identical in nullability and type. We can therefore reuse the existing slot.
        if (shouldDuplicate) {
          // In case we want to copy a whole bunch of slots at runtime using Arraycopy, we dont want to exclude same cached property slot,
          // even if it already exists in the row. To make that possible, we add a new duplicated slot key and a ref slot that will act as
          // a placeholder for the additional slot. We can then simply copy the cached property into that position together with all
          // the other slots. We won't read it ever again from that array position, we will rather read the duplicate that exists at some other position
          // in the row.
          newDuplicatedRefSlot(key.asCanonicalStringVal)
        }

      case None =>
        slots.put(slotKey, RefSlot(numberOfReferences, nullable = false, CTAny))
        numberOfReferences = numberOfReferences + 1
    }
    this
  }

  def newDuplicatedRefSlot(name: String, typ: CypherType = CTAny): SlotConfigurationBuilder = {
    require(!finalized)
    slots.put(
      DuplicatedSlotKey(name, numberOfReferences),
      RefSlot(numberOfReferences, nullable = true, typ)
    )
    numberOfReferences = numberOfReferences + 1
    this
  }

  def newDuplicatedLongSlot(originalName: String, typ: CypherType = CTAny): SlotConfigurationBuilder = {
    require(!finalized)
    slots.put(
      DuplicatedSlotKey(originalName, numberOfLongs),
      LongSlot(numberOfLongs, nullable = true, typ)
    )
    numberOfLongs = numberOfLongs + 1
    this
  }

  def newMetaData(key: String, planId: Id = Id.INVALID_ID): SlotConfigurationBuilder = {
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

  /**
   * Add all slots to another slot configuration. Also add aliases.
   *
   * @param other     the slots will be added here
   * @param skipFirst the amount of longs and refs to be skipped in the beginning
   */
  def addAllSlotsInOrderTo(
    other: SlotConfigurationBuilder,
    skipFirst: SlotConfiguration.Size = SlotConfiguration.Size.zero
  ): Unit = {
    require(!finalized)
    keyedSlotsOrdered(skipFirst).foreach {
      case SlotWithKeyAndAliases(VariableSlotKey(key), slot, aliases) =>
        if (other.get(VariableSlotKey(key)).isDefined) {
          // `key` is already bound in `other` (e.g. the same variable is bound on both sides
          // of a CartesianProduct). We must still reserve a slot here: the runtime pipe that
          // consumes this slot configuration (e.g. CartesianProductSlottedPipe) copies a
          // contiguous, count-based range of slots via System.arraycopy and has no notion of
          // by-name deduplication, so every slot beyond `skipFirst` needs a corresponding
          // reserved slot in `other`, even when we already have a slot for this variable name.
          // We add a placeholder duplicate slot (never looked up by name) purely to reserve the
          // array position; the live/authoritative slot for `key` remains the one already
          // present in `other`. See issue #13981: without this, the reserved slot count could
          // undercount what the runtime actually copies, causing an
          // ArrayIndexOutOfBoundsException.
          if (slot.isLongSlot) other.newDuplicatedLongSlot(key)
          else other.newDuplicatedRefSlot(key)
        } else {
          other.add(key, slot)
        }
        aliases.foreach { alias =>
          other.addAlias(alias, key)
        }
      case SlotWithKeyAndAliases(CachedPropertySlotKey(key), _, _) =>
        other.newCachedProperty(key, shouldDuplicate = true)
      case SlotWithKeyAndAliases(DuplicatedSlotKey(key, _), slot, _) =>
        if (slot.isLongSlot) other.newDuplicatedLongSlot(key)
        else other.newDuplicatedRefSlot(key)
      case SlotWithKeyAndAliases(MetaDataSlotKey(key, id), _, _)      => other.newMetaData(key, id)
      case SlotWithKeyAndAliases(ApplyPlanSlotKey(applyPlanId), _, _) => other.newArgument(applyPlanId)
      case SlotWithKeyAndAliases(OuterNestedApplyPlanSlotKey(applyPlanId), _, _) =>
        other.newNestedArgument(applyPlanId)
    }
  }

  def addArgumentAliasesTo(other: SlotConfigurationBuilder, argumentSize: SlotConfiguration.Size): Unit = {
    require(!finalized)
    slots.foreach {
      case (VariableSlotKey(original), slot) =>
        if (argumentSize.contains(slot)) {
          slotAliases.get(original).foreach(_.foreach(alias => other.addAlias(alias, original)))
        }
      case _ => ()
    }
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[SlotConfigurationBuilder]

  override def equals(other: Any): Boolean = other match {
    case that: SlotConfigurationBuilder =>
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
    s"markedDiscarded=${markedDiscarded.mkString(",")}, " +
    s"slots=$slots)"

  /**
   * Marks that a slot can be discarded in next copy.
   */
  def markDiscarded(key: String): Unit = {
    require(!finalized)
    // We only discard ref slots that are not an alias and has ano aliases
    if (slotAliases.get(key).exists(_.isEmpty)) {
      get(key) match {
        case Some(RefSlot(offset, _, _)) => markedDiscarded += offset
        case _                           =>
      }
    }
  }

  def markDiscarded(key: LogicalVariable): Unit = {
    require(!finalized)
    markDiscarded(key.name)
  }

  private def markNotDiscarded(slot: Slot): Unit = {
    require(!finalized)
    if (!slot.isLongSlot && markedDiscarded.contains(slot.offset)) {
      markedDiscarded.remove(slot.offset)
    }
  }

  def discardedRefOffsets(): Array[Int] = markedDiscarded.toArray

  def build(): SlotConfiguration = {
    finalized = true
    SlotConfiguration(this)
  }
}

trait LegacySlotView {
  def iterable: Iterable[(SlotKey, Slot)]
  def iterableWithAliases: Iterable[(SlotKey, Slot)]
  def keyedSlots: Iterable[SlotWithKeyAndAliases]

  def keyedSlotsOrdered(skipFirst: SlotConfiguration.Size = SlotConfiguration.Size.zero): Seq[SlotWithKeyAndAliases] = {
    keyedSlots
      .filter {
        case SlotWithKeyAndAliases(_, LongSlot(offset, _, _), _) => offset >= skipFirst.nLongs
        case SlotWithKeyAndAliases(_, RefSlot(offset, _, _), _)  => offset >= skipFirst.nReferences
      }
      .toSeq
      .sortBy(s => (!s.slot.isLongSlot, s.slot.offset))
  }
}
