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

package org.neo4j.cypher.internal.compatibility.v3_4.runtime

import org.neo4j.cypher.internal.util.v3_4.InternalException
import org.neo4j.cypher.internal.util.v3_4.symbols.CypherType
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlan

import scala.collection.{immutable, mutable}

object PipelineInformation {
  def empty = new PipelineInformation(mutable.Map.empty, 0, 0)

  def apply(slots: Map[String, Slot], numberOfLongs: Int, numberOfReferences: Int) = {

    val stringToSlot = mutable.Map(slots.toSeq: _*)
    new PipelineInformation(stringToSlot, numberOfLongs, numberOfReferences)

  }

  def toString(startFrom: LogicalPlan, m: Map[LogicalPlan, PipelineInformation]): String = {
    var lastSeen = 0

    def ordinal(): Int = {
      val result = lastSeen
      lastSeen += 1
      result
    }

    class Pipeline(val order: Int, val info: PipelineInformation, var plans: Seq[LogicalPlan], var dependsOn: Seq[Pipeline]) {
      def addDependencyTo(toP: Pipeline): Unit = dependsOn = dependsOn :+ toP

      def addPlan(p: LogicalPlan): Unit = plans = plans :+ p
    }

    class PipelineBuilder(var buffer: Map[PipelineInformation, Pipeline] = Map.empty[PipelineInformation, Pipeline] ) {
      def addPlanAndPipelineInformation(lp: LogicalPlan, pipelineInformation: PipelineInformation): PipelineBuilder = {
        val p = getOrCreatePipeline(pipelineInformation)
        p.addPlan(lp)
        this
      }

      def getOrCreatePipeline(key: PipelineInformation): Pipeline = {
        buffer.getOrElse(key, {
          val pipeline = new Pipeline(ordinal(), key, Seq.empty, Seq.empty)
          buffer = buffer + (key -> pipeline)
          pipeline
        })
      }

      def addDependencyBetween(from: PipelineInformation, to: PipelineInformation): PipelineBuilder = {
        val fromP = getOrCreatePipeline(from)
        val toP = getOrCreatePipeline(to)
        if (fromP != toP) {
          fromP.addDependencyTo(toP)
        }
        this
      }
    }

    val acc = new PipelineBuilder()
    m.foreach {
      case (lp, pipelineInformation) =>
        acc.addPlanAndPipelineInformation(lp, pipelineInformation)

        lp.lhs.foreach {
          plan =>
            val incomingPipeline = m(plan)
            acc.addDependencyBetween(pipelineInformation, incomingPipeline)
        }

        lp.rhs.foreach {
          plan =>
            val incomingPipeline = m(plan)
            acc.addDependencyBetween(pipelineInformation, incomingPipeline)
        }
    }

    val result = new mutable.StringBuilder()

    def addToString(pipeline: Pipeline): Unit = {
      result.append(s"Pipeline ${pipeline.order}:\n")

      //Plans
      result.append(s"  -> ${pipeline.plans.head.getClass.getSimpleName}")
      pipeline.plans.tail.foreach {
        plan => result.append(", ").append(plan.getClass.getSimpleName)
      }
      result.append("\n")

      //Slots
      result.append("Slots:\n")
      pipeline.info.slots.foreach {
        case (key, slot) =>
          val s = if (slot.isInstanceOf[LongSlot]) "L" else "V"
          val r = if (slot.nullable) "T" else "F"

          result.append(s"[$s $r ${slot.offset} ${slot.typ}] -> ${key}\n")
      }

      result.append("\n")

      // Dependencies:
      result.append("Dependends on: ")
      pipeline.dependsOn.foreach(p => result.append("#").append(p.order))

      result.append("\n")
      result.append("*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+*-+\n")

      pipeline.dependsOn.foreach(addToString)
    }

    addToString(acc.buffer(m(startFrom)))

    result.toString()
  }

  def isLongSlot(slot: Slot) = slot match {
    case _: LongSlot => true
    case _ => false
  }
}

class PipelineInformation(private val slots: mutable.Map[String, Slot],
                          val initialNumberOfLongs: Int,
                          val initialNumberOfReferences: Int) {

  var numberOfLongs = initialNumberOfLongs
  var numberOfReferences = initialNumberOfReferences

  private val aliases: mutable.Set[String] = mutable.Set()

  def addAliasFor(slot: Slot, key: String): PipelineInformation = {
    checkNotAlreadyTaken(key, slot)
    slots.put(key, slot)
    aliases.add(key)
    slotAliases.addBinding(slot, key)
    this
  }

  def isAlias(key: String): Boolean = {
    aliases.contains(key)
  }
  private val slotAliases = new mutable.HashMap[Slot, mutable.Set[String]] with mutable.MultiMap[Slot, String]

  def apply(key: String): Slot = slots.apply(key)

  def get(key: String): Option[Slot] = slots.get(key)

  def add(key: String, slotInformation: Slot): Unit = slotInformation match {
    case LongSlot(_, nullable, typ) => newLong(key, nullable, typ)
    case RefSlot(_, nullable, typ) => newReference(key, nullable, typ)
  }

  def breakPipelineAndClone(): PipelineInformation = {
    val newPipeline = new PipelineInformation(this.slots.clone(), numberOfLongs, numberOfReferences)
    newPipeline.aliases ++= aliases
    newPipeline.slotAliases ++= slotAliases
    newPipeline
  }

  def newLong(key: String, nullable: Boolean, typ: CypherType): PipelineInformation = {
    val slot = LongSlot(numberOfLongs, nullable, typ)
    checkNotAlreadyTaken(key, slot)
    slots.put(key, slot)
    slotAliases.addBinding(slot, key)
    numberOfLongs = numberOfLongs + 1
    this
  }

  def newReference(key: String, nullable: Boolean, typ: CypherType): PipelineInformation = {
    val slot = RefSlot(numberOfReferences, nullable, typ)
    checkNotAlreadyTaken(key, slot)
    slots.put(key, slot)
    slotAliases.addBinding(slot, key)
    numberOfReferences = numberOfReferences + 1
    this
  }

  def getReferenceOffsetFor(name: String): Int = slots.get(name) match {
    case Some(s: RefSlot) => s.offset
    case Some(s) => throw new InternalException(s"Uh oh... There was no reference slot for `$name`. It was a $s")
    case _ => throw new InternalException(s"Uh oh... There was no slot for `$name`")
  }

  def getLongOffsetFor(name: String): Int = slots.get(name) match {
    case Some(s: LongSlot) => s.offset
    case Some(s) => throw new InternalException(s"Uh oh... There was no long slot for `$name`. It was a $s")
    case _ => throw new InternalException(s"Uh oh... There was no slot for `$name`")
  }

  // NOTE: This will give duplicate slots when we have aliases
  def foreachSlot[U](f: ((String,Slot)) => U): Unit =
    slots.foreach(f)

  // NOTE: This will give duplicate slots when we have aliases
  def foreachSlotOrdered[U](f: ((String,Slot)) => U): Unit =
    slots.toSeq.sortBy(_._2)(SlotOrdering).foreach(f)

  // NOTE: This will give duplicate slots when we have aliases
  def mapSlot[U](f: ((String,Slot)) => U): Iterable[U] = slots.map(f)

  def canEqual(other: Any): Boolean = other.isInstanceOf[PipelineInformation]

  override def equals(other: Any): Boolean = other match {
    case that: PipelineInformation =>
      (that canEqual this) &&
        slots == that.slots &&
        numberOfLongs == that.numberOfLongs &&
        numberOfReferences == that.numberOfReferences
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(slots, numberOfLongs, numberOfReferences)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"PipelineInformation(longs=$numberOfLongs, objs=$numberOfReferences, slots=$slots)"

  private def checkNotAlreadyTaken(key: String, slot: Slot) =
    if (slots.contains(key))
      throw new InternalException(s"Tried overwriting already taken variable name $key as $slot (was: ${slots.get(key).get})")

  /**
    * NOTE: Only use for debugging
    */
  def getLongSlots: immutable.IndexedSeq[SlotWithAliases] =
    slotAliases.toIndexedSeq.collect {
      case (slot: LongSlot, aliases) => LongSlotWithAliases(slot, aliases.toSet)
    }.sorted(SlotWithAliasesOrdering)

  /**
    * NOTE: Only use for debugging
    */
  def getRefSlots: immutable.IndexedSeq[SlotWithAliases] =
    slotAliases.toIndexedSeq.collect {
      case (slot: RefSlot, aliases) => RefSlotWithAliases(slot, aliases.toSet)
    }.sorted(SlotWithAliasesOrdering)

  object SlotWithAliasesOrdering extends Ordering[SlotWithAliases] {
    def compare(x: SlotWithAliases, y: SlotWithAliases): Int = (x, y) match {
      case (_: LongSlotWithAliases, _: RefSlotWithAliases) =>
        -1
      case (_: RefSlotWithAliases, _: LongSlotWithAliases) =>
        1
      case _ =>
        x.slot.offset - y.slot.offset
    }
  }

  object SlotOrdering extends Ordering[Slot] {
    def compare(x: Slot, y: Slot): Int = (x, y) match {
      case (_: LongSlot, _: RefSlot) =>
        -1
      case (_: RefSlot, _: LongSlot) =>
        1
      case _ =>
        x.offset - y.offset
    }
  }
}
