/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.compatibility.v3_4.runtime

import org.neo4j.cypher.internal.util.v3_4.InternalException
import org.neo4j.cypher.internal.util.v3_4.symbols.CypherType
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.values.AnyValue

import scala.collection.{immutable, mutable}

object SlotConfiguration {
  def empty = new SlotConfiguration(mutable.Map.empty, 0, 0)

  def apply(slots: Map[String, Slot], numberOfLongs: Int, numberOfReferences: Int): SlotConfiguration = {
    val stringToSlot = mutable.Map(slots.toSeq: _*)
    new SlotConfiguration(stringToSlot, numberOfLongs, numberOfReferences)
  }

  def toString(startFrom: LogicalPlan, m: Map[LogicalPlan, SlotConfiguration]): String = {
    var lastSeen = 0

    def ordinal(): Int = {
      val result = lastSeen
      lastSeen += 1
      result
    }

    class Pipeline(val order: Int, val info: SlotConfiguration, var plans: Seq[LogicalPlan], var dependsOn: Seq[Pipeline]) {
      def addDependencyTo(toP: Pipeline): Unit = dependsOn = dependsOn :+ toP

      def addPlan(p: LogicalPlan): Unit = plans = plans :+ p
    }

    class PipelineBuilder(var buffer: Map[SlotConfiguration, Pipeline] = Map.empty[SlotConfiguration, Pipeline] ) {
      def addPlanAndPipelineInformation(lp: LogicalPlan, pipelineInformation: SlotConfiguration): PipelineBuilder = {
        val p = getOrCreatePipeline(pipelineInformation)
        p.addPlan(lp)
        this
      }

      def getOrCreatePipeline(key: SlotConfiguration): Pipeline = {
        buffer.getOrElse(key, {
          val pipeline = new Pipeline(ordinal(), key, Seq.empty, Seq.empty)
          buffer = buffer + (key -> pipeline)
          pipeline
        })
      }

      def addDependencyBetween(from: SlotConfiguration, to: SlotConfiguration): PipelineBuilder = {
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

          result.append(s"[$s $r ${slot.offset} ${slot.typ}] -> $key\n")
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

  def isLongSlot(slot: Slot): Boolean = slot match {
    case _: LongSlot => true
    case _ => false
  }

  case class Size(nLongs: Int, nReferences: Int)
  object Size {
    val zero = Size(nLongs = 0, nReferences = 0)
  }
}

/**
  * A configuration which maps variables to slots. Two types of slot exists: LongSlot and RefSlot. In LongSlots we
  * store nodes and relationships, represented by their ids, and in RefSlots everything else, represented as AnyValues.
  *
  * @param slots the slots of the configuration.
  * @param numberOfLongs the number of long slots.
  * @param numberOfReferences the number of ref slots.
  */
class SlotConfiguration(private val slots: mutable.Map[String, Slot],
                        var numberOfLongs: Int,
                        var numberOfReferences: Int) {


  private val aliases: mutable.Set[String] = mutable.Set()
  private val slotAliases = new mutable.HashMap[Slot, mutable.Set[String]] with mutable.MultiMap[Slot, String]

  private val getters: mutable.Map[String, ExecutionContext => AnyValue] = new mutable.HashMap[String, ExecutionContext => AnyValue]()
  private val setters: mutable.Map[String, (ExecutionContext, AnyValue) => Unit] = new mutable.HashMap[String, (ExecutionContext, AnyValue) => Unit]()
  private val primitiveNodeSetters: mutable.Map[String, (ExecutionContext, Long) => Unit] = new mutable.HashMap[String, (ExecutionContext, Long) => Unit]()
  private val primitiveRelationshipSetters: mutable.Map[String, (ExecutionContext, Long) => Unit] = new mutable.HashMap[String, (ExecutionContext, Long) => Unit]()

  def size() = SlotConfiguration.Size(numberOfLongs, numberOfReferences)

  def addAlias(newKey: String, existingKey: String): SlotConfiguration = {
    val slot = slots.getOrElse(existingKey,
      throw new SlotAllocationFailed(s"Tried to alias non-existing slot '$existingKey'  with alias '$newKey'"))
    slots.put(newKey, slot)
    aliases.add(newKey)
    slotAliases.addBinding(slot, newKey)
    this
  }

  def getAliasOf(slot: Slot): String = slotAliases(slot).head

  def isAlias(key: String): Boolean = {
    aliases.contains(key)
  }

  def apply(key: String): Slot = slots.apply(key)

  def get(key: String): Option[Slot] = slots.get(key)

  def add(key: String, slot: Slot): Unit = slot match {
    case LongSlot(_, nullable, typ) => newLong(key, nullable, typ)
    case RefSlot(_, nullable, typ) => newReference(key, nullable, typ)
  }

  def copy(): SlotConfiguration = {
    val newPipeline = new SlotConfiguration(this.slots.clone(), numberOfLongs, numberOfReferences)
    newPipeline.aliases ++= aliases
    newPipeline.slotAliases ++= slotAliases
    newPipeline
  }

  private def replaceExistingSlot(key: String, existingSlot: Slot, modifiedSlot: Slot): Unit = {
    slots.put(key, modifiedSlot)
    val existingAliases = slotAliases.get(existingSlot).get
    assert(existingAliases.contains(key))
    slotAliases.put(modifiedSlot, existingAliases)
    slotAliases.remove(existingSlot)
  }

  private def unifyTypeAndNullability(key: String, existingSlot: Slot, newSlot: Slot) = {
    val updateNullable = !existingSlot.nullable && newSlot.nullable
    val updateTyp = existingSlot.typ != newSlot.typ && !existingSlot.typ.isAssignableFrom(newSlot.typ)
    assert(!updateTyp || newSlot.typ.isAssignableFrom(existingSlot.typ))
    if (updateNullable || updateTyp) {
      val modifiedSlot = (existingSlot, updateNullable, updateTyp) match {
        // We are conservative about nullability and increase it to true
        case ((LongSlot(offset, _, _), true, true)) =>
          LongSlot(offset, true, newSlot.typ)
        case ((RefSlot(offset, _, _), true, true)) =>
          RefSlot(offset, true, newSlot.typ)
        case ((LongSlot(offset, _, typ), true, false)) =>
          LongSlot(offset, true, typ)
        case ((RefSlot(offset, _, typ), true, false)) =>
          RefSlot(offset, true, typ)
        case ((LongSlot(offset, nullable, _), false, true)) =>
          LongSlot(offset, nullable, newSlot.typ)
        case ((RefSlot(offset, nullable, _), false, true)) =>
          RefSlot(offset, nullable, newSlot.typ)
      }
      replaceExistingSlot(key, existingSlot, modifiedSlot);
    }
  }

  def newLong(key: String, nullable: Boolean, typ: CypherType): SlotConfiguration = {
    val slot = LongSlot(numberOfLongs, nullable, typ)
    slots.get(key) match {
      case Some(existingSlot) =>
        if (!existingSlot.isTypeCompatibleWith(slot)) {
          throw new InternalException(s"Tried overwriting already taken variable name $key as $slot (was: ${existingSlot})")
        }
        // Reuse the existing (compatible) slot
        unifyTypeAndNullability(key, existingSlot, slot)

      case None =>
        slots.put(key, slot)
        slotAliases.addBinding(slot, key)
        numberOfLongs = numberOfLongs + 1
    }
    this
  }

  def newReference(key: String, nullable: Boolean, typ: CypherType): SlotConfiguration = {
    val slot = RefSlot(numberOfReferences, nullable, typ)
    slots.get(key) match {
      case Some(existingSlot) =>
        if (!existingSlot.isTypeCompatibleWith(slot)) {
          throw new InternalException(s"Tried overwriting already taken variable name $key as $slot (was: ${existingSlot})")
        }
        // Reuse the existing (compatible) slot
        unifyTypeAndNullability(key, existingSlot, slot)

      case None =>
        slots.put(key, slot)
        slotAliases.addBinding(slot, key)
        numberOfReferences = numberOfReferences + 1
    }
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

  def updateAccessorFunctions(key: String, getter: ExecutionContext => AnyValue, setter: (ExecutionContext, AnyValue) => Unit,
                              primitiveNodeSetter: Option[(ExecutionContext, Long) => Unit],
                              primitiveRelationshipSetter: Option[(ExecutionContext, Long) => Unit]) = {
    getters += key -> getter
    setters += key -> setter
    primitiveNodeSetter.map(primitiveNodeSetters += key -> _)
    primitiveRelationshipSetter.map(primitiveRelationshipSetters += key -> _)
  }

  def getter(key: String): ExecutionContext => AnyValue = {
    getters(key)
  }

  def setter(key: String): (ExecutionContext, AnyValue) => Unit = {
    setters(key)
  }

  def maybeGetter(key: String): Option[ExecutionContext => AnyValue] = {
    getters.get(key)
  }

  def maybeSetter(key: String): Option[(ExecutionContext, AnyValue) => Unit] = {
    setters.get(key)
  }

  def maybePrimitiveNodeSetter(key: String): Option[(ExecutionContext, Long) => Unit] = {
    primitiveNodeSetters.get(key)
  }

  def maybePrimitiveRelationshipSetter(key: String): Option[(ExecutionContext, Long) => Unit] = {
    primitiveRelationshipSetters.get(key)
  }

  // NOTE: This will give duplicate slots when we have aliases
  def foreachSlot[U](f: ((String,Slot)) => U): Unit =
    slots.foreach(f)

  // NOTE: This will give duplicate slots when we have aliases
  def foreachSlotOrdered[U](f: ((String, Slot)) => U): Unit =
    slots.toSeq.sortBy(_._2)(SlotOrdering).foreach(f)

  // NOTE: This will give duplicate slots when we have aliases
  def mapSlot[U](f: ((String,Slot)) => U): Iterable[U] = slots.map(f)

  def partitionSlots(p: (String, Slot) => Boolean): (Seq[(String, Slot)], Seq[(String, Slot)]) = {
    slots.toSeq.partition {
      case (k, slot) =>
        p(k, slot)
    }
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[SlotConfiguration]

  override def equals(other: Any): Boolean = other match {
    case that: SlotConfiguration =>
      (that canEqual this) &&
        slots == that.slots &&
        numberOfLongs == that.numberOfLongs &&
        numberOfReferences == that.numberOfReferences
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq[Any](slots, numberOfLongs, numberOfReferences)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"SlotConfiguration(longs=$numberOfLongs, refs=$numberOfReferences, slots=$slots)"

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
