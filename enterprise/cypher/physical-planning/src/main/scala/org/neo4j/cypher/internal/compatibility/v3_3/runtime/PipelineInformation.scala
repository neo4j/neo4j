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

package org.neo4j.cypher.internal.compatibility.v3_3.runtime

import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.frontend.v3_3.InternalException
import org.neo4j.cypher.internal.frontend.v3_3.symbols.CypherType

import scala.collection.mutable

object PipelineInformation {
  def empty = new PipelineInformation(Map.empty, 0, 0)

  def apply(slots: Map[String, Slot], numberOfLongs: Int, numberOfReferences: Int) =
    new PipelineInformation(slots, numberOfLongs, numberOfReferences)

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
      pipeline.info.slots.values.foreach {
        slot =>
          val s = if (slot.isInstanceOf[LongSlot]) "L" else "V"
          val r = if (slot.nullable) "T" else "F"

          result.append(s"[$s $r ${slot.offset} ${slot.typ}] -> ${slot.name}\n")
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
}

class PipelineInformation(private var slots: Map[String, Slot],
                          val initialNumberOfLongs: Int,
                          val initialNumberOfReferences: Int) {

  var numberOfLongs = initialNumberOfLongs
  var numberOfReferences = initialNumberOfReferences

  def apply(key: String): Slot = slots.apply(key)

  def get(key: String): Option[Slot] = slots.get(key)

  def add(key: String, slotInformation: Slot): Unit = slotInformation match {
    case LongSlot(_, nullable, typ, _) => newLong(key, nullable, typ)
    case RefSlot(_, nullable, typ, _) => newReference(key, nullable, typ)
  }

  def seedClone(): PipelineInformation = {
    new PipelineInformation(this.slots, numberOfLongs, numberOfReferences)
  }

  def newLong(name: String, nullable: Boolean, typ: CypherType): PipelineInformation = {
    checkNotAlreadyTaken(name)
    val slot = LongSlot(numberOfLongs, nullable, typ, name)
    slots = slots + (name -> slot)
    numberOfLongs = numberOfLongs + 1
    this
  }

  def newReference(name: String, nullable: Boolean, typ: CypherType): PipelineInformation = {
    checkNotAlreadyTaken(name)
    val slot = RefSlot(numberOfReferences, nullable, typ, name)
    slots = slots + (name -> slot)
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
    case _ => throw new InternalException("Uh oh... There was no slot for `$name`")
  }

  def foreachSlot[U](f: ((String,Slot)) => U): Unit =
    slots.foreach(f)

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

  override def toString = s"PipelineInformation(slots=$slots, longs=$numberOfLongs, objs=$numberOfReferences)"

  private def checkNotAlreadyTaken(key: String) =
    if (slots.contains(key))
      throw new InternalException("Tried overwriting already taken variable name")
}
