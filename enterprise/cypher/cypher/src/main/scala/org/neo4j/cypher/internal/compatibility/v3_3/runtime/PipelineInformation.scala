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

import org.neo4j.cypher.internal.frontend.v3_3.InternalException
import org.neo4j.cypher.internal.frontend.v3_3.symbols.CypherType


object PipelineInformation {
  def empty = new PipelineInformation(Map.empty, 0, 0)

  def apply(slots: Map[String, Slot], numberOfLongs: Int, numberOfReferences: Int) =
    new PipelineInformation(slots, numberOfLongs, numberOfReferences)
}

class PipelineInformation(private var slots: Map[String, Slot], var numberOfLongs: Int, var numberOfReferences: Int) {

  def apply(key: String): Slot = slots.apply(key)

  def get(key: String): Option[Slot] = slots.get(key)

  def add(key: String, slotInformation: Slot): Unit = slotInformation match {
    case LongSlot(_, nullable, typ, key) => newLong(key, nullable, typ)
    case RefSlot(_, nullable, typ, key) => newReference(key, nullable, typ)
  }

  def deepClone(): PipelineInformation = {
    new PipelineInformation(this.slots, numberOfLongs, numberOfReferences)
  }

  def newLong(name: String, nullable: Boolean, typ: CypherType): Unit = {
    checkNotAlreadyTaken(name)
    val slot = LongSlot(numberOfLongs, nullable, typ, name)
    slots = slots + (name -> slot)
    numberOfLongs = numberOfLongs + 1
  }

  def newReference(name: String, nullable: Boolean, typ: CypherType): Unit = {
    checkNotAlreadyTaken(name)
    val slot = RefSlot(numberOfReferences, nullable, typ, name)
    slots = slots + (name -> slot)
    numberOfReferences = numberOfReferences + 1
  }

  def getReferenceOffsetFor(name: String): Int = slots.get(name) match {
    case Some(s: RefSlot) => s.offset
    case Some(s) => throw new InternalException(s"Uh oh... There was no reference slot for `$name`. It was a $s")
    case _ => throw new InternalException("Uh oh... There was no slot for `$name`")
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
