/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cypher.internal.compatibility.v3_4.runtime

import org.neo4j.cypher.internal.util.v3_4.symbols.CypherType

sealed trait Slot {
  def offset: Int
  def nullable: Boolean
  def typ: CypherType
  def isTypeCompatibleWith(other: Slot): Boolean
  def isLongSlot: Boolean
  def asNullable: Slot
}

case class LongSlot(offset: Int, nullable: Boolean, typ: CypherType) extends Slot {
  override def isTypeCompatibleWith(other: Slot): Boolean = other match {
    case LongSlot(_, _, otherTyp) =>
      typ.isAssignableFrom(otherTyp) || otherTyp.isAssignableFrom(typ)
    case _ => false
  }

  override def isLongSlot: Boolean = true

  override def asNullable = LongSlot(offset, true, typ)
}

case class RefSlot(offset: Int, nullable: Boolean, typ: CypherType) extends Slot {
  override def isTypeCompatibleWith(other: Slot): Boolean = other match {
    case RefSlot(_, _, otherTyp) =>
      typ.isAssignableFrom(otherTyp) || otherTyp.isAssignableFrom(typ)
    case _ => false
  }

  override def isLongSlot: Boolean = false

  override def asNullable = RefSlot(offset, true, typ)
}

sealed trait SlotWithAliases {
  def slot: Slot
  def aliases: Set[String]

  protected def makeString: String = {
    val aliasesString = s"${aliases.mkString("'", "','", "'")}"
    f"$slot%-30s $aliasesString%-10s"
  }
}

case class LongSlotWithAliases(slot: LongSlot, aliases: Set[String]) extends SlotWithAliases {
  override def toString: String = makeString
}

case class RefSlotWithAliases(slot: RefSlot, aliases: Set[String]) extends SlotWithAliases {
  override def toString: String = makeString
}
