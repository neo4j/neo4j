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

import org.neo4j.cypher.internal.util.v3_4.symbols.CypherType

sealed trait Slot {
  def offset: Int
  def nullable: Boolean
  def typ: CypherType
}

case class LongSlot(offset: Int, nullable: Boolean, typ: CypherType) extends Slot
case class RefSlot(offset: Int, nullable: Boolean, typ: CypherType) extends Slot

sealed trait SlotWithAliases {
  def slot: Slot
  def aliases: Set[String]

  protected def makeString: String = {
    val aliasesString = s"${aliases.mkString("'", "','", "'")}"
    f"${slot}%-30s ${aliasesString}%-10s"
  }
}

case class LongSlotWithAliases(slot: LongSlot, aliases: Set[String]) extends SlotWithAliases {
  override def toString: String = makeString
}

case class RefSlotWithAliases(slot: RefSlot, aliases: Set[String]) extends SlotWithAliases {
  override def toString: String = makeString
}
