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
package org.neo4j.internal.kernel.api.helpers.traversal

sealed trait SlotOrName {

  override def toString: String = this match {
    case SlotOrName.VarName(name, _)       => name
    case SlotOrName.Slotted(slotOffset, _) => slotOffset.toString
    case SlotOrName.None                   => ""
  }
}

object SlotOrName {
  case class VarName(name: String, isGroup: Boolean) extends SlotOrName
  case class Slotted(slotOffset: Int, isGroup: Boolean) extends SlotOrName
  case object None extends SlotOrName

  val none: SlotOrName = None
}
