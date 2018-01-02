/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.frontend.v2_3

case class InputPosition(offset: Int, line: Int, column: Int) {
  self =>

  override def hashCode = 41 * offset

  override def equals(that: Any): Boolean = that match {
    case that: InputPosition =>
      (that canEqual this) && offset == that.offset
    case _ =>
      false
  }

  def canEqual(that: Any): Boolean = that.isInstanceOf[InputPosition]

  override def toString = s"line $line, column $column (offset: $toOffsetString)"

  def toOffsetString = offset.toString

  def withOffset(pos: Option[InputPosition]) = pos match {
    case Some(p) =>
      val newColumn = if (line == p.line) column + p.column - 1 else column
      copy(offset + p.offset, line + p.line - 1, newColumn)
    case None => self
  }

  def bumped() = copy(offset = offset + 1) // HACKISH
}

object InputPosition {
  implicit val byOffset =
    Ordering.by { (pos: InputPosition) => pos.offset }

  val NONE = null
}
