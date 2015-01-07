/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0

class InputPosition(val offset: Int, val line: Int, val column: Int) {
  override def hashCode = 41 * offset
  override def equals(that: Any): Boolean = that match {
    case that: InputPosition =>
      (that canEqual this) && offset == that.offset
    case _ =>
      false
  }
  def canEqual(that: Any): Boolean = that.isInstanceOf[InputPosition]

  override def toString = "line " + line + ", column " + column
}

object InputPosition {
  implicit object InputPositionOrdering extends Ordering[InputPosition] {
    def compare(p1: InputPosition, p2: InputPosition) =
      p1.offset.compare(p2.offset)
  }
}
