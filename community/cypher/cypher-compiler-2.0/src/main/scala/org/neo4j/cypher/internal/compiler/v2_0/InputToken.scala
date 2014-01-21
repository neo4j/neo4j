/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

class InputToken(val startPosition: InputPosition, val endPosition: InputPosition) extends Equals {
  override def hashCode = 41 * startPosition.hashCode * endPosition.hashCode
  override def equals(that: Any): Boolean = that match {
    case that: InputToken =>
      (that canEqual this) && startPosition == that.startPosition && endPosition == that.endPosition
    case _ =>
      false
  }
  def canEqual(that: Any): Boolean = that.isInstanceOf[InputToken]

  def startOnly: InputToken = new InputToken(startPosition, startPosition)
  def endOnly: InputToken = new InputToken(endPosition, endPosition)

  override def toString: String = s"InputToken{start=${startPosition.offset},end=${endPosition.offset}}"
}

object InputToken {
  implicit object InputTokenOrdering extends Ordering[InputToken] {
    def compare(t1: InputToken, t2: InputToken) = {
      val comp = t1.startPosition.offset.compare(t2.startPosition.offset)
      if (comp != 0)
        comp
      else
        t1.endPosition.offset.compare(t2.endPosition.offset)
    }
  }
}
