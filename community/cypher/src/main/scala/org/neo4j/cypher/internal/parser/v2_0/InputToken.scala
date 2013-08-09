/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.parser.v2_0

abstract class InputPosition {
  val offset : Int
  val line : Int
  val column : Int

  override def toString = "line " + line + ", column " + column
}

abstract class InputToken {
  def startPosition : InputPosition
  def endPosition : InputPosition
  def toString : String

  def startOnly : InputToken = SinglePositionToken(startPosition)
  def endOnly : InputToken = SinglePositionToken(endPosition)
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

case class SinglePositionToken(position: InputPosition) extends InputToken {
  val startPosition = position
  val endPosition = position
  override val toString = ""
}
