/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_4

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

  val NONE = InputPosition(0, 0, 0)
}
