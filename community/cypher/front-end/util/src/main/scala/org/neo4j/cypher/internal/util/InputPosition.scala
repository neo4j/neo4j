/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.util

/** The position of an AST node. */
sealed trait InputPosition {

  /** The offset in characters (not codepoints!) from the beginning of the query string */
  def offset: Int

  /** The line in the query string */
  def line: Int

  /** The column in the query string */
  def column: Int

  /**
   * Offset this position by a number of characters and return the new position.
   */
  def withOffset(pos: Option[InputPosition]): InputPosition = pos match {
    case Some(p) if p.offset != 0 =>
      val newColumn = if (line == p.line) column + p.column - 1 else column
      InputPosition(offset + p.offset, line + p.line - 1, newColumn)
    case _ => this
  }

  def withInputLength(length: Int): InputPosition.Range = {
    InputPosition.withLength(offset, line, column, length)
  }

  override def toString = s"line $line, column $column (offset: $offset)"
}

object InputPosition {
  case class Simple(offset: Int, line: Int, column: Int) extends InputPosition
  case class Range(offset: Int, line: Int, column: Int, inputLength: Int) extends InputPosition

  implicit val byOffset: Ordering[InputPosition] = Ordering.by(_.offset)

  val NONE: InputPosition = Simple(0, 0, 0)

  def apply(offset: Int, line: Int, column: Int): InputPosition = Simple(offset, line, column)

  def withLength(offset: Int, line: Int, column: Int, length: Int): Range = Range(offset, line, column, length)
}
