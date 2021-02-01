/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.util.UUID

/**
 * The position of an AST node.
 *
 * @param offset the offset in characters from the beginning of the query string
 * @param line   the line in the query string
 * @param column the column in the query string
 * @param uuid   a unique identifier to disambiguate positions of generated AST nodes that do not have a real position in the query string
 */
case class InputPosition(offset: Int,
                         line: Int,
                         column: Int,
                         private val uuid: Option[UUID] = None) {

  /**
   * This method is mostly defined to make the otherwise generated copy method inaccessible.
   * We want to prevent copying input positions without changing the uuid.
   */
  private[util] def copy(offset: Int = this.offset,
                         line: Int = this.line,
                         column: Int = this.column): InputPosition = InputPosition(offset, line, column, Some(UUID.randomUUID()))

  override def toString = s"line $line, column $column (offset: $toUniqueOffsetString)"

  /**
   * @return a string that consists of the offset and the uuid, thus uniquely identifying an InputPosition.
   */
  def toUniqueOffsetString: String = uuid match {
    case Some(uuidVal) => s"$offset($uuidVal)"
    case None => offset.toString
  }

  /**
   * Offset this position by a number of characters and return the new position.
   */
  def withOffset(pos: Option[InputPosition]): InputPosition = pos match {
    case Some(p) =>
      val newColumn = if (line == p.line) column + p.column - 1 else column
      InputPosition(offset + p.offset, line + p.line - 1, newColumn)
    case None => this
  }

  /**
   * @return a new unique InputPosition with the same offset, line and column.
   */
  def newUniquePos(): InputPosition = InputPosition(offset, line, column, Some(UUID.randomUUID()))
}

object InputPosition {
  implicit val byOffset: Ordering[InputPosition] =
    Ordering.by { pos: InputPosition =>
      pos.offset
    }

  val NONE: InputPosition = InputPosition(0, 0, 0)
}
