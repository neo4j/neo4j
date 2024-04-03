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
package org.neo4j.util

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

import scala.language.implicitConversions

trait AnyValueConversions {
  implicit def fromInt(int: Int): AnyValue = Values.intValue(int)
  implicit def fromLong(long: Long): AnyValue = Values.longValue(long)
  implicit def fromString(str: String): AnyValue = Values.stringValue(str)
}

case class Table(header: Seq[String], rows: Seq[Seq[AnyValue]]) {

  def row(row: AnyValue*): Table = {
    assert(row.length == header.length, "Table row must have same length as header")
    copy(rows = rows :+ row)
  }

  def asRows: Seq[CypherRow] =
    rows.map(header zip _)
      .map(x => CypherRow.from(x: _*))

  override def toString: String = {
    val widths = (header +: rows).transpose.map(_.map(_.toString.length).max)
    val sb = new scala.collection.mutable.StringBuilder("Table: \n")
    def appendLine(line: Seq[Object]) = {
      sb.append(line.zip(widths).map { case (h, w) =>
        h.toString.padTo(w, ' ')
      }.mkString(" | "))
        .append('\n')
    }

    appendLine(header)
    rows.foreach(appendLine)

    sb.toString()
  }
}

object Table {
  def hdr(header: String*): Table = Table(header, Seq.empty)
}
