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
package org.neo4j.cypher.internal.frontend.v2_3.perty.format

import org.neo4j.cypher.internal.frontend.v2_3.perty._
import org.neo4j.cypher.internal.frontend.v2_3.perty.print.{PrintCommand, PrintNewLine, PrintText}

import scala.collection.mutable

class StringPrintingConverter(var builder: mutable.StringBuilder = new mutable.StringBuilder()) extends PrintingConverter[String] {
  def clear() {
    builder.clear()
  }

  def result() = builder.result()

  def +=(elem: PrintCommand) = {
    elem match {
      case PrintText(text) =>
        builder = builder ++= text

      case PrintNewLine(indent) =>
        builder.append(System.lineSeparator())
        var remaining = indent
        while (remaining > 0) {
          builder = builder += ' '
          remaining  -= 1
        }
    }
    this
  }
}
