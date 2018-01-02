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
package org.neo4j.cypher.internal.frontend.v2_3.helpers

import org.apache.commons.lang3.SystemUtils

object StringHelper {
  private val positionPattern = """\(line (\d+), column (\d+) \(offset: (\d+)\)\)""".r

  implicit class RichString(val text: String) extends AnyVal {
    def stripLinesAndMargins: String =
      text.stripMargin.filter((ch: Char) => Character.isDefined(ch) && !Character.isISOControl(ch))

    def cypherEscape: String =
      text.replace("\\", "\\\\")

    // (line 1, column 8 (offset: 7))
    def fixPosition: String = if (SystemUtils.IS_OS_WINDOWS) {
      positionPattern.replaceAllIn(text, (matcher) => {
        val line = matcher.group(1).toInt
        val column = matcher.group(2).toInt
        val offset = matcher.group(3).toInt + line - 1
        s"(line $line, column $column (offset: $offset))"
      } )
    } else { text }
  }
}
