/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.frontend.v3_4.helpers

import org.apache.commons.lang3.SystemUtils

object StringHelper {
  private val positionPattern = """\(line (\d+), column (\d+) \(offset: (\d+)\)\)""".r

  implicit class RichString(val text: String) extends AnyVal {
    def stripLinesAndMargins: String =
      text.stripMargin.filter((ch: Char) => Character.isDefined(ch) && !Character.isISOControl(ch))

    def cypherEscape: String =
      text.replace("\\", "\\\\")

    def fixNewLines: String = text.replaceAll("\r\n", "\n")

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
