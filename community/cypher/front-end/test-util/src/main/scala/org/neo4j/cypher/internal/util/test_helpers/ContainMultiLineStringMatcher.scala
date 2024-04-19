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
package org.neo4j.cypher.internal.util.test_helpers

import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher
import org.scalatest.matchers.dsl.IncludeWord

/**
 * To enable importing the matcher directly.
 */
object ContainMultiLineStringMatcher extends ContainMultiLineStringMatcher

/**
 * Enables testing that a multi-line String appears within another String:
 * {{{
 *    """+---+---+---+
 *       || 1 | 2 | 3 |
 *       |+---+---+---+
 *       || 4 | 5 | 6 |
 *       |+---+---+---+
 *       || 7 | 8 | 9 |
 *       |+---+---+---+
 *       |""".stripMargin should containMultiLineString(
 *       """+---+
 *         || 5 |
 *         |+---+""".stripMargin
 *       )
 * }}}
 *
 * Can have some false positives, since it does not test for vertical alignment.
 * It also allows lines in-between the matching lines.
 */
trait ContainMultiLineStringMatcher {
  private val NEWLINE = System.lineSeparator()

  private class ContainMultiLineString(expectedString: String) extends Matcher[String] {

    override def apply(actual: String): MatchResult = {
      val expectedLinesIterator = expectedString.linesIterator
      val matchers = LazyList.unfold(actual) { remainingActual =>
        expectedLinesIterator.nextOption().map { expectedLine =>
          val matcher = new IncludeWord()(expectedLine)(remainingActual)

          val nextRemainingActual = remainingActual.indexOf(expectedLine) match {
            case -1 => "" // The match failed.
            case i =>
              val remainingAfterMatch = remainingActual.substring(i + expectedLine.length)
              remainingAfterMatch.indexOf(NEWLINE) match {
                case -1 => "" // there was no newline after this matching line
                case i  => remainingAfterMatch.substring(i + NEWLINE.length)
              }
          }
          (matcher, nextRemainingActual)
        }
      }
      matchers.find(!_.matches).getOrElse(matchers.head)
    }
  }

  def containMultiLineString(expectedString: String): Matcher[String] = new ContainMultiLineString(expectedString)
}
