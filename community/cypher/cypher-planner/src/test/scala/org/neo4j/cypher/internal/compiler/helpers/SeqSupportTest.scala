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
package org.neo4j.cypher.internal.compiler.helpers

import org.neo4j.cypher.internal.compiler.helpers.SeqSupport.RichSeq
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.CypherScalaCheckDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers

class SeqSupportTest extends CypherFunSuite with Matchers with CypherScalaCheckDrivenPropertyChecks {

  test("prefix strings with their offset using foldMap") {
    val strings = Seq("foo", "", "a", "bar", "", "b", "a")
    val result = strings.foldMap(0) {
      case (offset, string) => (offset + string.length, s"$offset:$string")
    }
    result shouldEqual (9, List("0:foo", "3:", "3:a", "4:bar", "7:", "7:b", "8:a"))
  }

  test("prefix strings with their indices using foldMap") {
    forAll { (is: List[String]) =>
      val result = is.foldMap(0) {
        case (index, string) => (index + 1, s"$index.$string")
      }
      val expected = is.zipWithIndex.map {
        case (string, index) => s"$index.$string"
      }
      result shouldEqual (is.length, expected)
    }
  }

  test("foldMap using the identity function returns the initial accumulator and the original sequence") {
    forAll { (is: List[Int], c: Char) =>
      is.foldMap(c)((x, y) => (x, y)) shouldEqual ((c, is))
    }
  }

  test("group strings by length preserving order") {
    val strings = Seq("foo", "", "a", "bar", "", "b", "a")
    val groups = strings.sequentiallyGroupBy(_.length)
    val expected =
      Seq(
        3 -> Seq("foo", "bar"),
        0 -> Seq("", ""),
        1 -> Seq("a", "b", "a")
      )

    groups shouldEqual expected
  }
}
