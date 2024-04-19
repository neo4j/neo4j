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

class ContainMultiLineStringMatcherTest extends CypherFunSuite with ContainMultiLineStringMatcher {

  test("should find exact match - 1 line") {
    containMultiLineString("foo")("foo").matches should be(true)
  }

  test("should fail for no match - 1 line") {
    containMultiLineString("foo")("bar").matches should be(false)
  }

  test("should find exact match - multiple lines") {
    val str =
      """foo
        |bar
        |baz""".stripMargin
    containMultiLineString(str)(str).matches should be(true)
  }

  test("should fail for no match - multiple lines") {
    val expected =
      """foo
        |boo
        |baz""".stripMargin
    val actual =
      """123foo
        |   bar123
        |123baz123""".stripMargin
    containMultiLineString(expected)(actual).matches should be(false)
  }

  test("should fail for matches out of order - multiple lines") {
    val expected =
      """foo
        |bar
        |baz""".stripMargin
    val actual =
      """123bar
        |   foo123
        |123baz123""".stripMargin
    containMultiLineString(expected)(actual).matches should be(false)
  }

  test("should fail for matches out of order - multiple lines - first match in order") {
    val expected =
      """foo
        |bar
        |baz""".stripMargin
    val actual =
      """123foo
        |   baz123
        |123bar123""".stripMargin
    containMultiLineString(expected)(actual).matches should be(false)
  }

  test("should find match with leading/trailing strings - multiple lines") {
    val expected =
      """foo
        |bar
        |baz""".stripMargin
    val actual =
      """123foo
        |   bar123
        |123baz123""".stripMargin
    containMultiLineString(expected)(actual).matches should be(true)
  }

  test("should find match with leading/trailing lines") {
    val expected =
      """foo
        |bar
        |baz""".stripMargin
    val actual =
      """123456789
        |123foo
        |   bar123
        |123baz123
        |123456789""".stripMargin
    containMultiLineString(expected)(actual).matches should be(true)
  }

  // Otherwise the implementation would get out-of-hand
  test("should find match with non-matching lines in-between") {
    val expected =
      """foo
        |bar
        |baz""".stripMargin
    val actual =
      """123456789
        |123foo
        |---------
        |   bar123
        |---------
        |123baz123
        |123456789""".stripMargin
    containMultiLineString(expected)(actual).matches should be(true)
  }

  // Otherwise the implementation would get out-of-hand
  test("should find misaligned match") {
    val expected =
      """foo
        |bar
        |baz""".stripMargin
    val actual =
      """123foo
        |bar123
        |123baz""".stripMargin
    containMultiLineString(expected)(actual).matches should be(true)
  }

  test("should not find matches if they are not on new lines") {
    val expected =
      """foo
        |bar
        |baz""".stripMargin
    val actual =
      """foo bar baz""".stripMargin
    containMultiLineString(expected)(actual).matches should be(false)
  }

  test("should not find the same match multiple times") {
    val expected =
      """foo
        |foo
        |foo""".stripMargin
    val actual =
      """foo""".stripMargin
    containMultiLineString(expected)(actual).matches should be(false)
  }

  test("should match each cell in a grid") {
    val expected = (1 to 9).map { i =>
      s"""+---+
         || $i |
         |+---+""".stripMargin
    }
    val unexpected = ('a' to 'z').map { c =>
      s"""+---+
         || $c|
         |+---+""".stripMargin
    }
    val actual =
      """+---+---+---+
        || 1 | 2 | 3 |
        |+---+---+---+
        || 4 | 5 | 6 |
        |+---+---+---+
        || 7 | 8 | 9 |
        |+---+---+---+
        |""".stripMargin

    expected.foreach(containMultiLineString(_)(actual).matches should be(true))
    unexpected.foreach(containMultiLineString(_)(actual).matches should be(false))
  }
}
