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
package org.neo4j.cypher.internal

import org.neo4j.cypher.CommunityCypherTestSuite
import org.neo4j.cypher.internal.util.LiteralOffset
import org.neo4j.cypher.internal.util.ObfuscationMetadata
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue

import scala.jdk.CollectionConverters.MapHasAsJava

class CypherQueryObfuscatorTest extends CommunityCypherTestSuite {

  test("empty obfuscator should not change query text") {
    val originalText = "not passwords here"
    val ob =
      CypherQueryObfuscator(
        ObfuscationMetadata(
          Vector.empty,
          Set.empty
        )
      )

    ob.obfuscateText(originalText, 0) should equal(originalText)
  }

  test("should obfuscate simple password") {
    val originalText = "password is 'here' // comment"
    val expectedText = "password is ****** // comment"
    val ob =
      CypherQueryObfuscator(
        ObfuscationMetadata(
          Vector(offsetOf(originalText, "'here'")),
          Set.empty
        )
      )

    ob.obfuscateText(originalText, 0) should equal(expectedText)
  }

  test("should obfuscate multiline password") {
    val originalText = "password is 'here is a\nmultiline\npassword' // comment"
    val expectedText = "password is ****** // comment"
    val ob =
      CypherQueryObfuscator(
        ObfuscationMetadata(
          Vector(offsetOf(originalText, "'here is a\nmultiline\npassword'")),
          Set.empty
        )
      )

    ob.obfuscateText(originalText, 0) should equal(expectedText)
  }

  test("should obfuscate password with nested quotes") {
    val originalText = "password is 'here is a \"password\"' // comment"
    val expectedText = "password is ****** // comment"
    val ob =
      CypherQueryObfuscator(
        ObfuscationMetadata(
          Vector(offsetOf(originalText, "'here is a \"password\"'")),
          Set.empty
        )
      )

    ob.obfuscateText(originalText, 0) should equal(expectedText)
  }

  test("should obfuscate password with escaped quotes") {
    val originalText = "password is 'here is a \\'password\\'' // comment"
    val expectedText = "password is ****** // comment"
    val ob =
      CypherQueryObfuscator(
        ObfuscationMetadata(
          Vector(offsetOf(originalText, "'here is a \\'password\\''")),
          Set.empty
        )
      )

    ob.obfuscateText(originalText, 0) should equal(expectedText)
  }

  test("should obfuscate multiple passwords") {
    val originalText = "password is 'here' and 'also here' // comment"
    val expectedText = "password is ****** and ****** // comment"
    val ob =
      CypherQueryObfuscator(
        ObfuscationMetadata(
          Vector(offsetOf(originalText, "'here'"), offsetOf(originalText, "'also here'")),
          Set.empty
        )
      )

    ob.obfuscateText(originalText, 0) should equal(expectedText)
  }

  test("should obfuscate multiple passwords next to each other") {
    val originalText = "password is 'here''and also here' // comment"
    val expectedText = "password is ************ // comment"
    val ob =
      CypherQueryObfuscator(
        ObfuscationMetadata(
          Vector(offsetOf(originalText, "'here'"), offsetOf(originalText, "'and also here'")),
          Set.empty
        )
      )

    ob.obfuscateText(originalText, 0) should equal(expectedText)
  }

  test("empty obfuscator should not change query parameters") {
    val originalParams = makeParams("a" -> "b", "c" -> "d")
    val ob =
      CypherQueryObfuscator(
        ObfuscationMetadata(
          Vector.empty,
          Set.empty
        )
      )

    ob.obfuscateParameters(originalParams) should equal(originalParams)
  }

  test("should obfuscated sensitive parameters") {
    val originalParams = makeParams("a" -> "b", "c" -> "d", "e" -> "f")
    val expectedParams = makeParams("a" -> "******", "c" -> "d", "e" -> "******")
    val ob =
      CypherQueryObfuscator(
        ObfuscationMetadata(
          Vector.empty,
          Set("a", "e")
        )
      )

    ob.obfuscateParameters(originalParams) should equal(expectedParams)
  }

  test("should obfuscate everything if missing an end quote") {
    val originalText = "password is here'"
    val expectedText = "password is ******"
    val ob =
      CypherQueryObfuscator(
        ObfuscationMetadata(
          Vector(offsetOf(originalText, "here")),
          Set.empty
        )
      )

    ob.obfuscateText(originalText, 0) should equal(expectedText)
  }

  test("should throw when missing closing quote") {
    val originalText = "password is 'here"
    val ob =
      CypherQueryObfuscator(
        ObfuscationMetadata(
          Vector(offsetOf(originalText, "'here")),
          Set.empty
        )
      )

    an[IllegalStateException] should be thrownBy ob.obfuscateText(originalText, 0)
  }

  test("should throw when index is out of bounds") {
    val originalText = "password is 'here'"
    val ob =
      CypherQueryObfuscator(
        ObfuscationMetadata(
          Vector(offsetOf(originalText, "'here'"), LiteralOffset(999, 0, Some(10))),
          Set.empty
        )
      )

    an[IllegalStateException] should be thrownBy ob.obfuscateText(originalText, 0)
  }

  test("should obfuscate different Cypher literal types in text") {
    val originalText = "CREATE (n {s: 'str\u0060', i: 42, b: true, f: 4.42, v: vector([2, 2, 2], 3, INT), z: null})"
    val expectedText =
      "CREATE (n {s: ******, i: ******, b: ******, f: ******, v: vector(******, ******, INT), z: ******})"

    val offsets = Vector(
      LiteralOffset(originalText.indexOf("'str\u0060'"), 0, Some(6)), // string literal including quotes
      LiteralOffset(originalText.indexOf("42"), 0, Some(2)), // integer literal
      LiteralOffset(originalText.indexOf("true"), 0, Some(4)), // boolean literal
      LiteralOffset(originalText.indexOf("4.42"), 0, Some(4)), // float literal
      LiteralOffset(originalText.indexOf("[2, 2, 2]"), 0, Some(9)), // list literal
      LiteralOffset(originalText.indexOf("3"), 0, Some(1)), // list literal
      LiteralOffset(originalText.indexOf("null"), 0, Some(4)) // null literal
    )

    val ob = CypherQueryObfuscator(ObfuscationMetadata(offsets, Set.empty))

    ob.obfuscateText(originalText, 0) should equal(expectedText)
  }

  test("should obfuscate sensitive parameters of different types") {
    val originalParams = makeAnyParams(
      "s" -> "str",
      "i" -> Int.box(42),
      "b" -> Boolean.box(true),
      "f" -> Double.box(3.14d),
      "v" -> Values.int32Vector(1, 2, 3, 4),
      "x" -> "kept"
    )

    val expectedParams = makeAnyParams(
      "s" -> "******",
      "i" -> "******",
      "b" -> "******",
      "f" -> "******",
      "v" -> "******",
      "x" -> "kept"
    )

    val ob = CypherQueryObfuscator(ObfuscationMetadata(Vector.empty, Set("s", "i", "b", "f", "v")))

    ob.obfuscateParameters(originalParams) should equal(expectedParams)
  }

  private def makeParams(params: (String, String)*): MapValue = {
    ValueUtils.asMapValue(Map(params: _*).asJava)
  }

  private def offsetOf(originalText: String, word: String): LiteralOffset = {
    LiteralOffset(originalText.indexOf(word), 0, None)
  }

  private def makeAnyParams(params: (String, AnyRef)*): MapValue = {
    ValueUtils.asMapValue(Map(params: _*).asJava)
  }

}
