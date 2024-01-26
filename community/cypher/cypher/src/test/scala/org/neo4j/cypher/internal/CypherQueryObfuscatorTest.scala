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

import org.neo4j.cypher.internal.util.LiteralOffset
import org.neo4j.cypher.internal.util.ObfuscationMetadata
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.virtual.MapValue

import scala.jdk.CollectionConverters.MapHasAsJava

class CypherQueryObfuscatorTest extends CypherFunSuite {

  test("empty obfuscator should not change query text") {
    val originalText = "not passwords here"
    val ob =
      CypherQueryObfuscator(
        ObfuscationMetadata(
          Vector.empty,
          Set.empty
        )
      )

    ob.obfuscateText(originalText) should equal(originalText)
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

    ob.obfuscateText(originalText) should equal(expectedText)
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

    ob.obfuscateText(originalText) should equal(expectedText)
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

    ob.obfuscateText(originalText) should equal(expectedText)
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

    ob.obfuscateText(originalText) should equal(expectedText)
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

    ob.obfuscateText(originalText) should equal(expectedText)
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

    ob.obfuscateText(originalText) should equal(expectedText)
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

    ob.obfuscateText(originalText) should equal(expectedText)
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

    an[IllegalStateException] should be thrownBy ob.obfuscateText(originalText)
  }

  test("should throw when index is out of bounds") {
    val originalText = "password is 'here'"
    val ob =
      CypherQueryObfuscator(
        ObfuscationMetadata(
          Vector(offsetOf(originalText, "'here'"), LiteralOffset(999, Some(10))),
          Set.empty
        )
      )

    an[IllegalStateException] should be thrownBy ob.obfuscateText(originalText)
  }

  private def makeParams(params: (String, String)*): MapValue = {
    ValueUtils.asMapValue(Map(params: _*).asJava)
  }

  private def offsetOf(originalText: String, word: String): LiteralOffset = {
    LiteralOffset(originalText.indexOf(word), None)
  }

}
