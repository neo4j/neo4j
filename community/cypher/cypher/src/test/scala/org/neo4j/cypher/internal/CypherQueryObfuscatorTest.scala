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
import org.neo4j.kernel.api.query.QueryObfuscator
import org.neo4j.kernel.api.query.QueryObfuscator.ObfuscatedQuery
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue

import scala.jdk.CollectionConverters.MapHasAsJava

class CypherQueryObfuscatorTest extends CommunityCypherTestSuite {

  test("empty obfuscator should not change query text") {
    val originalText = "not passwords here"
    val ob =
      CypherQueryObfuscator(
        secretMeta(
          Vector.empty,
          Set.empty
        ),
        obfuscateLiterals = false,
        exposeFullView = true
      )

    ob.obfuscateText(originalText, 0) should equal(originalText)
  }

  test("should obfuscate simple password") {
    val originalText = "password is 'here' // comment"
    val expectedText = "password is ****** // comment"
    val ob =
      CypherQueryObfuscator(
        secretMeta(
          Vector(offsetOf(originalText, "'here'")),
          Set.empty
        ),
        obfuscateLiterals = false,
        exposeFullView = true
      )

    ob.obfuscateText(originalText, 0) should equal(expectedText)
  }

  test("should obfuscate multiline password") {
    val originalText = "password is 'here is a\nmultiline\npassword' // comment"
    val expectedText = "password is ****** // comment"
    val ob =
      CypherQueryObfuscator(
        secretMeta(
          Vector(offsetOf(originalText, "'here is a\nmultiline\npassword'")),
          Set.empty
        ),
        obfuscateLiterals = false,
        exposeFullView = true
      )

    ob.obfuscateText(originalText, 0) should equal(expectedText)
  }

  test("should obfuscate password with nested quotes") {
    val originalText = "password is 'here is a \"password\"' // comment"
    val expectedText = "password is ****** // comment"
    val ob =
      CypherQueryObfuscator(
        secretMeta(
          Vector(offsetOf(originalText, "'here is a \"password\"'")),
          Set.empty
        ),
        obfuscateLiterals = false,
        exposeFullView = true
      )

    ob.obfuscateText(originalText, 0) should equal(expectedText)
  }

  test("should obfuscate password with escaped quotes") {
    val originalText = "password is 'here is a \\'password\\'' // comment"
    val expectedText = "password is ****** // comment"
    val ob =
      CypherQueryObfuscator(
        secretMeta(
          Vector(offsetOf(originalText, "'here is a \\'password\\''")),
          Set.empty
        ),
        obfuscateLiterals = false,
        exposeFullView = true
      )

    ob.obfuscateText(originalText, 0) should equal(expectedText)
  }

  test("should obfuscate multiple passwords") {
    val originalText = "password is 'here' and 'also here' // comment"
    val expectedText = "password is ****** and ****** // comment"
    val ob =
      CypherQueryObfuscator(
        secretMeta(
          Vector(offsetOf(originalText, "'here'"), offsetOf(originalText, "'also here'")),
          Set.empty
        ),
        obfuscateLiterals = false,
        exposeFullView = true
      )

    ob.obfuscateText(originalText, 0) should equal(expectedText)
  }

  test("should obfuscate multiple passwords next to each other") {
    val originalText = "password is 'here''and also here' // comment"
    val expectedText = "password is ************ // comment"
    val ob =
      CypherQueryObfuscator(
        secretMeta(
          Vector(offsetOf(originalText, "'here'"), offsetOf(originalText, "'and also here'")),
          Set.empty
        ),
        obfuscateLiterals = false,
        exposeFullView = true
      )

    ob.obfuscateText(originalText, 0) should equal(expectedText)
  }

  test("empty obfuscator should not change query parameters") {
    val originalParams = makeParams("a" -> "b", "c" -> "d")
    val ob =
      CypherQueryObfuscator(
        secretMeta(
          Vector.empty,
          Set.empty
        ),
        obfuscateLiterals = false,
        exposeFullView = true
      )

    ob.obfuscateParameters(originalParams) should equal(originalParams)
  }

  test("should obfuscated sensitive parameters") {
    val originalParams = makeParams("a" -> "b", "c" -> "d", "e" -> "f")
    val expectedParams = makeParams("a" -> "******", "c" -> "d", "e" -> "******")
    val ob =
      CypherQueryObfuscator(
        secretMeta(
          Vector.empty,
          Set("a", "e")
        ),
        obfuscateLiterals = false,
        exposeFullView = true
      )

    ob.obfuscateParameters(originalParams) should equal(expectedParams)
  }

  test("should obfuscate everything if missing an end quote") {
    val originalText = "password is here'"
    val expectedText = "password is ******"
    val ob =
      CypherQueryObfuscator(
        secretMeta(
          Vector(offsetOf(originalText, "here")),
          Set.empty
        ),
        obfuscateLiterals = false,
        exposeFullView = true
      )

    ob.obfuscateText(originalText, 0) should equal(expectedText)
  }

  test("should throw when missing closing quote") {
    val originalText = "password is 'here"
    val ob =
      CypherQueryObfuscator(
        secretMeta(
          Vector(offsetOf(originalText, "'here")),
          Set.empty
        ),
        obfuscateLiterals = false,
        exposeFullView = true
      )

    an[IllegalStateException] should be thrownBy ob.obfuscateText(originalText, 0)
  }

  test("should throw when index is out of bounds") {
    val originalText = "password is 'here'"
    val ob =
      CypherQueryObfuscator(
        secretMeta(
          Vector(offsetOf(originalText, "'here'"), LiteralOffset(999, 0, Some(10))),
          Set.empty
        ),
        obfuscateLiterals = false,
        exposeFullView = true
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

    val ob = CypherQueryObfuscator(
      secretMeta(offsets, Set.empty),
      obfuscateLiterals = false,
      exposeFullView = true
    )

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

    val ob = CypherQueryObfuscator(
      secretMeta(Vector.empty, Set("s", "i", "b", "f", "v")),
      obfuscateLiterals = false,
      exposeFullView = true
    )

    ob.obfuscateParameters(originalParams) should equal(expectedParams)
  }

  test("both views redact sensitive literals and parameters; only the all view redacts ordinary literals") {
    val text = "RETURN 'secret', 42"
    val params = makeParams("p" -> "secret", "q" -> "kept")
    val expectedParams = makeParams("p" -> "******", "q" -> "kept")
    val secretOffset = LiteralOffset(text.indexOf("'secret'"), 0, Some(8))
    val intOffset = LiteralOffset(text.indexOf("42"), 0, Some(2))
    val ob = CypherQueryObfuscator(
      ObfuscationMetadata(Vector(secretOffset), Vector(secretOffset, intOffset), Set("p")),
      obfuscateLiterals = false,
      exposeFullView = true
    )

    ob.sensitiveObfuscatedQuery(text, params, 0).text should equal("RETURN ******, 42")
    ob.fullyObfuscatedQuery(text, params, 0).text should equal("RETURN ******, ******")
    ob.sensitiveObfuscatedQuery(text, params, 0).parameters should equal(expectedParams)
    ob.fullyObfuscatedQuery(text, params, 0).parameters should equal(expectedParams)
  }

  test("defaultObfuscatedQuery redacts only sensitive literals when obfuscate_literals is false") {
    val text = "RETURN 'secret', 42"
    val secretOffset = LiteralOffset(text.indexOf("'secret'"), 0, Some(8))
    val intOffset = LiteralOffset(text.indexOf("42"), 0, Some(2))
    val ob = CypherQueryObfuscator(
      ObfuscationMetadata(Vector(secretOffset), Vector(secretOffset, intOffset), Set.empty),
      obfuscateLiterals = false,
      exposeFullView = true
    )

    ob.defaultObfuscatedQuery(text, MapValue.EMPTY, 0).text should equal("RETURN ******, 42")
  }

  test("with the expose-full-view fail-safe off, only the all-literals view is absent") {
    val text = "RETURN 'secret', 42"
    val secretOffset = LiteralOffset(text.indexOf("'secret'"), 0, Some(8))
    val intOffset = LiteralOffset(text.indexOf("42"), 0, Some(2))
    val ob = CypherQueryObfuscator(
      ObfuscationMetadata(Vector(secretOffset), Vector(secretOffset, intOffset), Set.empty),
      obfuscateLiterals = false,
      exposeFullView = false
    )

    ObfuscatedQuery.optional(ob.fullyObfuscatedQuery(text, MapValue.EMPTY, 0)).isPresent() shouldBe false
    // The sensitive and default views are unaffected by the fail-safe.
    ob.sensitiveObfuscatedQuery(text, MapValue.EMPTY, 0).text should equal("RETURN ******, 42")
    ob.defaultObfuscatedQuery(text, MapValue.EMPTY, 0).text should equal("RETURN ******, 42")
  }

  test("obfuscate_literals=true re-enables the all-literals view even when the fail-safe is off") {
    val text = "RETURN 'secret', 42"
    val secretOffset = LiteralOffset(text.indexOf("'secret'"), 0, Some(8))
    val intOffset = LiteralOffset(text.indexOf("42"), 0, Some(2))
    val ob = CypherQueryObfuscator(
      ObfuscationMetadata(Vector(secretOffset), Vector(secretOffset, intOffset), Set.empty),
      obfuscateLiterals = true,
      exposeFullView = false
    )

    ob.fullyObfuscatedQuery(text, MapValue.EMPTY, 0).text should equal("RETURN ******, ******")
    ob.defaultObfuscatedQuery(text, MapValue.EMPTY, 0).text should equal("RETURN ******, ******")
  }

  test("with the fail-safe off, a query with no literals exposes no all-literals view") {
    // Empty metadata used to short-circuit to PASSTHROUGH, whose fullyObfuscatedQuery returns raw text.
    // With the fail-safe off that raw text must not be exposed as the all-literals view.
    val ob = CypherQueryObfuscator(
      ObfuscationMetadata.empty(),
      obfuscateLiterals = false,
      exposeFullView = false
    )

    val view = ob.fullyObfuscatedQuery("MATCH (n) RETURN n", MapValue.EMPTY, 0)
    ObfuscatedQuery.optional(view).isPresent() shouldBe false
  }

  test("uncollected metadata (None) never exposes a full-literals view, whatever the policy") {
    // Metadata was never collected: raw text must not be served as the all-literals view, so the full view
    // is absent by construction even with the most permissive policy.
    val ob = CypherQueryObfuscator(None, ObfuscationPolicy.FullLiteralsAlways)

    val text = "MATCH (n) RETURN n"
    ObfuscatedQuery.optional(ob.fullyObfuscatedQuery(text, MapValue.EMPTY, 0)).isPresent() shouldBe false
    // The default view falls back to best-effort raw text (no offsets to redact).
    ob.defaultObfuscatedQuery(text, MapValue.EMPTY, 0).text should equal(text)
  }

  test("collected-but-empty metadata with the full view available selects PASSTHROUGH") {
    val ob = CypherQueryObfuscator(Some(ObfuscationMetadata.empty()), ObfuscationPolicy.FullLiteralsOnDemand)

    ob should be theSameInstanceAs QueryObfuscator.PASSTHROUGH
  }

  // Single-view convenience for the per-piece (sensitive) assertions: the given offsets are both the sensitive
  // and the all view, so obfuscateText/obfuscatePosition (which use the sensitive view) behave as before.
  private def secretMeta(offsets: Vector[LiteralOffset], params: Set[String]): ObfuscationMetadata =
    ObfuscationMetadata(offsets, offsets, params)

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
