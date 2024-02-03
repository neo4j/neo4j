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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast.factory.neo4j.LiteralsParserTest.escapeSequences
import org.neo4j.cypher.internal.ast.factory.neo4j.LiteralsParserTest.genCodepoint
import org.neo4j.cypher.internal.ast.factory.neo4j.LiteralsParserTest.genCypherUnicodeEscape
import org.neo4j.cypher.internal.ast.factory.neo4j.LiteralsParserTest.toCypherHex
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.Antlr
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.JavaCc
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.LegacyAstParsingTestSupport
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.DecimalDoubleLiteral
import org.neo4j.cypher.internal.expressions.Infinity
import org.neo4j.cypher.internal.expressions.Literal
import org.neo4j.cypher.internal.expressions.NaN
import org.neo4j.cypher.internal.expressions.Null
import org.neo4j.cypher.internal.expressions.NumberLiteral
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.SignedHexIntegerLiteral
import org.neo4j.cypher.internal.expressions.SignedOctalIntegerLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.test_helpers.CypherScalaCheckDrivenPropertyChecks
import org.scalacheck.Gen
import org.scalacheck.Shrink

class LiteralsParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport
    with CypherScalaCheckDrivenPropertyChecks {
  private val t = DummyPosition(0)
  implicit def noShrink[T]: Shrink[T] = Shrink.shrinkAny // 🤯

  test("test variable can contain ascii") {
    parsing[Variable]("abc") shouldGive expressions.Variable("abc")(t)
    parsing[Variable]("a123") shouldGive expressions.Variable("a123")(t)
    parsing[Variable]("ABC") shouldGive expressions.Variable("ABC")(t)
    parsing[Variable]("_abc") shouldGive expressions.Variable("_abc")(t)
    parsing[Variable]("abc_de") shouldGive expressions.Variable("abc_de")(t)
  }

  test("test variable can contain utf8") {
    parsing[Variable]("aé") shouldGive expressions.Variable("aé")(t)
    parsing[Variable]("⁔") shouldGive expressions.Variable("⁔")(t)
    parsing[Variable]("＿test") shouldGive expressions.Variable("＿test")(t)
    parsing[Variable]("a＿test") shouldGive expressions.Variable("a＿test")(t)
  }

  test("test variable name can not start with number") {
    assertFails[Variable]("1bcd")
  }

  test("can parse numbers") {
    val validInts = Seq("123", "0", "-23", "-0")
    for (i <- validInts) {
      i should parseTo[NumberLiteral](SignedDecimalIntegerLiteral(i)(pos))
    }

    val validOctalInts = Seq("0234", "-0234", "01", "0o1", "0_2")
    for (o <- validOctalInts) {
      o should parseTo[NumberLiteral](SignedOctalIntegerLiteral(o)(pos))
    }

    val validHexInts = Seq("0x1", "0X1", "0xffff", "-0x45FG")
    for (h <- validHexInts) {
      h should parseTo[NumberLiteral](SignedHexIntegerLiteral(h)(pos))
    }

    val validDoubles = Seq(
      "1.23",
      "13434.23399",
      ".3454",
      "-0.0",
      "-54366.4",
      "-0.3454",
      "1E23",
      "1e23",
      "1E+23",
      "1.34E99",
      "9E-443",
      "0.0d",
      ".0d",
      "1e0d",
      "0.0f",
      "0.0somegibberish",
      "0.0"
    )
    for (d <- validDoubles) {
      d should parseTo[NumberLiteral](DecimalDoubleLiteral(d)(pos))
    }
    "- 1.4" should parseTo[NumberLiteral](DecimalDoubleLiteral("-1.4")(pos))
    "--1.0" should notParse[NumberLiteral]

    val invalid = Seq("NaN", "Infinity", "Ox", "0_.0", "1_._1", "._2", "1_.0001", "1._0001")
    for (i <- invalid) {
      assertFails[NumberLiteral](i)
    }
  }

  test("can parse parameter syntax") {
    parsing[Parameter]("$p") shouldGive expressions.ExplicitParameter("p", CTAny)(t)
    parsing[Parameter]("$`the funny horse`") shouldGive expressions.ExplicitParameter("the funny horse", CTAny)(t)
    parsing[Parameter]("$0") shouldGive expressions.ExplicitParameter("0", CTAny)(t)

    // parameter number boundaries

    parsing[Parameter]("$1_2") shouldGive parameter("1_2", CTAny)
    parsing[Parameter]("$1") shouldGive parameter("1", CTAny)
    parsing[Parameter]("$1gibberish") shouldGive parameter("1gibberish", CTAny)

    assertFails[Parameter]("$0_2")
    assertFails[Parameter]("$1.0f")
  }

  test("variables are not allowed to start with currency symbols") {
    Seq("$", "¢", "£", "₲", "₶", "\u20BD", "＄", "﹩").foreach { curr =>
      assertFails[Variable](s"${curr}var")
    }
  }

  test("keyword literals") {
    "  \n NaN" should parse[Literal].toAstPositioned(NaN()(InputPosition(4, 2, 2)))
    "   nan" should parse[Literal].toAstPositioned(NaN()(InputPosition(3, 1, 4)))
    "  \n inf" should parse[Literal].toAstPositioned(Infinity()(InputPosition(4, 2, 2)))
    " INF" should parse[Literal].toAstPositioned(Infinity()(InputPosition(1, 1, 2)))
    "  INFINITY" should parse[Literal].toAstPositioned(Infinity()(InputPosition(2, 1, 3)))
    "  \n null" should parse[Literal].toAstPositioned(Null()(InputPosition(4, 2, 2)))
    " NULL" should parse[Literal].toAstPositioned(Null()(InputPosition(1, 1, 2)))
  }

  test("string literal escape sequences") {
    escapeSequences.foreach { case (cypher, result) =>
      s"'$cypher'" should parseTo[Literal](literalString(result))
      s"\"$cypher\"" should parseTo[Literal](literalString(result))
      if (result != "'" && result != "\\") {
        s"'\\$cypher'" should parseTo[Literal](literalString("\\" + cypher.drop(1)))
      }
    }

    // Non escape sequences, for example '\x', '\y' ...
    forAll(genCodepoint.map(c => s"\\$c"), minSuccessful(20)) { backslashX =>
      whenever(!escapeSequences.contains(backslashX)) {
        s"'$backslashX'" should parseTo[Literal](literalString(backslashX))
      }
    }

    // TODO Message
    "'\\'" should notParse[Literal]
    "'\\\\\\'" should notParse[Literal]
  }

  test("string literal unicode escape") {
    s"'${toCypherHex("꠲".codePointAt(0))}'" should parseTo[Literal](literalString("꠲"))

    // Arbitrary unicode escape codes
    forAll(genCodepoint, minSuccessful(100)) { codepoint =>
      whenever(codepoint != 0 && codepoint != '\'') {
        s"'${toCypherHex(codepoint)}'" should parseTo[Literal](literalString(Character.toString(codepoint)))
      }
    }

    s"'${toCypherHex('\\')}'" should notParse[Literal]
    s"'${toCypherHex('\'')}'" should parseAs[Literal]
      .parseIn(JavaCc)(_.withAnyFailure)
      .parseIn(Antlr)(_.toAst(literalString("")))

    "'\\U1'" should parseTo[Literal](literalString("\\U1"))
    "'\\U12'" should parseTo[Literal](literalString("\\U12"))
    "'\\U123'" should parseTo[Literal](literalString("\\U123"))

    // TODO Messages
    "'\\u1'" should notParse[Literal]
    "'\\u12'" should notParse[Literal]
    "'\\u123'" should notParse[Literal]
    "'\\ux111'" should notParse[Literal]
  }

  test("arbitrary string literals") {
    "'\\f\\'6\\u0046\\u8da4\\''" should parseTo[Literal](literalString("\f\'6\u0046\u8da4\'"))
    """'\\u\t\n'""" should parseTo[Literal](literalString("\\u\t\n"))

    forAll(
      Gen.listOf[(String, String)](Gen.oneOf(
        genCodepoint.filter(c => c != '"' && c != '\\').map(c => Character.toString(c) -> Character.toString(c)),
        genCypherUnicodeEscape,
        Gen.oneOf(escapeSequences)
      )),
      minSuccessful(100)
    ) { stringParts =>
      val (cypherParts, expectedParts) = stringParts.unzip
      val cypher = cypherParts.mkString("\"", "", "\"")
      val expected = expectedParts.mkString("")
      cypher should parseTo[Literal](literalString(expected))
    }
  }

  test("position with unicode") {
    "\\u0009\\u2003'hello'" should parse[Literal].toAstPositioned(
      StringLiteral("hello")(InputPosition(12, 1, 13), InputPosition(18, 1, 19))
    )
    "/* \uD80C\uDCDF */'hello'" should parse[Literal].toAstPositioned(
      StringLiteral("hello")(InputPosition(8, 1, 9), InputPosition(18, 1, 19))
    )
  }
}

object LiteralsParserTest {

  private val escapeSequences: Map[String, String] = Map(
    """\t""" -> "\t",
    """\b""" -> "\b",
    """\n""" -> "\n",
    """\r""" -> "\r",
    """\f""" -> "\f",
    """\'""" -> "'",
    """\"""" -> "\"",
    """\\""" -> "\\"
  )

  private def toCypherHex(codepoint: Int): String = {
    Character.toString(codepoint)
      .map(c => "\\u" + ("0000" + Integer.toHexString(c)).takeRight(4))
      .mkString("")
  }

  // Weighted to include common codepoints more often
  private val genCodepoint: Gen[Int] = {
    Gen.oneOf(
      Gen.chooseNum(Character.MIN_CODE_POINT + 1, Character.MIN_SURROGATE - 1),
      Gen.chooseNum(Character.MAX_SURROGATE + 1, Character.MAX_CODE_POINT),
      Gen.alphaNumChar.map(_.toInt) // Increase probability of alpha nums
    )
  }

  private val genCypherUnicodeEscape: Gen[(String, String)] = {
    genCodepoint.map(codepoint => toCypherHex(codepoint) -> Character.toString(codepoint))
  }
}
