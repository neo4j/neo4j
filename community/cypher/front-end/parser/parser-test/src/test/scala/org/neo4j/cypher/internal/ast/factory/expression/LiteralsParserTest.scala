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
package org.neo4j.cypher.internal.ast.factory.expression

import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.expression.LiteralsParserTest.escapeSequences
import org.neo4j.cypher.internal.ast.factory.expression.LiteralsParserTest.genCodepoint
import org.neo4j.cypher.internal.ast.factory.expression.LiteralsParserTest.genCypherUnicodeEscape
import org.neo4j.cypher.internal.ast.factory.expression.LiteralsParserTest.toCypherHex
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher6
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.expressions.DecimalDoubleLiteral
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.Infinity
import org.neo4j.cypher.internal.expressions.Literal
import org.neo4j.cypher.internal.expressions.NaN
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.Null
import org.neo4j.cypher.internal.expressions.NumberLiteral
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.SignedHexIntegerLiteral
import org.neo4j.cypher.internal.expressions.SignedOctalIntegerLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.test_helpers.CypherScalaCheckDrivenPropertyChecks
import org.scalacheck.Gen
import org.scalacheck.Shrink

import scala.collection.compat.immutable.ArraySeq

class LiteralsParserTest extends AstParsingTestBase
    with CypherScalaCheckDrivenPropertyChecks {
  implicit def noShrink[T]: Shrink[T] = Shrink.shrinkAny // 🤯

  test("can parse numbers") {
    val validInts = Seq("123", "0", "-23", "-0")
    for (i <- validInts) {
      i should parseTo[NumberLiteral](SignedDecimalIntegerLiteral(i)(pos))
    }

    val validOctalIntsCypher5 = Seq("0234", "0o234", "-0o234", "-0234", "01", "0o1", "0_2", "0o_2")
    val validOctalIntsCypher6 = Seq("0o234", "-0o234", "0o1", "0o_2")
    for (o <- validOctalIntsCypher5) {
      o should parseIn[NumberLiteral] {
        case Cypher6 if !validOctalIntsCypher6.contains(o) => _.withMessageStart("""Invalid input""".stripMargin)
        case _                                             => _.toAst(SignedOctalIntegerLiteral(o)(pos))
      }
    }

    val validHexIntsCypher5 = Seq("0x1", "0X1", "0xffff", "-0x45FG")
    val validHexIntsCypher6 = Seq("0x1", "0xffff", "-0x45FG")
    for (h <- validHexIntsCypher5) {
      h should parseIn[NumberLiteral] {
        case Cypher6 if !validHexIntsCypher6.contains(h) => _.withMessageStart("""Invalid input""".stripMargin)
        case _                                           => _.toAst(SignedHexIntegerLiteral(h)(pos))
      }
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
    "--1.0" should notParse[NumberLiteral].in {
      case Cypher5JavaCc => _.withMessageStart("Encountered \" \"-\" \"-\"\" at line 1, column 2.")
      case _ => _.withSyntaxError(
          """Invalid input '-': expected a number (line 1, column 2 (offset: 1))
            |"--1.0"
            |  ^""".stripMargin
        )
    }

    "RETURN NaN" should parseTo[Statements](
      Statements(Seq(singleQuery(return_(returnItem(NaN()(pos), "NaN")))))
    )
    "RETURN Infinity" should parseTo[Statements](
      Statements(Seq(singleQuery(return_(returnItem(Infinity()(pos), "Infinity")))))
    )
    "RETURN Ox" should parseTo[Statements](
      Statements(Seq(singleQuery(return_(returnItem(varFor("Ox"), "Ox")))))
    )
    "RETURN 0_.0" should notParse[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '.0'")
      case Cypher5 => _.withSyntaxError(
          """Invalid input '.0': expected an expression, 'FOREACH', ',', 'AS', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF> (line 1, column 10 (offset: 9))
            |"RETURN 0_.0"
            |          ^""".stripMargin
        )
      case Cypher6 => _.withSyntaxError(
          """Invalid input '_': expected an expression, 'FOREACH', ',', 'AS', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF> (line 1, column 9 (offset: 8))
            |"RETURN 0_.0"
            |         ^""".stripMargin
        )
    }
    "RETURN 1_._1" should parseTo[Statements](
      Statements(Seq(singleQuery(return_(returnItem(prop(SignedDecimalIntegerLiteral("1_")(pos), "_1"), "1_._1")))))
    )
    "RETURN ._2" should notParse[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          "Invalid input '.': expected \"*\", \"DISTINCT\" or an expression (line 1, column 8 (offset: 7))"
        )
      case _ => _.withSyntaxError(
          """Invalid input '.': expected an expression, '*' or 'DISTINCT' (line 1, column 8 (offset: 7))
            |"RETURN ._2"
            |        ^""".stripMargin
        )
    }
    "RETURN 1_.0001" should notParse[Statements].withMessageStart("Invalid input '.0001'")
    "RETURN 1._0001" should parseTo[Statements](
      Statements(Seq(singleQuery(return_(returnItem(prop(SignedDecimalIntegerLiteral("1")(pos), "_0001"), "1._0001")))))
    )
  }

  test("can parse parameter syntax") {
    "$p" should parseTo(parameter("p", CTAny))
    "$`the funny horse`" should parseTo(parameter("the funny horse", CTAny))
    "$0" should parseTo(parameter("0", CTAny))

    // parameter number boundaries

    "$1_2" should parseTo(parameter("1_2", CTAny))
    "$1" should parseTo(parameter("1", CTAny))
    "$1gibberish" should parseTo(parameter("1gibberish", CTAny))

    "$0_2" should notParse[Parameter].in {
      case Cypher5JavaCc => _.withMessageStart("Encountered")
      case Cypher5 => _.withSyntaxError(
          """Invalid input '0_2': expected an identifier or an integer value (line 1, column 2 (offset: 1))
            |"$0_2"
            |  ^""".stripMargin
        )
      case Cypher6 => _.withSyntaxError(
          """Invalid input '_2' (line 1, column 3 (offset: 2))
            |"$0_2"
            |   ^""".stripMargin
        )
    }
    "return $1.0f" should notParse[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '$': expected \"+\" or \"-\"")
      case _ => _.withSyntaxError(
          """Invalid input '1.0f': expected an identifier or an integer value (line 1, column 9 (offset: 8))
            |"return $1.0f"
            |         ^""".stripMargin
        )
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

    "'\\'" should notParse[Literal].in {
      case Cypher5JavaCc => _.withMessageStart("Lexical error")
      case _ => _.withSyntaxErrorContaining(
          "Failed to parse string literal. The query must contain an even number of non-escaped quotes."
        )
    }
    "'\\\\\\'" should notParse[Literal].in {
      case Cypher5JavaCc => _.withMessageStart("Lexical error")
      case _ => _.withSyntaxErrorContaining(
          "Failed to parse string literal. The query must contain an even number of non-escaped quotes."
        )
    }
  }

  test("string literal unicode escape") {
    s"'${toCypherHex("꠲".codePointAt(0))}'" should parseTo[Literal](literalString("꠲"))

    // Arbitrary unicode escape codes
    forAll(genCodepoint, minSuccessful(100)) { codepoint =>
      whenever(codepoint != 0 && codepoint != '\'') {
        s"'${toCypherHex(codepoint)}'" should parseTo[Literal](literalString(Character.toString(codepoint)))
      }
    }

    s"RETURN '${toCypherHex('\\')}'" should notParse[Statements]
    s"RETURN '${toCypherHex('\'')}'" should notParse[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Lexical error")
      case _ => _.withSyntaxErrorContaining(
          """Failed to parse string literal. The query must contain an even number of non-escaped quotes. (line 1, column 15 (offset: 14))"""
        )
    }

    "'\\U1'" should parseTo[Literal](literalString("\\U1"))
    "'\\U12'" should parseTo[Literal](literalString("\\U12"))
    "'\\U123'" should parseTo[Literal](literalString("\\U123"))

    "'\\u1'" should notParse[Literal].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input ''': expected four hexadecimal digits")
      case _             => _.withSyntaxErrorContaining("Invalid input '1'': expected four hexadecimal digits")
    }
    "'\\u12'" should notParse[Literal].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input ''': expected four hexadecimal digits")
      case _             => _.withSyntaxErrorContaining("Invalid input '12'': expected four hexadecimal digits")
    }
    "'\\u123'" should notParse[Literal].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input ''': expected four hexadecimal digits")
      case _             => _.withSyntaxErrorContaining("Invalid input '123'': expected four hexadecimal digits")
    }
    "'\\ux111'" should notParse[Literal].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'x': expected four hexadecimal digits")
      case _             => _.withSyntaxErrorContaining("Invalid input 'x111': expected four hexadecimal digits")
    }
  }

  test("arbitrary string literals") {
    "'\\f\\'6\\u0046\\u8da4\\''" should parseTo[Literal](literalString("\f\'6\u0046\u8da4\'"))
    """'\\u\t\n'""" should parseTo[Literal](literalString("\\u\t\n"))

    forAll(
      Gen.listOf[(String, String)](Gen.oneOf(
        genCodepoint.filter(c => c != '"' && c != '\\').map(c => Character.toString(c) -> Character.toString(c)),
        genCypherUnicodeEscape.filter { case (_, e) => e != "\"" && e != "\\" },
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
      StringLiteral("hello")(InputPosition(12, 1, 13).withInputLength(7))
    )
    "/* \uD80C\uDCDF */'hello'" should parse[Literal].toAstPositioned(
      StringLiteral("hello")(InputPosition(8, 1, 9).withInputLength(7))
    )
  }

  test("correct position length of string literals") {
    "return \n''\n AS res" should parse[Statements].toAstPositioned {
      Statements(Seq(singleQuery(return_(
        AliasedReturnItem(
          StringLiteral("")(InputPosition(8, 2, 1).withInputLength(2)),
          varFor("res")
        )(pos)
      ))))
    }

    "return \n'\na\nb\nc\n'\n AS res" should parse[Statements].toAstPositioned {
      Statements(Seq(singleQuery(return_(
        AliasedReturnItem(
          StringLiteral("\na\nb\nc\n")(InputPosition(8, 2, 1).withInputLength(9)),
          varFor("res")
        )(pos)
      ))))
    }

    "return 'abc\\u000A' AS res" should parse[Statements].toAstPositioned {
      Statements(Seq(singleQuery(return_(
        AliasedReturnItem(
          StringLiteral("abc\n")(InputPosition(7, 1, 8).withInputLength(11)),
          varFor("res")
        )(pos)
      ))))
    }

    "return 'abc\uD80C\uDCDF' AS res" should parse[Statements].toAstPositioned {
      Statements(Seq(singleQuery(return_(
        AliasedReturnItem(
          StringLiteral("abc\uD80C\uDCDF")(InputPosition(7, 1, 8).withInputLength(7)),
          varFor("res")
        )(pos)
      ))))
    }

    "\\u000A return \\u000A\n\\u000A" +
      "'\\u000A\n\\u000Aa" +
      "\\u000A\n\\u000Ab" +
      "\\u000A\n\\u000Ac" +
      "\\u000A\n\\u000A'" +
      "\\u000A\n\\u000A AS res" should parse[Statements].toAstPositioned {
        Statements(Seq(singleQuery(return_(
          AliasedReturnItem(
            StringLiteral("\n\n\na\n\n\nb\n\n\nc\n\n\n")(InputPosition(27, 2, 7).withInputLength(57)),
            varFor("res")
          )(pos)
        ))))
      }

    "return test.function('hello', 'hallåhallå') as res" should parse[Statements].toAstPositioned {
      Statements(Seq(singleQuery(return_(
        AliasedReturnItem(
          FunctionInvocation(
            FunctionName(Namespace(List("test"))(pos), "function")(pos),
            distinct = false,
            ArraySeq(
              StringLiteral("hello")(InputPosition(21, 1, 22).withInputLength(7)),
              StringLiteral("hallåhallå")(InputPosition(30, 1, 31).withInputLength(12))
            )
          )(pos),
          varFor("res")
        )(pos)
      ))))
    }
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
