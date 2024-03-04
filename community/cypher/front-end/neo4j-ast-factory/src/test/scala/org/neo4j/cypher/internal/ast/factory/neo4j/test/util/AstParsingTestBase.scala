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
package org.neo4j.cypher.internal.ast.factory.neo4j.test.util

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.Antlr
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.ParseResults
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.ParserSupport.All
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.ParserSupport.NotAntlr
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.ParserSupport.NotAnyAntlr
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.scalatest.matchers.Matcher
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import scala.reflect.ClassTag

/**
 * Test helpers for cypher ast parsing.
 *
 * Here are just a few examples to get you going, each statement in a group has the same meaning:
 *
 * {{{
 * test("[1]") {
 *   parsesTo[Expression](LiteralList(Literal(1)))
 *   parses[Expression].toAst(LiteralList(Literal(1)))
 *   "[1]" should parseTo[Expression](LiteralList(Literal(1)))
 *   "[1]" should parse[Expression].toAst(LiteralList(Literal(1)))
 *
 *   parses[Expression].toAstPositioned(LiteralList(Literal(1)))
 *   "[1]" should parse[Expression].toAstPositioned(LiteralList(Literal(1)))
 *
 *   parses[Expression]
 *     .toAst(LiteralList(LiteralInt(1)))
 *     .withPositionOf[LiteralInt](InputPosition(1,2,3))
 *   "[1]" should parse[Expression]
 *     .toAst(LiteralList(LiteralInt(1)))
 *     .withPositionOf[LiteralInt](InputPosition(1,2,3))
 *
 *   parses[Expression].toAsts {
 *     case JavaCc => LiteralList(Literal(1))
 *     case Antlr => LiteralList(AntlrLiteral(1))
 *   }
 *   "[1]" should parse[Expression].toAsts {
 *     case JavaCc => LiteralList(Literal(1))
 *     case Antlr => LiteralList(AntlrLiteral(1))
 *   }
 *
 *   parses[Expression].
 *     .parseIn(JavaCc)(_.toPositionedAst(...))
 *     .parseIn(Antlr)(_.toAst(...))
 *   "[1]" should parse[Expression]
 *     .parseIn(JavaCc)(_.toPositionedAst(...))
 *     .parseIn(Antlr)(_.toAst(...))
 *
 *   // Avoid if possible, only when parsers have completely different behaviour.
 *   whenParsing[Expression]
 *     .parseIn(JavaCc)(_.toAst(...))
 *     .parseIn(Antlr)(_.throws[OhNoException])
 *   "[1]" should parseAs[Expression]
 *     .parseIn(JavaCc)(_.toAst(...))
 *     .parseIn(Antlr)(_.throws[OhNoException])
 * }
 *
 *
 * test("with 1 as 1v return 1v") {
 *   failsParsing[Statements] // Prefer to assert on the error type/message
 *   "with 1 as 1v return 1v" should notParse[Statements]
 *
 *   failsParsing[Statements]
 *     .throws[InvalidVariableNameException]
 *     .withMessage("Incorrect name")
 *   "with 1 as 1v return 1v" should notParse[Statements]
 *     .throws[InvalidVariableNameException]
 *     .withMessage("Incorrect name")
 * }
 * }}}
 */
trait AstParsingTestBase extends CypherFunSuite
    with AstParsingMatchers
    with TestNameAstAssertions
    with AstConstructionTestSupport

/**
 * Provides scalatest matchers for cypher parsing.
 * See [[AstParsingTestBase]] for some examples.
 */
trait AstParsingMatchers extends TestName {

  /**
   * Parse successfully to any ast in all '''supported''' parsers.
   * The returned [[ParseStringMatcher]] can be used to add assertions.
   */
  def parse[T <: ASTNode : ClassTag](support: ParserSupport): ParseStringMatcher[T] =
    handleAntlrParse(parseAs[T](support).withoutErrors)

  /** See [[parse(ParserSupport)]]. */
  def parse[T <: ASTNode : ClassTag]: ParseStringMatcher[T] = parse[T](All)

  /**
   * Parse successfully to the specified ast in all '''supported''' parsers.
   * The returned [[ParseStringMatcher]] can be used to add assertions.
   */
  def parseTo[T <: ASTNode : ClassTag](expected: T): ParseStringMatcher[T] = parseAs[T].toAst(expected)

  /**
   * See [[parseTo(ParserSupport)]].
   */
  def parseTo[T <: ASTNode : ClassTag](support: ParserSupport)(expected: T): ParseStringMatcher[T] =
    handleAntlrParse(parse[T](support).toAst(expected))

  /**
   * Fail to parse in all '''supported''' parsers.
   * The returned [[ParseStringMatcher]] can be used to add assertions.
   */
  def notParse[T <: ASTNode : ClassTag](support: ParserSupport): ParseStringMatcher[T] =
    handleAntlrNotParse(parseAs[T](support).withAnyFailure)

  /**
   * See [[notParse(ParserSupport)]]
   */
  def notParse[T <: ASTNode : ClassTag]: ParseStringMatcher[T] =
    parseAs[T].withAnyFailure

  /**
   * No prior expectations.
   *
   * Avoid if possible, useful when you want completely separate behaviour from different parsers.
   */
  def parseAs[T <: ASTNode : ClassTag](support: ParserSupport): ParseStringMatcher[T] = ParseStringMatcher[T](support)

  /**
   * See [[parseAs(ParserSupport)]]
   */
  def parseAs[T <: ASTNode : ClassTag]: ParseStringMatcher[T] = parseAs[T](All)

  // Handle general limited support in antlr during development
  protected def handleAntlrParse[T <: FluentMatchers[T, _]](f: T): T = {
    f.support match {
      case NotAntlr    => f.parseIn(Antlr)(_.withoutErrors.toAst(null))
      case NotAnyAntlr => f.parseIn(Antlr)(_.withoutErrors)
      case _           => f
    }
  }

  // Handle general limited support in antlr during development
  protected def handleAntlrNotParse[T <: FluentMatchers[T, _]](f: T): T = {
    f.support match {
      case NotAntlr    => f.parseIn(Antlr)(_.withAnyFailure)
      case NotAnyAntlr => f.parseIn(Antlr)(_.withAnyFailure)
      case _           => f
    }
  }
}

object AstParsingMatchers extends AstParsingMatchers

/**
 * Provides methods to assert on cypher parsing, based on the test name.
 * See [[AstParsingTestBase]] for some examples.
 */
trait TestNameAstAssertions extends AstParsingMatchers with AstParsing with TestName {

  /**
   * Parse successfully to any ast in all parsers.
   * The returned [[Parses]] can be used to add assertions.
   */
  def parses[T <: ASTNode : ClassTag]: Parses[T] = parses[T](All)

  /**
   * Parse successfully to any ast in all '''supported''' parsers.
   * The returned [[Parses]] can be used to add assertions.
   */
  def parses[T <: ASTNode : ClassTag](support: ParserSupport): Parses[T] =
    handleAntlrParse(Parses(parseAst[T](testName), support).withoutErrors)

  /**
   * Parse successfully to the specified ast in all parsers.
   * The returned [[Parses]] can be used to add assertions.
   */
  def parsesTo[T <: ASTNode : ClassTag](e: T): Unit = parses[T].toAst(e)

  /**
   * Parse successfully to the specified ast in all '''supported''' parsers.
   * The returned [[Parses]] can be used to add assertions.
   */
  def parsesTo[T <: ASTNode : ClassTag](s: ParserSupport)(e: T): Unit = parses[T](s).toAst(e)

  /**
   * Fails to parse in all parsers.
   * The returned [[Parses]] can be used to add assertions.
   */
  def failsParsing[T <: ASTNode : ClassTag]: Parses[T] = failsParsing[T](All)

  /**
   * Fails to parse in all '''supported''' parsers.
   * The returned [[Parses]] can be used to add assertions.
   */
  def failsParsing[T <: ASTNode : ClassTag](support: ParserSupport): Parses[T] =
    handleAntlrNotParse(Parses(parseAst[T](testName), support).withAnyFailure)

  /**
   * Avoid if possible. Makes no prior assertions.
   * The returned [[Parses]] can be used to add assertions.
   */
  def whenParsing[T <: ASTNode : ClassTag]: Parses[T] = whenParsing[T](All)

  /**
   * Avoid if possible. Makes no prior assertions.
   * The returned [[Parses]] can be used to add assertions.
   */
  def whenParsing[T <: ASTNode : ClassTag](support: ParserSupport): Parses[T] = Parses(parseAst[T](testName), support)
}

case class Parses[T <: ASTNode : ClassTag](
  result: ParseResults[T],
  override val support: ParserSupport
) extends FluentMatchers[Parses[T], T] {

  override protected def copyWith(matchers: Seq[Matcher[ParseResults[_]]]): Parses[T] = {
    matchers.foreach(`match` => result should `match`)
    this
  }

  override protected def createForParser(support: ParserSupport): Parses[T] = Parses(result, support)
  override protected def matchers: Seq[Matcher[ParseResults[_]]] = Seq.empty
}
