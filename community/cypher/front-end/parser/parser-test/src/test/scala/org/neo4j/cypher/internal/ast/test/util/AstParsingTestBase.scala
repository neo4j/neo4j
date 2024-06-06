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
package org.neo4j.cypher.internal.ast.test.util

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.test.util.AstParsing.ParseResults
import org.neo4j.cypher.internal.ast.test.util.AstParsing.ParserInTest
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.scalatest.matchers.Matcher
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import scala.reflect.ClassTag
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
 * Test helpers for cypher ast parsing.
 */
trait AstParsingTestBase extends CypherFunSuite
    with AstParsingMatchers
    with TestNameAstAssertions
    with AstConstructionTestSupport {

  /** Debug the specified cypher. */
  def debug[T <: ASTNode : ClassTag](cypher: String)(implicit p: Parsers[T]): Unit = Try(parseAst[T](cypher)) match {
    case Success(results) => throw new RuntimeException(MatchResults.describe(results))
    case Failure(e)       => throw new RuntimeException(s"Test framework failed unexpectedly\nCypher: $cypher", e)
  }
}

/**
 * Provides scalatest matchers for cypher parsing.
 * See [[AstParsingTestBase]] for some examples.
 */
trait AstParsingMatchers extends TestName {

  /**
   * Parse successfully to any ast in all parsers.
   * The returned [[ParseStringMatcher]] can be used to add assertions.
   */
  def parse[T <: ASTNode : ClassTag](implicit p: Parsers[T]): ParseStringMatcher[T] =
    ParseStringMatcher[T]().withoutErrors

  /**
   * Parse successfully to the specified ast in all parsers.
   * The returned [[ParseStringMatcher]] can be used to add assertions.
   */
  def parseTo[T <: ASTNode : ClassTag](expected: T)(implicit p: Parsers[T]): ParseStringMatcher[T] =
    parse[T].toAst(expected)

  /**
   * Fail to parse in all parsers.
   * The returned [[ParseStringMatcher]] can be used to add assertions.
   */
  def notParse[T <: ASTNode : ClassTag](implicit p: Parsers[T]): ParseStringMatcher[T] =
    ParseStringMatcher[T]().withAnyFailure

  /**
   * Custom assertion for each parser.
   * The returned [[ParseStringMatcher]] can be used to add assertions.
   */
  def parseIn[T <: ASTNode : ClassTag](
    f: ParserInTest => ParseStringMatcher[T] => ParseStringMatcher[T]
  )(implicit p: Parsers[T]): ParseStringMatcher[T] =
    ParseStringMatcher[T]().in(f)
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
  def parses[T <: ASTNode : ClassTag](implicit p: Parsers[T]): Parses[T] =
    Parses(parseAst[T](testName)).withoutErrors

  /**
   * Parse successfully to the specified ast in all parsers.
   * The returned [[Parses]] can be used to add assertions.
   */
  def parsesTo[T <: ASTNode : ClassTag](e: T)(implicit p: Parsers[T]): Unit =
    parses[T].toAst(e)

  /**
   * Fails to parse in all parsers.
   * The returned [[Parses]] can be used to add assertions.
   */
  def failsParsing[T <: ASTNode : ClassTag]()(implicit p: Parsers[T]): Parses[T] =
    Parses(parseAst[T](testName)).withAnyFailure

  /**
   * Custom assertion for each parser.
   * The returned [[Parses]] can be used to add assertions.
   */
  def parsesIn[T <: ASTNode : ClassTag](
    f: ParserInTest => Parses[T] => Parses[T]
  )(implicit p: Parsers[T]): Parses[T] = Parses(parseAst[T](testName)).in(f)
}

case class Parses[T <: ASTNode : ClassTag](
  result: ParseResults[T],
  override val support: ParserInTest => Boolean = _ => true
) extends FluentMatchers[Parses[T], T] {

  override protected def copyWith(matchers: Seq[Matcher[ParseResults[_]]]): Parses[T] = {
    matchers.foreach(`match` => result should `match`)
    this
  }

  override protected def createForParser(p: ParserInTest): Parses[T] = Parses(result, _ == p)
  override protected def matchers: Seq[Matcher[ParseResults[_]]] = Seq.empty
}
