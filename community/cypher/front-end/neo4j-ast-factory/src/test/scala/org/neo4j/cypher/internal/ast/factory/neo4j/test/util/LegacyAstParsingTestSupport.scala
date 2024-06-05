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

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * Provides some methods to support old tests.
 *
 * DO NOT USE PLEASE! Prefer to use methods from [[AstParsingTestBase]] to unify the tests.
 * If there is something here worth keeping, move it to [[AstParsingTestBase]].
 *
 * @deprecated Use [[AstParsingTestBase]]
 */
@deprecated("Use AstParsingTestBase", "-")
trait LegacyAstParsingTestSupport {
  self: AstParsingTestBase =>
  implicit protected def lift(pos: (Int, Int, Int)): InputPosition = InputPosition(pos._3, pos._1, pos._2)
  implicit def statementToStatements(statement: Statement): Statements = Statements(Seq(statement))

  implicit def posToStatementToStatements(statement: InputPosition => Statement): InputPosition => Statements =
    p => Statements(Seq(statement(p)))

  /**
   * @deprecated use [[parsesTo]], `parsesTo[T](expected)`
   *             or [[parseTo]], `"cypher" should parseTo[T](expected)`
   */
  @deprecated("Use methods from AstParsingTestBase", "-")
  final def yields[T <: ASTNode : ClassTag](expected: InputPosition => T)(implicit p: Parsers[T]): Unit =
    parses[T].toAst(expected(InputPosition.NONE))

  /**
   * @deprecated use [[parsesTo]], `parsesTo[T](expected)`
   *             or [[parseTo]], `"cypher" should parseTo[T](expected)`
   */
  @deprecated("Use methods from AstParsingTestBase", "-")
  final def gives[T <: ASTNode : ClassTag](expected: T)(implicit p: Parsers[T]): Unit = parsesTo[T](expected)

  /**
   * @deprecated use [[parses]], `parses[T].toAstPositioned(expected)`
   *             or [[parse]], `"cypher" should parse[T].toAstPositioned(expected)`.
   */
  @deprecated("Use methods from AstParsingTestBase", "-")
  final def givesIncludingPositions[T <: ASTNode : ClassTag](expected: T, query: String = testName)(implicit
  p: Parsers[T]): Unit =
    query should parse[T].toAstPositioned(expected)

  /**
   * @deprecated use [[parse]] instead, `"cypher" should parse[T].toAstLike(ast => ast.hej shouldBe "hej")`
   */
  @deprecated("Use methods from AstParsingTestBase", "-")
  final def parsing[T <: ASTNode : ClassTag](cypher: String): LegacyParse[T] = new LegacyParse(cypher)

  /**
   * @deprecated use [[failsParsing]].
   */
  @deprecated("Use methods from AstParsingTestBase", "-")
  def failsToParse[T <: ASTNode : ClassTag]()(implicit p: Parsers[T]): Unit = failsParsing[T]

  /**
   * @deprecated use [[notParse]].
   */
  @deprecated("Use methods from AstParsingTestBase", "-")
  def failsToParse[T <: ASTNode : ClassTag](cypher: String)(implicit p: Parsers[T]): Unit =
    cypher should notParse[T]

  /**
   * @deprecated use [[notParse]] instead.
   */
  @deprecated("Use methods from AstParsingTestBase", "-")
  def assertFails[T <: ASTNode : ClassTag](cypher: String)(implicit p: Parsers[T]): Unit =
    cypher should notParse[T]

  /**
   * @deprecated use [[parseIn]], `"cypher" should parseIn[T] { case JavaCc =>_.withAnyFailure }`.
   */
  @deprecated("Use methods from AstParsingTestBase", "-")
  def failsToParseOnlyJavaCC[T <: ASTNode : ClassTag](query: String)(implicit p: Parsers[T]): Unit =
    assertFailsOnlyJavaCC[T](query)

  /**
   * @deprecated use [[parsesIn]], `parsesIn[T] { case JavaCc =>_.withAnyFailure)` }.
   */
  @deprecated("Use methods from AstParsingTestBase", "-")
  def failsToParseOnlyJavaCC[T <: ASTNode : ClassTag]()(implicit p: Parsers[T]): Unit =
    assertFailsOnlyJavaCC[T](testName)

  /**
   * @deprecated use [[parseIn]], `"cypher" should parseIn[T] { case JavaCc =>_.withAnyFailure }`.
   */
  @deprecated("Use methods from AstParsingTestBase", "-")
  def assertFailsOnlyJavaCC[T <: ASTNode : ClassTag](s: String)(implicit p: Parsers[T]): Unit = {
    s should parseIn[T] {
      case Cypher5JavaCc => _.withAnyFailure
      case _             => _.withoutErrors
    }
  }

  /**
   * @deprecated use [[parses]] and create ast with [[AstConstructionTestSupport]] instead.
   */
  @deprecated("Use methods from AstParsingTestBase", "-")
  def assertFailsWithMessage[T <: ASTNode : ClassTag](
    cypher: String,
    expectedMessage: String,
    failsOnlyJavaCC: Boolean = false
  )(implicit p: Parsers[T]): Unit = {
    if (failsOnlyJavaCC)
      cypher should parseIn[T] {
        case Cypher5JavaCc => _.withAnyFailure.withMessage(expectedMessage)
        case _             => _.withoutErrors
      }
    else
      cypher should notParse[T].withMessage(expectedMessage)
  }

  /**
   * @deprecated use [[parseIn]].
   */
  @deprecated("Use methods from AstParsingTestBase", "-")
  def assertFailsWithMessageStart[T <: ASTNode : ClassTag](
    cypher: String,
    expectedMessage: String,
    failsOnlyJavaCC: Boolean = false
  )(implicit p: Parsers[T]): Unit = {
    if (failsOnlyJavaCC)
      cypher should parseIn[T] {
        case Cypher5JavaCc => _.withAnyFailure.withMessageStart(expectedMessage)
        case _             => _.withoutErrors
      }
    else
      cypher should notParse[T].withMessageStart(expectedMessage)
  }

  /**
   * @deprecated use [[notParse]]
   */
  @deprecated("Use methods from AstParsingTestBase", "-")
  def assertFailsWithMessageContains[T <: ASTNode : ClassTag](
    cypher: String,
    expectedMessage: String
  )(implicit p: Parsers[T]): Unit = {
    cypher should notParse[T].withMessageContaining(expectedMessage)
  }

  /**
   * @deprecated use [[notParse]]
   */
  @deprecated("Use methods from AstParsingTestBase", "-")
  def assertFailsWithException[T <: ASTNode : ClassTag](
    cypher: String,
    expected: Exception
  )(implicit p: Parsers[T]): Unit = {
    cypher should notParse[T].similarTo(expected)
  }

  // If these are helpful, why not add them to AstConstructionTestSupport!
  final def id(id: String): Variable = varFor(id)
  final def lt(lhs: Expression, rhs: Expression): Expression = lessThan(lhs, rhs)
  final def lte(lhs: Expression, rhs: Expression): Expression = lessThanOrEqual(lhs, rhs)
  final def gt(lhs: Expression, rhs: Expression): Expression = greaterThan(lhs, rhs)
  final def gte(lhs: Expression, rhs: Expression): Expression = greaterThanOrEqual(lhs, rhs)
  final def eq(lhs: Expression, rhs: Expression): Expression = equals(lhs, rhs)
  final def ne(lhs: Expression, rhs: Expression): Expression = notEquals(lhs, rhs)

  class LegacyParse[T <: ASTNode : ClassTag](cypher: String) {
    def shouldVerify(assertion: T => Unit)(implicit p: Parsers[T]): Unit = cypher should parse[T].withAstLike(assertion)
    def shouldGive(expected: T)(implicit p: Parsers[T]): Unit = cypher should parseTo[T](expected)
    def shouldGive(expected: InputPosition => T)(implicit p: Parsers[T]): Unit = cypher should parseTo[T](expected(pos))
  }
}
