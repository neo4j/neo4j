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
package org.neo4j.cypher.internal.rewriting

import org.neo4j.cypher.internal.ast.CreateUser
import org.neo4j.cypher.internal.ast.SetOwnPassword
import org.neo4j.cypher.internal.ast.factory.neo4j.JavaCCParser
import org.neo4j.cypher.internal.expressions.AutoExtractedParameter
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.SensitiveStringLiteral
import org.neo4j.cypher.internal.rewriting.rewriters.sensitiveLiteralReplacement
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.matchers.Matcher

import java.nio.charset.StandardCharsets

class SensitiveLiteralReplacementTest extends CypherFunSuite {

  private val exceptionFactory = OpenCypherExceptionFactory(None)

  private val passwordBytes = "password".getBytes(StandardCharsets.UTF_8)
  private val currentBytes = "current".getBytes(StandardCharsets.UTF_8)

  test("should extract password") {
    val expectedPattern: Matcher[Any] =
      matchPattern { case CreateUser(_, _, AutoExtractedParameter(_, _, _), _, _) => }

    assertRewrite("CREATE USER foo SET PASSWORD 'password'", expectedPattern, Map("  AUTOSTRING0" -> passwordBytes))
  }

  test("should extract password in the presence of other vars") {
    val expectedPattern: Matcher[Any] =
      matchPattern { case CreateUser(_, _, AutoExtractedParameter(_, _, _), _, _) => }

    assertRewrite("CREATE USER $foo SET PASSWORD 'password'", expectedPattern, Map("  AUTOSTRING0" -> passwordBytes))
  }

  test("should extract nothing if password is already parameterised") {
    val expectedPattern: Matcher[Any] = matchPattern { case CreateUser(_, _, _: ExplicitParameter, _, _) => }

    assertRewrite("CREATE USER $foo SET PASSWORD $password", expectedPattern, Map())
  }

  test("should extract two passwords") {
    val expectedPattern: Matcher[Any] = matchPattern {
      case SetOwnPassword(
          AutoExtractedParameter(_, _, _),
          AutoExtractedParameter(_, _, _)
        ) =>
    }

    assertRewrite(
      "ALTER CURRENT USER SET PASSWORD FROM 'current' TO 'password'",
      expectedPattern,
      Map("  AUTOSTRING1" -> currentBytes, "  AUTOSTRING0" -> passwordBytes)
    )
  }

  test("should ignore queries with no passwords") {
    val query = "MATCH (n:Node{name:'foo'}) RETURN n"

    val expected = JavaCCParser.parse(query, exceptionFactory)
    val expectedPattern: Matcher[Any] = matchPattern { case `expected` => }

    assertRewrite(query, expectedPattern, Map())
  }

  private def assertRewrite(
    originalQuery: String,
    matchExpectedPattern: Matcher[Any],
    replacements: Map[String, Any]
  ): Unit = {
    val original = JavaCCParser.parse(originalQuery, exceptionFactory)

    val (rewriter, replacedParameters) = sensitiveLiteralReplacement(original)
    val replacedLiterals = replacedParameters.map {
      case (k, v) => k.name -> v
    }

    val result = original.rewrite(rewriter)
    result should matchExpectedPattern

    replacements.foreach {
      case (k, v: Array[Byte]) =>
        replacedLiterals(k) should equal(SensitiveStringLiteral(v)(InputPosition.NONE, InputPosition.NONE))
      case (k, v) => throw new IllegalStateException(s"Unknown value: $v for key: $k")
    }
  }
}
