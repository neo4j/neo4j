/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.nio.charset.StandardCharsets

import org.neo4j.cypher.internal.parser.ParserFixture.parser
import org.neo4j.cypher.internal.rewriting.rewriters.sensitiveLiteralReplacement
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SensitiveLiteralReplacementTest extends CypherFunSuite {

  val passwordBytes = "password".getBytes(StandardCharsets.UTF_8)
  val currentBytes = "current".getBytes(StandardCharsets.UTF_8)

  test("should extract password") {
    assertRewrite("CREATE USER foo SET PASSWORD 'password'", "CREATE USER foo SET PASSWORD $`  AUTOSTRING0`", Map("  AUTOSTRING0" -> passwordBytes))
  }

  test("should extract password in the presence of other vars") {
    assertRewrite("CREATE USER $foo SET PASSWORD 'password'", "CREATE USER $foo SET PASSWORD $`  AUTOSTRING0`", Map("  AUTOSTRING0" -> passwordBytes))
  }

  test("should extract nothing if password is already parameterised") {
    assertRewrite("CREATE USER $foo SET PASSWORD $password", "CREATE USER $foo SET PASSWORD $password", Map())
  }

  test("should extract two passwords") {
    assertRewrite("ALTER CURRENT USER SET PASSWORD FROM 'current' TO 'password'", "ALTER CURRENT USER SET PASSWORD FROM $`  AUTOSTRING1` TO $`  AUTOSTRING0`",
      Map("  AUTOSTRING1" -> currentBytes, "  AUTOSTRING0" -> passwordBytes))
  }

  test("should ignore queries with no passwords") {
    assertRewrite("MATCH (n:Node{name:'foo'}) RETURN n", "MATCH (n:Node{name:'foo'}) RETURN n", Map())
  }

  private def assertRewrite(originalQuery: String, expectedQuery: String, replacements: Map[String, Any]) {
    val exceptionFactory = OpenCypherExceptionFactory(None)
    val original = parser.parse(originalQuery, exceptionFactory)
    val expected = parser.parse(expectedQuery, exceptionFactory)

    val (rewriter, replacedLiterals) = sensitiveLiteralReplacement(original)

    val result = original.rewrite(rewriter)
    assert(result === expected)

    replacements.foreach {
      case (k, v: Array[Byte]) =>
        replacedLiterals(k) should equal(v)
    }
  }
}
