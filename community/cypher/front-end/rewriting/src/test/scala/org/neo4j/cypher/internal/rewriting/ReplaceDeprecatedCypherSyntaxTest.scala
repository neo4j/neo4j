/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.SignedHexIntegerLiteral
import org.neo4j.cypher.internal.rewriting.rewriters.replaceDeprecatedCypherSyntax
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ReplaceDeprecatedCypherSyntaxTest extends CypherFunSuite with AstConstructionTestSupport {

  private val rewriter = replaceDeprecatedCypherSyntax(Deprecations.deprecatedFeaturesIn4_X)

  test("should rewrite timestamp()") {
    val before = function("timestamp")

    val after = prop(function("datetime"), "epochMillis")
    rewriter(before) should equal(after)
  }

  test("should also rewrite TiMeStAmP()") {
    val before = function("TiMeStAmP")

    val after = prop(function("datetime"), "epochMillis")
    rewriter(before) should equal(after)
  }

  test("should rewrite 0X123 to 0x123") {
    val before = SignedHexIntegerLiteral("0X123")(InputPosition(0, 0, 0))

    val after = SignedHexIntegerLiteral("0x123")(InputPosition(0, 0, 0))
    rewriter(before) should equal(after)
  }

  test("should rewrite 0X9fff to 0x9fff") {
    val before = SignedHexIntegerLiteral("0X9fff")(InputPosition(13, 17, 19))

    val after = SignedHexIntegerLiteral("0x9fff")(InputPosition(13, 17, 19))
    rewriter(before) should equal(after)
  }
}
