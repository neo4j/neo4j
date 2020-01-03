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
package org.neo4j.cypher.internal.v3_5.rewriting

import org.neo4j.cypher.internal.v3_5.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.rewriting.rewriters.replaceAliasedFunctionInvocations
import org.neo4j.cypher.internal.v3_5.util.InputPosition
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class ReplaceAliasedFunctionInvocationsTest extends CypherFunSuite with AstConstructionTestSupport {

  private val rewriter = replaceAliasedFunctionInvocations(Deprecations.V1)
  private val deprecatedNameMap = Map(
    "toInt" -> "toInteger",
    "upper" -> "toUpper",
    "lower" -> "toLower",
    "rels" -> "relationships")

  test("should rewrite deprecated names regardless of casing") {
    for ((oldName, newName) <- deprecatedNameMap ) {
      rewriter(function(oldName, varFor("arg"))) should equal(function(oldName, deprecated = true, varFor("arg")).copy(functionName = FunctionName(newName)(pos))(pos))
      rewriter(function(oldName.toLowerCase(), varFor("arg"))) should equal(function(newName, deprecated = true, varFor("arg")))
      rewriter(function(oldName.toUpperCase(), varFor("arg"))) should equal(function(newName, deprecated = true, varFor("arg")))
    }
  }

  test("should not touch new names of regardless of casing") {
    for (newName <- deprecatedNameMap.values ) {
      rewriter(function(newName, varFor("arg"))) should equal(function(newName, varFor("arg")))
      rewriter(function(newName.toLowerCase(), varFor("arg"))) should equal(function(newName, deprecated = false, varFor("arg")))
      rewriter(function(newName.toUpperCase(), varFor("arg"))) should equal(function(newName, deprecated = false, varFor("arg")))
    }
  }

  test("should rewrite timestamp()") {
    val before = FunctionInvocation(FunctionName("timestamp")(pos), distinct = false, IndexedSeq.empty)(pos)

    val after =
      Property(
        FunctionInvocation(Namespace()(pos), FunctionName("datetime")(pos), distinct = false, IndexedSeq.empty, deprecated = true)(pos),
        PropertyKeyName("epochMillis")(pos))(pos)
    rewriter(before) should equal(after)
  }

  test("should also rewrite TiMeStAmP()") {
    val before = FunctionInvocation(FunctionName("TiMeStAmP")(pos), distinct = false, IndexedSeq.empty)(pos)

    val after =
      Property(
        FunctionInvocation(Namespace()(pos), FunctionName("datetime")(pos), distinct = false, IndexedSeq.empty, deprecated = true)(pos),
        PropertyKeyName("epochMillis")(pos))(pos)
    rewriter(before) should equal(after)
  }

  test("should rewrite extract() in V2") {
    val scope = ExtractScope(varFor("a"), None, None)(pos)
    val before = ExtractExpression(scope, literalFloat(3.0))(pos)
    val expected = ListComprehension(scope, literalFloat(3.0), generatedThroughRewrite = true)(pos)

    replaceAliasedFunctionInvocations(Deprecations.V1)(before) should equal(before)
    replaceAliasedFunctionInvocations(Deprecations.V2)(before) should equal(expected)
  }

  test("should rewrite filter() in V2") {
    val scopePosition = InputPosition(30, 1, 31)
    val scope = FilterScope(varFor("a"), Some(TRUE))(scopePosition)
    val before = FilterExpression(scope, literalFloat(3.0))(pos)
    val expected = ListComprehension(ExtractScope(varFor("a"), Some(TRUE), None)(scopePosition), literalFloat(3.0), generatedThroughRewrite = true)(pos)

    replaceAliasedFunctionInvocations(Deprecations.V1)(before) should equal(before)
    replaceAliasedFunctionInvocations(Deprecations.V2)(before) should equal(expected)
  }

}
