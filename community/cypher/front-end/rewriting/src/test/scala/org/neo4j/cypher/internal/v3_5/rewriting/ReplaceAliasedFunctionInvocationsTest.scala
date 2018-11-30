/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

  test("should rewrite toInt()") {
    val before = FunctionInvocation(FunctionName("toInt")(pos), literalInt(1))(pos)

    rewriter(before) should equal(before.copy(functionName = FunctionName("toInteger")(pos))(pos))
  }

  test("doesn't touch toInteger()") {
    val before = FunctionInvocation(FunctionName("toInteger")(pos), literalInt(1))(pos)

    rewriter(before) should equal(before)
  }

  test("should rewrite timestamp()") {
    val before = FunctionInvocation(FunctionName("timestamp")(pos), distinct = false, IndexedSeq.empty)(pos)

    val after =
      Property(
        FunctionInvocation(Namespace()(pos), FunctionName("datetime")(pos), distinct = false, IndexedSeq.empty)(pos),
        PropertyKeyName("epochMillis")(pos))(pos)
    rewriter(before) should equal(after)
  }

  test("should also rewrite TiMeStAmP()") {
    val before = FunctionInvocation(FunctionName("TiMeStAmP")(pos), distinct = false, IndexedSeq.empty)(pos)

    val after =
      Property(
        FunctionInvocation(Namespace()(pos), FunctionName("datetime")(pos), distinct = false, IndexedSeq.empty)(pos),
        PropertyKeyName("epochMillis")(pos))(pos)
    rewriter(before) should equal(after)
  }

  test("should rewrite extract() in V2") {
    val scope = ExtractScope(varFor("a"), None, None)(pos)
    val before = ExtractExpression(scope, literalFloat(3.0))(pos)
    val expected = ListComprehension(scope, literalFloat(3.0))(pos)

    replaceAliasedFunctionInvocations(Deprecations.V1)(before) should equal(before)
    replaceAliasedFunctionInvocations(Deprecations.V2)(before) should equal(expected)
  }

  test("should rewrite filter() in V2") {
    val scopePosition = InputPosition(30, 1, 31)
    val scope = FilterScope(varFor("a"), Some(TRUE))(scopePosition)
    val before = FilterExpression(scope, literalFloat(3.0))(pos)
    val expected = ListComprehension(ExtractScope(varFor("a"), Some(TRUE), None)(scopePosition), literalFloat(3.0))(pos)

    replaceAliasedFunctionInvocations(Deprecations.V1)(before) should equal(before)
    replaceAliasedFunctionInvocations(Deprecations.V2)(before) should equal(expected)
  }

}
