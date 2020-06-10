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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.ExtractExpression
import org.neo4j.cypher.internal.expressions.FilterExpression
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.ParameterWithOldSyntax
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.rewriting.rewriters.replaceDeprecatedCypherSyntax
import org.neo4j.cypher.internal.util.symbols
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class RemovedFeaturesTest extends CypherFunSuite with AstConstructionTestSupport {

  private val rewriter4_x = replaceDeprecatedCypherSyntax(Deprecations.removedFeaturesIn4_x)
  private val deprecatedNameMap4_x = Deprecations.removedFeaturesIn4_x.removedFunctionsRenames

  private val rewriter4_2 = replaceDeprecatedCypherSyntax(Deprecations.removedFeaturesIn4_2)
  private val deprecatedNameMap4_2 = Deprecations.removedFeaturesIn4_2.removedFunctionsRenames

  test("should rewrite removed function names regardless of casing") {
    for (deprecatedMap <- Seq(deprecatedNameMap4_x, deprecatedNameMap4_2)) {
      for ((oldName, newName) <- deprecatedMap) {
        rewriter4_x(function(oldName, varFor("arg"))) should equal(function(oldName, varFor("arg")).copy(functionName = FunctionName(newName)(pos))(pos))
        rewriter4_x(function(oldName.toLowerCase(), varFor("arg"))) should equal(function(newName, varFor("arg")))
        rewriter4_x(function(oldName.toUpperCase(), varFor("arg"))) should equal(function(newName, varFor("arg")))
      }
    }
  }

  test("should not touch new function names of regardless of casing") {
    for (deprecatedMap <- Seq(deprecatedNameMap4_x, deprecatedNameMap4_2)) {
      for (newName <- deprecatedMap.values) {
        rewriter4_x(function(newName, varFor("arg"))) should equal(function(newName, varFor("arg")))
        rewriter4_x(function(newName.toLowerCase(), varFor("arg"))) should equal(function(newName, varFor("arg")))
        rewriter4_x(function(newName.toUpperCase(), varFor("arg"))) should equal(function(newName, varFor("arg")))
      }
    }
  }

  test("should rewrite length of strings and collections to size regardless of casing") {
    val str = literalString("a string")
    val list = listOfInt(1, 2, 3)

    for (lengthFunc <- Seq("length", "LENGTH", "leNgTh")) {
      rewriter4_x(function(lengthFunc, str)) should equal(function("size", str))
      rewriter4_x(function(lengthFunc, list)) should equal(function("size", list))
    }
  }

  test("should rewrite filter to list comprehension") {
    val x = varFor("x")
    val list = listOfString("a", "aa", "aaa")
    val predicate = startsWith(x, literalString("aa"))

    // filter(x IN ["a", "aa", "aaa"] WHERE x STARTS WITH "aa") -> [x IN ["a", "aa", "aaa"] WHERE x STARTS WITH "aa"]
    val before = FilterExpression(x, list, Some(predicate))(pos)
    val after = listComprehension(x, list, Some(predicate), None)
    rewriter4_x(before) should equal(after)
  }

  test("should rewrite extract to list comprehension") {
    val x = varFor("x")
    val list = listOfString("a", "aa", "aaa")
    val extractExpression = function("size", x)

    // extract(x IN ["a", "aa", "aaa"] | size(x)) -> [x IN ["a", "aa", "aaa"] | size(x)]
    val before = ExtractExpression(x, list, None, Some(extractExpression))(pos)
    val after = listComprehension(x, list, None, Some(extractExpression))
    rewriter4_x(before) should equal(after)
  }

  test("should rewrite old parameter syntax") {
    val before = ParameterWithOldSyntax("param", symbols.CTString)(pos)

    val after = parameter("param", symbols.CTString)
    rewriter4_x(before) should equal(after)
  }

  //noinspection RedundantDefaultArgument
  test("should rewrite legacy type separator") {
    val types = Seq(RelTypeName("A")(pos), RelTypeName("B")(pos))
    val beforeVariable = RelationshipPattern(Some(varFor("a")), types, None, None, SemanticDirection.BOTH, legacyTypeSeparator = true)(pos)
    val beforeVarlength = RelationshipPattern(None, types, Some(None), None, SemanticDirection.BOTH, legacyTypeSeparator = true)(pos)
    val beforeProperties = RelationshipPattern(None, types, None, Some(varFor("x")), SemanticDirection.BOTH, legacyTypeSeparator = true)(pos)

    val afterVariable = RelationshipPattern(Some(varFor("a")), types, None, None, SemanticDirection.BOTH, legacyTypeSeparator = false)(pos)
    val afterVarlength = RelationshipPattern(None, types, Some(None), None, SemanticDirection.BOTH, legacyTypeSeparator = false)(pos)
    val afterProperties = RelationshipPattern(None, types, None, Some(varFor("x")), SemanticDirection.BOTH, legacyTypeSeparator = false)(pos)

    rewriter4_x(beforeVariable) should equal(afterVariable)
    rewriter4_x(beforeVarlength) should equal(afterVarlength)
    rewriter4_x(beforeProperties) should equal(afterProperties)
  }

  test("4.2 rewriter should not rewrite things removed in 4.0 or 4.1") {
    val oldParam = ParameterWithOldSyntax("param", symbols.CTString)(pos)
    rewriter4_2(oldParam) should equal(oldParam)
  }
}
