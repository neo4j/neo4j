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
package org.neo4j.cypher.internal.v4_0.rewriting

import org.neo4j.cypher.internal.v4_0.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.rewriting.rewriters.replaceDeprecatedCypherSyntax
import org.neo4j.cypher.internal.v4_0.util.symbols
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class RemovedFeaturesTest extends CypherFunSuite with AstConstructionTestSupport {

  private val rewriter = replaceDeprecatedCypherSyntax(Deprecations.removedFeaturesIn4_0)
  private val deprecatedNameMap = Deprecations.removedFeaturesIn4_0.removedFunctionsRenames

  test("should rewrite removed function names regardless of casing") {
    for ((oldName, newName) <- deprecatedNameMap ) {
      rewriter(function(oldName, varFor("arg"))) should equal(function(oldName, varFor("arg")).copy(functionName = FunctionName(newName)(pos))(pos))
      rewriter(function(oldName.toLowerCase(), varFor("arg"))) should equal(function(newName, varFor("arg")))
      rewriter(function(oldName.toUpperCase(), varFor("arg"))) should equal(function(newName, varFor("arg")))
    }
  }

  test("should not touch new function names of regardless of casing") {
    for (newName <- deprecatedNameMap.values ) {
      rewriter(function(newName, varFor("arg"))) should equal(function(newName, varFor("arg")))
      rewriter(function(newName.toLowerCase(), varFor("arg"))) should equal(function(newName, varFor("arg")))
      rewriter(function(newName.toUpperCase(), varFor("arg"))) should equal(function(newName, varFor("arg")))
    }
  }


  test("should rewrite length of strings and collections to size regardless of casing") {
    val str = literalString("a string")
    val list = listOfInt(1, 2, 3)

    for (lengthFunc <- Seq("length", "LENGTH", "leNgTh")) {
      rewriter(function(lengthFunc, str)) should equal(function("size", str))
      rewriter(function(lengthFunc, list)) should equal(function("size", list))
    }
  }

  test("should rewrite filter to list comprehension") {
    val x = varFor("x")
    val list = listOfString("a", "aa", "aaa")
    val predicate = startsWith(x, literalString("aa"))

    // filter(x IN ["a", "aa", "aaa"] WHERE x STARTS WITH "aa") -> [x IN ["a", "aa", "aaa"] WHERE x STARTS WITH "aa"]
    val before = FilterExpression(x, list, Some(predicate))(pos)
    val after = listComprehension(x, list, Some(predicate), None)
    rewriter(before) should equal(after)
  }

  test("should rewrite extract to list comprehension") {
    val x = varFor("x")
    val list = listOfString("a", "aa", "aaa")
    val extractExpression = function("size", x)

    // extract(x IN ["a", "aa", "aaa"] | size(x)) -> [x IN ["a", "aa", "aaa"] | size(x)]
    val before = ExtractExpression(x, list, None, Some(extractExpression))(pos)
    val after = listComprehension(x, list, None, Some(extractExpression))
    rewriter(before) should equal(after)
  }

  test("should rewrite old parameter syntax") {
    val before = ParameterWithOldSyntax("param", symbols.CTString)(pos)

    val after = parameter("param", symbols.CTString)
    rewriter(before) should equal(after)
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

    rewriter(beforeVariable) should equal(afterVariable)
    rewriter(beforeVarlength) should equal(afterVarlength)
    rewriter(beforeProperties) should equal(afterProperties)
  }
}
