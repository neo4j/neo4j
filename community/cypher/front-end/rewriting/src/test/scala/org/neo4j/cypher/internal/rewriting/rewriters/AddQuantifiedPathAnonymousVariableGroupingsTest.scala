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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.QuantifiedPath
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class AddQuantifiedPathAnonymousVariableGroupingsTest extends CypherFunSuite with AstConstructionTestSupport {

  private def rewrite(qpp: QuantifiedPath): QuantifiedPath = {
    val anonymousVariableNameGenerator = new AnonymousVariableNameGenerator()
    qpp.endoRewrite(inSequence(
      nameAllPatternElements(anonymousVariableNameGenerator),
      AddQuantifiedPathAnonymousVariableGroupings.instance
    ))
  }

  private def anonVar(n: Int): String = AnonymousVariableNameGenerator.anonymousVarName(n)

  private def testGroupingsRewriter(
    qpp: QuantifiedPath,
    before: Set[String],
    after: Set[String]
  ): Unit = {
    qpp.variableGroupings shouldBe before.map(v => variableGrouping(v, v))
    rewrite(qpp).variableGroupings shouldBe after.map(v => variableGrouping(v, v))
  }

  test("(()-[]->())+") {
    val qpp = quantifiedPath(
      relChain = relationshipChain(nodePat(), relPat(), nodePat()),
      quantifier = plusQuantifier
    )

    testGroupingsRewriter(
      qpp,
      before = Set.empty,
      after = Set(anonVar(0), anonVar(1), anonVar(2))
    )
  }

  test("((a)-[]->(b))+") {
    val qpp = quantifiedPath(
      relChain = relationshipChain(nodePat(Some("a")), relPat(), nodePat(Some("b"))),
      quantifier = plusQuantifier
    )

    testGroupingsRewriter(
      qpp,
      before = Set("a", "b"),
      after = Set("a", anonVar(0), "b")
    )
  }

  test("((a)-[rel]->(b))+") {
    val qpp = quantifiedPath(
      relChain = relationshipChain(nodePat(Some("a")), relPat(Some("rel")), nodePat(Some("b"))),
      quantifier = plusQuantifier
    )

    testGroupingsRewriter(
      qpp,
      before = Set("a", "rel", "b"),
      after = Set("a", "rel", "b")
    )
  }
}
