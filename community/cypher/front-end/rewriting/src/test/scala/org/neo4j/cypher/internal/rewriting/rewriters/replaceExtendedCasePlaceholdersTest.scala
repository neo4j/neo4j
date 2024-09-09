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
import org.neo4j.cypher.internal.expressions.CaseExpression
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.Inside

class replaceExtendedCasePlaceholdersTest extends CypherFunSuite with AstConstructionTestSupport with Inside {

  def rewriterUnderTest: Rewriter =
    replaceExtendedCasePlaceholders.getRewriter(null, new AnonymousVariableNameGenerator())

  test("case expression with placeholders is rewritten to anonymous variables") {
    val input = caseExpression()
      .withCandidate(literal("a"))
      .withExtendedCase(equals(_, literal("b")), literal("c"))
      .withExtendedCase(equals(_, literal("d")), literal("e"))

    val output = input.endoRewrite(rewriterUnderTest)

    inside(output) {
      case CaseExpression(candidate, Some(varName), alternatives, _) =>
        candidate shouldBe input.candidate
        alternatives shouldBe Vector(
          equals(varName, literal("b")) -> literal("c"),
          equals(varName, literal("d")) -> literal("e")
        )
    }
  }

  test("nested case expression with placeholders is recursively rewritten to different anonymous variables") {
    val innerInput = caseExpression()
      .withCandidate(literal("a"))
      .withExtendedCase(equals(_, literal("b")), literal("c"))
      .withExtendedCase(equals(_, literal("d")), literal("e"))

    val outerInput = caseExpression()
      .withCandidate(innerInput)
      .withExtendedCase(equals(_, literal("f")), literal("g"))
      .withExtendedCase(equals(_, literal("h")), literal("i"))

    val outerOutput = outerInput.endoRewrite(rewriterUnderTest)

    inside(outerOutput) {
      case CaseExpression(
          Some(CaseExpression(_, Some(innerVarName), innerAlternatives, _)),
          Some(outerVarName),
          outerAlternatives,
          _
        ) =>
        innerVarName should not be outerVarName

        innerAlternatives shouldBe Vector(
          equals(innerVarName, literal("b")) -> literal("c"),
          equals(innerVarName, literal("d")) -> literal("e")
        )

        outerAlternatives shouldBe Vector(
          equals(outerVarName, literal("f")) -> literal("g"),
          equals(outerVarName, literal("h")) -> literal("i")
        )
    }
  }

}
