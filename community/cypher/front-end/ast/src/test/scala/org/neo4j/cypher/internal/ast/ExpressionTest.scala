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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.functions.Nodes
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ExpressionTest extends CypherFunSuite with AstConstructionTestSupport {

  test("should compute dependencies of simple expressions") {
    varFor("a").dependencies should equal(Set(varFor("a")))
    literalInt(1).dependencies should equal(Set())
  }

  test("should compute dependencies of composite expressions") {
    add(varFor("a"), subtract(literalInt(1), varFor("b"))).dependencies should equal(Set(varFor("a"), varFor("b")))
  }

  test("should compute dependencies for filtering expressions") {
    // [x IN (n)-->(k) | head(nodes(x)) ]
    val pat: RelationshipsPattern = RelationshipsPattern(
      RelationshipChain(
        NodePattern(Some(varFor("n")), None, None, None) _,
        RelationshipPattern(None, None, None, None, None, SemanticDirection.OUTGOING) _,
        NodePattern(Some(varFor("k")), None, None, None) _
      ) _
    ) _
    val expr: Expression = listComprehension(
      varFor("x"),
      PatternExpression(pat)(Some(Set(varFor("x"))), Some(Set(varFor("n"), varFor("k")))),
      None,
      Some(function("head", function("nodes", varFor("x"))))
    )

    expr.dependencies should equal(Set(varFor("n"), varFor("k")))
  }

  test("should compute dependencies for nested filtering expressions") {
    // [x IN (n)-->(k) | [y IN [1,2,3] | y] ]
    val pat: RelationshipsPattern = RelationshipsPattern(
      RelationshipChain(
        NodePattern(Some(varFor("n")), None, None, None) _,
        RelationshipPattern(None, None, None, None, None, SemanticDirection.OUTGOING) _,
        NodePattern(Some(varFor("k")), None, None, None) _
      ) _
    ) _
    val innerExpr: Expression = listComprehension(
      varFor("y"),
      listOfInt(1, 2, 3),
      None,
      Some(varFor("y"))
    )
    val expr: Expression = listComprehension(
      varFor("x"),
      PatternExpression(pat)(Some(Set(varFor("x"), varFor("y"))), Some(Set(varFor("n"), varFor("k")))),
      None,
      Some(innerExpr)
    )

    expr.dependencies should equal(Set(varFor("n"), varFor("k")))
  }

  test("occurrences of simple expression") {
    val a1 = varFor("a")
    val a2 = varFor("a")
    val exp = listOf(a1, varFor("b"), a2)
    exp.occurrences(varFor("a")) should equal(Set(
      Ref(a1),
      Ref(a2)
    ))
  }

  test("occurrences should find occurrences in nested scope expressions") {
    val a1 = varFor("a")
    val a2 = varFor("a")
    val a3 = varFor("a")
    val a4 = varFor("a")
    val listComp = listComprehension(
      varFor("y"),
      listOf(a1),
      Some(equals(prop(a2, "prop"), literalInt(5))),
      Some(prop(a3, "prop"))
    )

    val exp = listOf(a4, listComp)
    exp.occurrences(varFor("a")) should equal(Set(
      Ref(a1),
      Ref(a2),
      Ref(a3),
      Ref(a4)
    ))
  }

  test("occurrences should not find shadowed occurrences in nested scope expressions") {
    val a1 = varFor("a")
    val a2 = varFor("a")
    val shadowed_a1 = varFor("a")
    val shadowed_a2 = varFor("a")
    val shadowed_a3 = varFor("a")
    val listComp = listComprehension(
      shadowed_a1,
      listOf(a1),
      Some(equals(prop(shadowed_a2, "prop"), literalInt(5))),
      Some(prop(shadowed_a3, "prop"))
    )

    val exp = listOf(a2, listComp)
    exp.occurrences(varFor("a")) should equal(Set(
      Ref(a1),
      Ref(a2)
    ))
  }

  test("occurrences should not find shadowed occurrences in nested scope expressions (pattern comprehension)") {
    val a1 = varFor("a")
    val shadowed_a1 = varFor("a")
    val shadowed_a2 = varFor("a")
    val shadowed_a3 = varFor("a")

    // [a = (n)-[r]-(m) | nodes(a) ]
    val patCom = patternComprehension(
      relationshipChain(
        nodePat(Some("n")),
        relPat(Some("r")),
        nodePat(Some("m"))
      ),
      Nodes(shadowed_a2)(pos)
    ).copy(namedPath = Some(shadowed_a1))(pos, null, null)
      .withComputedScopeDependencies(Set.empty)
      .withComputedIntroducedVariables(Set(shadowed_a3))

    val exp = listOf(a1, patCom)
    exp.occurrences(varFor("a")) should equal(Set(
      Ref(a1)
    ))
  }

  test("occurrences should find occurrences in scope dependencies of ExpressionWithComputedDependencies") {
    val a1 = varFor("a")
    val a2 = varFor("a")
    val a3 = varFor("a")
    // [(n)-[r]-(m) | a.prop ]
    val patCom = patternComprehension(
      relationshipChain(
        nodePat(Some("n")),
        relPat(Some("r")),
        nodePat(Some("m"))
      ),
      prop(a1, "prop")
    ).withComputedScopeDependencies(Set(a2))
      .withComputedIntroducedVariables(Set.empty)

    val exp = listOf(a3, patCom)
    exp.occurrences(varFor("a")) should equal(Set(
      Ref(a1),
      Ref(a2),
      Ref(a3)
    ))
  }

  test("replaceAllOccurrencesBy of simple expression") {
    val exp = listOf(varFor("a"), varFor("b"), varFor("a"))
    exp.replaceAllOccurrencesBy(varFor("a"), varFor("b")) should equal(
      listOf(varFor("b"), varFor("b"), varFor("b"))
    )
  }

  test("replaceAllOccurrencesBy should replace occurrences in nested scope expressions") {
    val listComp = listComprehension(
      varFor("y"),
      listOf(varFor("a")),
      Some(equals(prop(varFor("a"), "prop"), literalInt(5))),
      Some(prop(varFor("a"), "prop"))
    )

    val exp = listOf(varFor("a"), listComp)
    exp.replaceAllOccurrencesBy(varFor("a"), varFor("b")) should equal(
      listOf(
        varFor("b"),
        listComprehension(
          varFor("y"),
          listOf(varFor("b")),
          Some(equals(prop(varFor("b"), "prop"), literalInt(5))),
          Some(prop(varFor("b"), "prop"))
        )
      )
    )
  }

  test("replaceAllOccurrencesBy should not replace shadowed occurrences in nested scope expressions") {
    val listComp = listComprehension(
      varFor("a"),
      listOf(varFor("a")),
      Some(equals(prop(varFor("a"), "prop"), literalInt(5))),
      Some(prop(varFor("a"), "prop"))
    )

    val exp = listOf(varFor("a"), listComp)
    exp.replaceAllOccurrencesBy(varFor("a"), varFor("b")) should equal(
      listOf(
        varFor("b"),
        listComprehension(
          varFor("a"),
          listOf(varFor("b")),
          Some(equals(prop(varFor("a"), "prop"), literalInt(5))),
          Some(prop(varFor("a"), "prop"))
        )
      )
    )
  }

  test(
    "replaceAllOccurrencesBy should not replace shadowed occurrences in nested scope expressions (pattern comprehension)"
  ) {
    // [a = (n)-[r]-(m) | nodes(a) ]
    val patCom = patternComprehension(
      relationshipChain(
        nodePat(Some("n")),
        relPat(Some("r")),
        nodePat(Some("m"))
      ),
      Nodes(varFor("a"))(pos)
    ).copy(namedPath = Some(varFor("a")))(pos, null, null)
      .withComputedScopeDependencies(Set.empty)
      .withComputedIntroducedVariables(Set(varFor("a")))

    val exp = listOf(varFor("a"), patCom)
    exp.replaceAllOccurrencesBy(varFor("a"), varFor("b")) should equal(
      listOf(
        varFor("b"),
        patternComprehension(
          relationshipChain(
            nodePat(Some("n")),
            relPat(Some("r")),
            nodePat(Some("m"))
          ),
          Nodes(varFor("a"))(pos)
        ).copy(namedPath = Some(varFor("a")))(pos, null, null)
      )
    )
  }

  test(
    "replaceAllOccurrencesBy should replace occurrences in scope dependencies of ExpressionWithComputedDependencies"
  ) {
    // [(n)-[r]-(m) | a.prop ]
    val patCom = patternComprehension(
      relationshipChain(
        nodePat(Some("n")),
        relPat(Some("r")),
        nodePat(Some("b"))
      ),
      prop(varFor("a"), "prop")
    ).withComputedScopeDependencies(Set(varFor("a")))
      .withComputedIntroducedVariables(Set.empty)

    val result = patCom.replaceAllOccurrencesBy(varFor("a"), varFor("b"))
    result should equal(
      patternComprehension(
        relationshipChain(
          nodePat(Some("n")),
          relPat(Some("r")),
          nodePat(Some("b"))
        ),
        prop(varFor("b"), "prop")
      )
    )
    result.asInstanceOf[PatternComprehension].scopeDependencies should equal(
      Set(varFor("b"))
    )
  }

  test("replaceAllOccurrencesBy accepts call-by-name parameter") {
    val list = listOf(varFor("a"), varFor("a"))
    var i = 0
    def replacement = { i += 1; varFor(s"var$i") }

    list.replaceAllOccurrencesBy(varFor("a"), replacement) should equal(
      listOf(varFor("var1"), varFor("var2"))
    )
  }
}
