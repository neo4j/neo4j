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
package org.neo4j.cypher.internal.ast.prettifier

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ExpressionStringifierTest extends CypherFunSuite with AstConstructionTestSupport {

  private val tests: Seq[(Expression, String)] = Seq(
    (
      PatternComprehension(
        namedPath = None,
        pattern = RelationshipsPattern(RelationshipChain(
          nodePat(Some("u")),
          RelationshipPattern(Some(varFor("r")), Some(labelRelTypeLeaf("FOLLOWS")), None, None, None, OUTGOING)(pos),
          nodePat(Some("u2"))
        )(pos))(pos),
        predicate = Some(hasLabels("u2", "User")),
        projection = prop("u2", "id")
      )(
        pos,
        computedIntroducedVariables = Some(Set(varFor("u"), varFor("u2"))),
        computedScopeDependencies = Some(Set(varFor("r")))
      ),
      "[(u)-[r:FOLLOWS]->(u2) WHERE u2:User | u2.id]"
    ),
    (
      PatternComprehension(
        namedPath = None,
        pattern = RelationshipsPattern(RelationshipChain(
          nodePat(Some("u")),
          RelationshipPattern(Some(varFor("r")), Some(labelRelTypeLeaf("FOLLOWS")), None, None, None, OUTGOING)(pos),
          nodePat(Some("u2"), Some(labelLeaf("User")))
        )(pos))(pos),
        predicate = None,
        projection = prop("u2", "id")
      )(
        pos,
        computedIntroducedVariables = Some(Set(varFor("u"), varFor("u2"))),
        computedScopeDependencies = Some(Set(varFor("r")))
      ),
      "[(u)-[r:FOLLOWS]->(u2:User) | u2.id]"
    )
  )

  private val stringifier = ExpressionStringifier()

  for (((expr, expectedResult), idx) <- tests.zipWithIndex) {
    test(s"[$idx] should produce $expectedResult") {
      withClue(expr) {
        lazy val stringifiedExpr = stringifier(expr)
        noException should be thrownBy stringifiedExpr
        stringifiedExpr shouldBe expectedResult
      }
    }
  }
}
