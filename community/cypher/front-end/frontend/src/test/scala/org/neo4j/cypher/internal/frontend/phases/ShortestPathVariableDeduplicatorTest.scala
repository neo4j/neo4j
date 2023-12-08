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
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.flattenBooleanOperators
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ShortestPathVariableDeduplicatorTest extends CypherFunSuite
    with AstConstructionTestSupport
    with RewritePhaseTest {
  override def rewriterPhaseUnderTest: Transformer[BaseContext, BaseState, BaseState] = ShortestPathVariableDeduplicator

  override def semanticFeatures: Seq[SemanticFeature] = Seq(SemanticFeature.GpmShortestPath, SemanticFeature.MatchModes)

  override def preProcessPhase(features: SemanticFeature*): Transformer[BaseContext, BaseState, BaseState] =
    super.preProcessPhase(features: _*) andThen
      flattenBooleanOperators andThen
      // flattenBooleanOperators invalidates SemanticInformation
      SemanticAnalysis(false, semanticFeatures.head)

  test("should rewrite repeated interior nodes and leave exterior nodes") {
    assertRewritten(
      "MATCH SHORTEST 1 (a)-->*(b)-->*(b)-->*(b)-->*(a) RETURN a",
      "MATCH SHORTEST 1 ((a)-->*(b)-->*(`  b@0`)-->*(`  b@1`)-->*(a) WHERE `  b@0` = b AND `  b@1` = b) RETURN a"
    )
    assertRewritten(
      "MATCH SHORTEST 1 (a)-->*(a)-->*(c)-->*(a)-->*(c)-->*(c) RETURN a",
      "MATCH SHORTEST 1 ((a)-->*(`  a@0`)-->*(`  c@2`)-->*(`  a@1`)-->*(`  c@3`)-->*(c) WHERE `  a@0` = a AND `  a@1` = a AND `  c@2` = c AND `  c@3` = c) RETURN a"
    )
    assertRewritten(
      "MATCH SHORTEST 1 (a)-->*(a)-->*(a)-->*(a)-->*(a) RETURN a",
      "MATCH SHORTEST 1 ((a)-->*(`  a@0`)-->*(`  a@1`)-->*(`  a@2`)-->*(a) WHERE `  a@0` = a AND `  a@1` = a AND `  a@2` = a) RETURN a"
    )
  }

  ignore("should rewrite repeated interior relationship under REPEATED ELEMENTS MATCH mode") {
    assertRewritten(
      "MATCH REPEATABLE ELEMENTS ANY SHORTEST ((d)-[b]->(e)-[b]->(f)) RETURN b",
      "MATCH REPEATABLE ELEMENTS ANY SHORTEST ((d)-[b]->(e)-[`  b@0`]->(f) WHERE `  b@0` = b) RETURN b"
    )
    assertRewritten(
      "MATCH REPEATABLE ELEMENTS ANY SHORTEST ((d)-[b*]->(e)-[b*]->(f)) RETURN b",
      "MATCH REPEATABLE ELEMENTS ANY SHORTEST ((d)-[b*]->(e)-[`  b@0`*]->(f) WHERE `  b@0` = b) RETURN b"
    )
  }

  test("should integrate rewritten predicates with existing ones") {
    assertRewritten(
      "MATCH SHORTEST 1 ((a)-->*(b)-->*(a)-->*(b) WHERE a.prop = 5) RETURN a",
      "MATCH SHORTEST 1 ((a)-->*(`  b@0`)-->*(`  a@1`)-->*(b) WHERE  a.prop = 5 AND `  b@0` = b AND `  a@1` = a) RETURN a"
    )
  }

  test("should rewrite repeated nodes in QPPs") {
    assertRewritten(
      "MATCH SHORTEST 1 (a)-->() ((b)--(c)--(b))+ ()-->(d) RETURN a",
      "MATCH SHORTEST 1 (a)-->() ((b)--(c)--(`  b@0`) WHERE `  b@0` = b)+ ()-->(d) RETURN a"
    )
  }

  // Assertion: node inside QPP cannot appear as a singleton before

  test("should rewrite nodes bound in previous clauses") {
    assertRewritten(
      "MATCH (b:User) MATCH ANY SHORTEST ((b)-->+(b)-->+(c)) RETURN b",
      "MATCH (b:User) MATCH ANY SHORTEST ((b)-->+(`  b@0`)-->+(c) WHERE `  b@0` = b) RETURN b"
    )
    assertRewritten(
      "MATCH (b) RETURN EXISTS { SHORTEST 1 (b)-->+(b)-->+(c) } AS exists",
      "MATCH (b) RETURN EXISTS { SHORTEST 1 ((b)-->+(`  b@0`)-->+(c) WHERE `  b@0` = b) } AS exists"
    )
    assertRewritten(
      "MATCH (b) WHERE EXISTS { SHORTEST 1 (b)-->+(b)-->+(c) } RETURN b",
      "MATCH (b) WHERE EXISTS { SHORTEST 1 ((b)-->+(`  b@0`)-->+(c) WHERE `  b@0` = b) } RETURN b"
    )
    assertRewritten(
      "MATCH (b:User) MATCH ANY SHORTEST ((a)-->+(b)-->+(c)) RETURN b",
      "MATCH (b:User) MATCH ANY SHORTEST ((a)-->+(`  b@0`)-->+(c) WHERE `  b@0` = b) RETURN b"
    )
    assertRewritten(
      "MATCH (b) RETURN EXISTS { SHORTEST 1 (a)-->+(b)-->+(c) } AS exists",
      "MATCH (b) RETURN EXISTS { SHORTEST 1 ((a)-->+(`  b@0`)-->+(c) WHERE `  b@0` = b) } AS exists"
    )
    assertRewritten(
      "MATCH (b) WHERE EXISTS { SHORTEST 1 (a)-->+(b)-->+(c) } RETURN b",
      "MATCH (b) WHERE EXISTS { SHORTEST 1 ((a)-->+(`  b@0`)-->+(c) WHERE `  b@0` = b) } RETURN b"
    )
  }

  test("should rewrite relationships bound in previous clauses") {
    // We have to use AST for the expected assertion in here,
    // since the preparatory phase generated uniqueness predicates (for both original and expected),
    // but the rewriter does actually not rewrite those.

    assertRewritten(
      "MATCH ()-[r]->() MATCH ANY SHORTEST (()-[r]->()-[*]->()) RETURN r",
      // "MATCH ()-[r]->() MATCH ANY SHORTEST (()-[`  r@0`]->()-[*]->() WHERE `  r@0` = r) RETURN r"
      singleQuery(
        match_(
          relationshipChain(
            nodePat(Some("  UNNAMED0")),
            relPat(Some("r")),
            nodePat(Some("  UNNAMED1"))
          )
        ),
        match_shortest(
          anyShortestPathSelector(1),
          parenthesizedPath(
            relationshipChain(
              nodePat(Some("  UNNAMED2")),
              relPat(Some("  r@0")),
              nodePat(Some("  UNNAMED3")),
              relPat(Some("  UNNAMED4"), length = Some(None)),
              nodePat(Some("  UNNAMED5"))
            ),
            Some(ands(
              equals(varFor("  r@0"), varFor("r")),
              noneOfRels(varFor("r"), varFor("  UNNAMED4")),
              unique(varFor("  UNNAMED4")),
              varLengthLowerLimitPredicate("  UNNAMED4", 1)
            ))
          )
        ),
        return_(aliasedReturnItem(varFor("r")))
      )
    )
    assertRewritten(
      "MATCH (a)-[b*]->(c:User) MATCH ANY SHORTEST ((d)-[b*]->(e)-->(f)) RETURN b",
      // MATCH (a)-[b*]->(c:User) MATCH ANY SHORTEST ((d)-[`  b@0`*]->(e)-->(f) WHERE `  b@0` = b) RETURN b
      singleQuery(
        match_(
          relationshipChain(nodePat(Some("a")), relPat(Some("b"), length = Some(None)), nodePat(Some("c"))),
          where = Some(where(ands(hasLabels("c", "User"), unique(varFor("b")), varLengthLowerLimitPredicate("b", 1))))
        ),
        match_shortest(
          anyShortestPathSelector(1),
          parenthesizedPath(
            relationshipChain(
              nodePat(Some("d")),
              relPat(Some("  b@0"), length = Some(None)),
              nodePat(Some("e")),
              relPat(Some("  UNNAMED0")),
              nodePat(Some("f"))
            ),
            Some(ands(
              equals(varFor("  b@0"), varFor("b")),
              // these were not rewritten, which creates a suboptimal plan.
              // But we accept that.
              noneOfRels(varFor("  UNNAMED0"), varFor("b")),
              unique(varFor("b")),
              varLengthLowerLimitPredicate("b", 1)
            ))
          )
        ),
        return_(aliasedReturnItem(varFor("b")))
      )
    )
    assertRewritten(
      "MATCH (a)-[b*1..3]->(c:User) MATCH ANY SHORTEST ((d)-[b*2..4]->(e)-->(f)) RETURN b",
      // MATCH (a)-[b*1..3]->(c:User) MATCH ANY SHORTEST ((d)-[`  b@0`*2..4]->(e)-->(f) WHERE `  b@0` = b) RETURN b
      singleQuery(
        match_(
          relationshipChain(
            nodePat(Some("a")),
            relPat(Some("b"), length = Some(Some(range(Some(1), Some(3))))),
            nodePat(Some("c"))
          ),
          where = Some(where(ands(
            hasLabels("c", "User"),
            unique(varFor("b")),
            varLengthLowerLimitPredicate("b", 1),
            varLengthUpperLimitPredicate("b", 3)
          )))
        ),
        match_shortest(
          anyShortestPathSelector(1),
          parenthesizedPath(
            relationshipChain(
              nodePat(Some("d")),
              relPat(Some("  b@0"), length = Some(Some(range(Some(2), Some(4))))),
              nodePat(Some("e")),
              relPat(Some("  UNNAMED0")),
              nodePat(Some("f"))
            ),
            Some(ands(
              equals(varFor("  b@0"), varFor("b")),
              // these were not rewritten, which creates a suboptimal plan.
              // But we accept that.
              noneOfRels(varFor("  UNNAMED0"), varFor("b")),
              unique(varFor("b")),
              varLengthLowerLimitPredicate("b", 2),
              varLengthUpperLimitPredicate("b", 4)
            ))
          )
        ),
        return_(aliasedReturnItem(varFor("b")))
      )
    )
    assertRewritten(
      "MATCH (a)-[b*1..3]->(c:User) MATCH ANY SHORTEST ((d)-[b*2..4]->(e)-->(f)) WHERE size(b) < 5 RETURN b",
      // MATCH (a)-[b*1..3]->(c:User) MATCH ANY SHORTEST ((d)-[`  b@0`*2..4]->(e)-->(f) WHERE `  b@0` = b) WHERE size(b) < 5  RETURN b
      singleQuery(
        match_(
          relationshipChain(
            nodePat(Some("a")),
            relPat(Some("b"), length = Some(Some(range(Some(1), Some(3))))),
            nodePat(Some("c"))
          ),
          where = Some(where(ands(
            hasLabels("c", "User"),
            unique(varFor("b")),
            varLengthLowerLimitPredicate("b", 1),
            varLengthUpperLimitPredicate("b", 3)
          )))
        ),
        match_shortest(
          anyShortestPathSelector(1),
          parenthesizedPath(
            relationshipChain(
              nodePat(Some("d")),
              relPat(Some("  b@0"), length = Some(Some(range(Some(2), Some(4))))),
              nodePat(Some("e")),
              relPat(Some("  UNNAMED0")),
              nodePat(Some("f"))
            ),
            Some(ands(
              equals(varFor("  b@0"), varFor("b")),
              // these were not rewritten, which creates a suboptimal plan.
              // But we accept that.
              noneOfRels(varFor("  UNNAMED0"), varFor("b")),
              unique(varFor("b")),
              varLengthLowerLimitPredicate("b", 2),
              varLengthUpperLimitPredicate("b", 4)
            ))
          ),
          where = Some(where(
            lessThan(size(varFor("b")), literalInt(5))
          ))
        ),
        return_(aliasedReturnItem(varFor("b")))
      )
    )
  }

  test("should not rewrite if no shortest path") {
    assertNotRewritten("MATCH (n)--(n)--(n)--(n) RETURN *")
    assertNotRewritten("MATCH (n)--()((m)--(m))+()--(n) RETURN *")
  }

  test("should not rewrite repeated exterior nodes") {
    assertNotRewritten("MATCH SHORTEST 1 (n)--(m)--(o)--(n) RETURN *")
  }
}
