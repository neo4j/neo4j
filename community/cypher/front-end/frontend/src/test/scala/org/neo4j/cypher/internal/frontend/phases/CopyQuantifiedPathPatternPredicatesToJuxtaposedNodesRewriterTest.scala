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
import org.neo4j.cypher.internal.ast.CollectExpression
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.expressions.PlusQuantifier
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.flattenBooleanOperators
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CopyQuantifiedPathPatternPredicatesToJuxtaposedNodesRewriterTest
    extends CypherFunSuite with AstConstructionTestSupport with RewritePhaseTest {

  override def preProcessPhase(features: SemanticFeature*): Transformer[BaseContext, BaseState, BaseState] =
    super.preProcessPhase(features: _*) andThen
      flattenBooleanOperators andThen
      Namespacer

  override def rewriterPhaseUnderTest: Transformer[BaseContext, BaseState, BaseState] =
    CopyQuantifiedPathPatternPredicatesToJuxtaposedNodes

  test("No predicates") {
    assertNotRewritten(
      """MATCH (a) ((n)-[r]->(m))+ (b)
        |RETURN *""".stripMargin
    )
  }

  test("Predicate with no dependency") {
    assertNotRewritten(
      """MATCH (a) ((n)-[r]->(m) WHERE 1 = true)+ (b)
        |RETURN *""".stripMargin
    )
  }

  test("Type predicates") {
    assertRewritten(
      """MATCH (a) ((n:N)-[r:R]->(m) WHERE m:M)+ (b)
        |RETURN *""".stripMargin,
      """MATCH (a) ((n:N)-[r:R]->(m:M))+ (b)
        |WHERE a:N AND b:M
        |RETURN *""".stripMargin
    )
  }

  test("Property predicates") {
    assertRewritten(
      """MATCH (a) ((n {p: 0})-[r {p: 1}]->(m) WHERE m.p > 0)+ (b)
        |RETURN *""".stripMargin,
      """MATCH (a) ((n {p: 0})-[r {p: 1}]->(m) WHERE m.p > 0)+ (b)
        |WHERE a.p = 0 AND b.p > 0
        |RETURN *""".stripMargin
    )
  }

  test("Anonymous variables and predicates") {
    assertRewritten(
      """MATCH () ((:N)-->(:M))+ ()
        |RETURN 1 AS s""".stripMargin,
      """MATCH (:N) ((:N)-->(:M))+ (:M)
        |RETURN 1 AS s""".stripMargin
    )
  }

  test("AND predicate") {
    assertRewritten(
      """MATCH (a) ((n)-[r]->(m) WHERE n.p = 0 AND m.p = 0)+ (b)
        |RETURN *""".stripMargin,
      """MATCH (a) ((n)-[r]->(m) WHERE n.p = 0 AND m.p = 0)+ (b)
        |WHERE a.p = 0 AND b.p = 0
        |RETURN *""".stripMargin
    )
  }

  test("Query with pre-existing post-filter predicate") {
    assertRewritten(
      """MATCH (a) ((n)-[r]->(m) WHERE n.p = 0)+ (b) WHERE a.i = 0
        |RETURN *""".stripMargin,
      """MATCH (a) ((n)-[r]->(m) WHERE n.p = 0)+ (b)
        |WHERE a.p = 0 AND a.i = 0
        |RETURN *""".stripMargin
    )
  }

  test("Predicate with single dependency") {
    assertRewritten(
      """MATCH (a) ((n)-[r]->(m) WHERE n.p = 0 OR n.p IS NOT NULL)+ (b)
        |RETURN *""".stripMargin,
      """MATCH (a) ((n)-[r]->(m) WHERE n.p = 0 OR n.p IS NOT NULL)+ (b)
        |WHERE a.p = 0 OR a.p IS NOT NULL
        |RETURN *""".stripMargin
    )
  }

  test("Predicate with multiple dependencies") {
    assertNotRewritten(
      """MATCH (a) ((n)-[r]->(m) WHERE n.p = 0 OR m.p = 0)+ (b)
        |RETURN *""".stripMargin
    )
  }

  test("Nested predicates") {
    assertRewritten(
      """MATCH (a) ((n)-[r]->(m) WHERE (n.p1 = 0 OR n.p2 = 0) AND (n.p3 = 0 OR r.p4 = 0))+ (b)
        |RETURN *""".stripMargin,
      """MATCH (a) ((n)-[r]->(m) WHERE (n.p1 = 0 OR n.p2 = 0) AND (n.p3 = 0 OR r.p4 = 0))+ (b)
        |WHERE a.p1 = 0 OR a.p2 = 0
        |RETURN *""".stripMargin
    )
  }

  test("QPP with intermediate node that has a predicate") {
    assertRewritten(
      """MATCH (a) ((n)-->(x)-->(m) WHERE n:N AND x:X AND m:M)+ (b)
        |RETURN *""".stripMargin,
      """MATCH (a:N) ((n:N)-->(x:X)-->(m:M))+ (b:M)
        |RETURN *""".stripMargin
    )
  }

  test("Multiple QPP with shared juxtaposed node") {
    assertRewritten(
      """MATCH (a) ((n)-[r]->(m) WHERE m.p = 0)+ (b) ((x)-[rr]->(z) WHERE x.p = 1)+ (c)
        |RETURN *""".stripMargin,
      """MATCH (a) ((n)-[r]->(m) WHERE m.p = 0)+ (b) ((x)-[rr]->(z) WHERE x.p = 1)+ (c)
        |WHERE b.p = 0 AND b.p = 1
        |RETURN *""".stripMargin
    )
  }

  test("Multiple pattern parts") {
    assertRewritten(
      """MATCH (a) ((n)-[r]->(m) WHERE n.p = 0)+ (b), (c) ((x)-[rr]->(y) WHERE x.p = 0)+ (d)
        |RETURN *""".stripMargin,
      """MATCH (a) ((n)-[r]->(m) WHERE n.p = 0)+ (b), (c) ((x)-[rr]->(y) WHERE x.p = 0)+ (d)
        |WHERE a.p = 0 AND c.p = 0
        |RETURN *""".stripMargin
    )
  }

  test("Multiple MATCH clauses") {
    assertRewritten(
      """MATCH (a) ((n:N)-[r]->(m))+ (b)
        |MATCH (c) ((x)-[rr]->(y:M))+ (d)
        |RETURN *""".stripMargin,
      """MATCH (a) ((n:N)-[r]->(m))+ (b)
        |WHERE a:N
        |MATCH (c) ((x)-[rr]->(y:M))+ (d)
        |WHERE d:M
        |RETURN *""".stripMargin
    )
  }

  test("Parenthesized path which is unwrappable") {
    assertRewritten(
      """MATCH ((a) ((n:N)-[r]->(m))+ (b))
        |RETURN *""".stripMargin,
      """MATCH (a) ((n:N)-[r]->(m))+ (b)
        |WHERE a:N
        |RETURN *""".stripMargin
    )
  }

  test("Parenthesized path which isn't unwrappable") {
    assertRewritten(
      """MATCH ANY SHORTEST ((a) ((n:N)-[r]->(m))+ (b) WHERE 1 = 2)
        |RETURN *""".stripMargin,
      """MATCH ANY SHORTEST ((a) ((n:N)-[r]->(m))+ (b) WHERE 1 = 2 AND a:N)
        |RETURN *""".stripMargin
    )
  }

  test("Scoped expression predicate with ambiguous name on local variable ") {
    assertRewritten(
      """MATCH (a) ((n)-[r]->(m) WHERE any(n IN n.list WHERE n > 0))+ (b)
        |RETURN 1 AS s""".stripMargin,
      singleQuery(
        match_(
          pathConcatenation(
            nodePat(Some("a")),
            quantifiedPath(
              relationshipChain(
                nodePat(Some("  n@0")),
                relPat(Some("  r@1")),
                nodePat(Some("  m@2"))
              ),
              PlusQuantifier()(pos),
              Some(anyInList(varFor("  n@3"), prop("  n@0", "list"), greaterThan(varFor("  n@3"), literalInt(0L)))),
              Set(
                variableGrouping("  n@0", "  n@4"),
                variableGrouping("  r@1", "  r@5"),
                variableGrouping("  m@2", "  m@6")
              )
            ),
            nodePat(Some("b"))
          ),
          where = Some(where(ands(
            anyInList(varFor("  n@3"), prop("a", "list"), greaterThan(varFor("  n@3"), literalInt(0L))),
            unique(varFor("  r@5"))
          )))
        ),
        returnLit((1, "s"))
      )
    )
  }

  test("Scoped expression predicate with ambiguous name on non-local variable") {
    assertRewritten(
      """MATCH (a) ((n)-[r]->(m) WHERE any(a IN n.list WHERE a > 0))+ (b)
        |RETURN 1 AS s""".stripMargin,
      singleQuery(
        match_(
          pathConcatenation(
            nodePat(Some("  a@0")),
            quantifiedPath(
              relationshipChain(
                nodePat(Some("  n@1")),
                relPat(Some("  r@2")),
                nodePat(Some("  m@3"))
              ),
              PlusQuantifier()(pos),
              Some(anyInList(varFor("  a@4"), prop("  n@1", "list"), greaterThan(varFor("  a@4"), literalInt(0L)))),
              Set(
                variableGrouping("  n@1", "  n@5"),
                variableGrouping("  r@2", "  r@6"),
                variableGrouping("  m@3", "  m@7")
              )
            ),
            nodePat(Some("b"))
          ),
          where = Some(where(ands(
            anyInList(varFor("  a@4"), prop("  a@0", "list"), greaterThan(varFor("  a@4"), literalInt(0L))),
            unique(varFor("  r@6"))
          )))
        ),
        returnLit((1, "s"))
      )
    )
  }

  test("Scoped expression predicate with ambiguous name that would cause a conflict if not disambiguated") {
    assertRewritten(
      """MATCH (a) ((n)-[r]->(m) WHERE any(a IN n.list WHERE n > a))+ (b)
        |RETURN 1 AS s""".stripMargin,
      singleQuery(
        match_(
          pathConcatenation(
            nodePat(Some("  a@0")),
            quantifiedPath(
              relationshipChain(
                nodePat(Some("  n@1")),
                relPat(Some("  r@2")),
                nodePat(Some("  m@3"))
              ),
              PlusQuantifier()(pos),
              Some(anyInList(varFor("  a@4"), prop("  n@1", "list"), greaterThan(varFor("  n@1"), varFor("  a@4")))),
              Set(
                variableGrouping("  n@1", "  n@5"),
                variableGrouping("  r@2", "  r@6"),
                variableGrouping("  m@3", "  m@7")
              )
            ),
            nodePat(Some("b"))
          ),
          where = Some(where(ands(
            anyInList(varFor("  a@4"), prop("  a@0", "list"), greaterThan(varFor("  a@0"), varFor("  a@4"))),
            unique(varFor("  r@6"))
          )))
        ),
        returnLit((1, "s"))
      )
    )
  }

  test("Pattern-expression scoped expression") {
    assertNotRewritten(
      """MATCH (a) ((n)-[r]->(m) WHERE (n)-->())+ (b)
        |RETURN 1 AS s""".stripMargin
    )
  }

  test("Pattern-comprehension scoped expression") {
    assertRewritten(
      """MATCH (a) ((n)-[r]->(m) WHERE [(n)-[rr]->(c) | n.a] IS NULL)+ (b)
        |RETURN 1 AS s""".stripMargin,
      singleQuery(
        match_(
          pathConcatenation(
            nodePat(Some("a")),
            quantifiedPath(
              relationshipChain(
                nodePat(Some("  n@0")),
                relPat(Some("  r@1")),
                nodePat(Some("  m@2"))
              ),
              PlusQuantifier()(pos),
              Some(isNull(CollectExpression(singleQuery(
                match_(relationshipChain(nodePat(Some("  n@0")), relPat(Some("rr")), nodePat(Some("c")))),
                return_(aliasedReturnItem(prop("  n@0", "a"), "  UNNAMED0"))
              ))(pos, null, null))),
              Set(
                variableGrouping("  n@0", "  n@3"),
                variableGrouping("  r@1", "  r@4"),
                variableGrouping("  m@2", "  m@5")
              )
            ),
            nodePat(Some("b"))
          ),
          where = Some(where(unique(varFor("  r@4"))))
        ),
        returnLit((1, "s"))
      )
    )
  }

  test("Pattern-comprehension scoped expression predicate with nested subquery") {
    assertNotRewritten(
      """MATCH (a) ((n)-[r]->(m) WHERE size([(n)-->() WHERE EXISTS { (n) } | n.a]) = 0)+ (b)
        |RETURN *""".stripMargin
    )
  }

  test("Full subquery scope expression predicate") {
    assertNotRewritten(
      """MATCH (a) ((n)-[r]->(m) WHERE EXISTS { MATCH (n) WITH n RETURN n AS n })+ (b)
        |RETURN *""".stripMargin
    )
  }

  test("QPP with lower-bound 0 and predicates") {
    assertNotRewritten(
      """MATCH (a) ((n:N)-[r]->(m:M))* (b)
        |RETURN *""".stripMargin
    )
  }

  test("Predicate with both inner node references") {
    assertNotRewritten(
      """MATCH (a) ((n)-[r]->(m) WHERE n.p > m.p)+ (b)
        |RETURN *""".stripMargin
    )
  }

  test("Predicate with reference to previous MATCH clause") {
    assertRewritten(
      """MATCH (pre) MATCH (a) ((n)-[r]->(m) WHERE n.p > pre.p)+ (b)
        |RETURN *""".stripMargin,
      """MATCH (pre)
        |MATCH (a) ((n)-[r]->(m) WHERE n.p > pre.p)+ (b)
        |WHERE a.p > pre.p
        |RETURN *""".stripMargin
    )
  }
}
