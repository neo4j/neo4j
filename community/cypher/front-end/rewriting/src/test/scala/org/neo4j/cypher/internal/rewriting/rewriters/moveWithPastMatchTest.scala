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

import org.neo4j.cypher.internal.rewriting.RewriteTest
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class moveWithPastMatchTest extends CypherFunSuite with RewriteTest {

  val rewriterUnderTest: Rewriter = moveWithPastMatch(CancellationChecker.neverCancelled()).instance

  test("does not move WITH if it is not a simple projection") {
    assertIsNotRewritten("WITH DISTINCT 1 AS foo MATCH (n) RETURN n")
    assertIsNotRewritten("WITH count(*) AS foo MATCH (n) RETURN n")
    assertIsNotRewritten("WITH 1 AS foo SKIP 1 MATCH (n) RETURN n")
    assertIsNotRewritten("WITH 1 AS foo LIMIT 1 MATCH (n) RETURN n")
    assertIsNotRewritten("WITH 1 AS foo ORDER BY foo MATCH (n) RETURN n")
    assertIsNotRewritten("WITH 1 AS foo WHERE foo > 0 MATCH (n) RETURN n")
    assertIsNotRewritten("WITH *, 1 AS foo MATCH (n) RETURN n")
    assertIsNotRewritten("WITH randomUUID() AS uuid MATCH (n:Node) RETURN n.index, uuid")
    assertIsNotRewritten("WITH count{ RETURN 0 AS x } AS subqueryExpr MATCH (n:Node) RETURN n.index, uuid")
    assertIsNotRewritten("WITH collect{ RETURN 0 AS x } AS subqueryExpr MATCH (n:Node) RETURN n.index, uuid")
    assertIsNotRewritten("WITH exists{ RETURN 0 AS x } AS subqueryExpr MATCH (n:Node) RETURN n.index, uuid")
  }

  test("does not move WITH if MATCH uses projected variable") {
    assertIsNotRewritten("WITH 1 AS foo MATCH (n) WHERE n.prop = foo RETURN n")
    assertIsNotRewritten("WITH m AS m MATCH (n)--(m) RETURN n, m")
  }

  test("does not move WITH if the projected variable is just an alias") {
    assertIsNotRewritten("MATCH (n), (x) WITH n AS n MATCH (x) RETURN x")
  }

  test("does not move WITH if the projected variables contain an alias") {
    assertIsNotRewritten("MATCH (n), (x) WITH n AS n, 1 AS foo MATCH (x) RETURN x")
  }

  // In the first case moving would be possible.
  // In the second case, we would have to do something about namespacing, since there are two different x's in the query.
  // To avoid that in general, we only move a WITH clause if it came from the beginning of the query, but we move it
  // as far back as we can.
  test("does not move WITH if not at the beginning of a query in general") {
    assertIsNotRewritten("MATCH (x) WITH 1 AS foo MATCH (n) RETURN n")
    assertIsNotRewritten("MATCH (x) WITH 1 AS foo MATCH (x) RETURN x")
  }

  test("moves single WITH past OPTIONAL MATCH") {
    assertRewrite("WITH 1 AS foo OPTIONAL MATCH (n) RETURN n", "OPTIONAL MATCH (n) WITH 1 AS foo, n AS n RETURN n")
  }

  test("moves single WITH past MATCH with multiple OPTIONAL MATCHes") {
    assertRewrite(
      "WITH 1 AS foo MATCH (n) OPTIONAL MATCH (n)--(q) OPTIONAL MATCH (n)--(m) RETURN n",
      "MATCH (n) OPTIONAL MATCH (n)--(q) OPTIONAL MATCH (n)--(m) WITH 1 AS foo, n AS n, q AS q, m AS m RETURN n"
    )
  }

  test("moves single WITH past OPTIONAL MATCH, but not after subsequent MATCH") {
    assertRewrite(
      "WITH 1 AS foo OPTIONAL MATCH (n) WITH n MATCH (m) RETURN n",
      "OPTIONAL MATCH (n) WITH 1 AS foo, n AS n WITH n MATCH (m) RETURN n"
    )
  }

  test("does not move WITH between update clause and MATCH") {
    assertIsNotRewritten("MERGE (n) WITH 1 AS foo MATCH (m) RETURN m")
  }

  test("moves single WITH past MATCH") {
    assertRewrite(
      "WITH 1 AS foo MATCH (a)-[r1]->(b)-[r2*0..1]->(c) RETURN *",
      "MATCH (a)-[r1]->(b)-[r2*0..1]->(c) WITH 1 AS foo, a AS a, b AS b, r1 AS r1, c AS c, r2 AS r2 RETURN *"
    )
  }

  test("moves single WITH past multiple MATCHes") {
    assertRewrite(
      "WITH 1 AS foo MATCH (n) MATCH (n)--(q) MATCH (n)--(m) RETURN n",
      "MATCH (n) MATCH (n)--(q) MATCH (n)--(m) WITH 1 AS foo, n AS n, q AS q, m AS m RETURN n"
    )
  }

  test("moves single WITH past MATCH, WHERE") {
    assertRewrite(
      "WITH 1 AS foo MATCH (a)-[r1]->(b)-[r2*0..1]->(c) WHERE a.prop = 2 RETURN *",
      "MATCH (a)-[r1]->(b)-[r2*0..1]->(c) WHERE a.prop = 2 WITH 1 AS foo, a AS a, b AS b, r1 AS r1, c AS c, r2 AS r2 RETURN *"
    )
  }

  test("moves two WITHs past MATCH") {
    assertRewrite(
      "WITH 1 AS foo WITH 2 AS bar, foo AS foo MATCH (a) RETURN *",
      "MATCH (a) WITH 1 AS foo, a AS a WITH 2 AS bar, foo AS foo, a AS a RETURN *"
    )
  }

  test("does not rewrite initial WITH in UNION subquery") {
    assertIsNotRewritten(
      """WITH 1 AS q
        |CALL {
        |  WITH q
        |  MATCH (a:A)
        |  RETURN a AS a
        |  UNION
        |  WITH q
        |  MATCH (a:B)
        |  RETURN b AS a
        |}
        |RETURN a AS a""".stripMargin
    )
  }

  test("rewrites other WITH in UNION subquery") {
    assertRewrite(
      """WITH 1 AS q
        |CALL {
        |  WITH q
        |  WITH 1 AS foo
        |  MATCH (a:A)
        |  RETURN a AS a
        |  UNION
        |  WITH q
        |  WITH 2 AS bar
        |  MATCH (a:B)
        |  RETURN b AS a
        |}
        |RETURN a AS a""".stripMargin,
      """WITH 1 AS q
        |CALL {
        |  WITH q
        |  MATCH (a:A)
        |  WITH 1 AS foo, a AS a
        |  RETURN a AS a
        |  UNION
        |  WITH q
        |  MATCH (a:B)
        |  WITH 2 AS bar, a AS a
        |  RETURN b AS a
        |}
        |RETURN a AS a""".stripMargin
    )
  }

  test("does not rewrite initial WITH in single subquery") {
    assertIsNotRewritten(
      """WITH 1 AS q
        |CALL {
        |  WITH q
        |  MATCH (a:B)
        |  RETURN b AS a
        |}
        |RETURN a AS a""".stripMargin
    )
  }

  test("rewrites other WITH in single subquery") {
    assertRewrite(
      """WITH 1 AS q
        |CALL {
        |  WITH q
        |  WITH 1 AS foo
        |  MATCH (a:B)
        |  RETURN b AS a
        |}
        |RETURN a AS a""".stripMargin,
      """WITH 1 AS q
        |CALL {
        |  WITH q
        |  MATCH (a:B)
        |  WITH 1 AS foo, a AS a
        |  RETURN b AS a
        |}
        |RETURN a AS a""".stripMargin
    )
  }

  test("does not move WITH if variables from MATCH exist in previous scope") {
    assertIsNotRewritten(
      """WITH 1 AS n
        |WITH n AS m
        |MATCH (n)
        |RETURN *
        |""".stripMargin
    )
    assertIsNotRewritten(
      """MATCH (n)
        |CALL {
        |  WITH n
        |  WITH n AS m
        |  MATCH (n)
        |  RETURN n AS x
        |}
        |RETURN *
        |""".stripMargin
    )
    assertIsNotRewritten(
      """WITH 1 AS n
        |WITH 2 AS m
        |MATCH (n)
        |RETURN *
        |""".stripMargin
    )
    assertIsNotRewritten(
      """MATCH (n)
        |CALL {
        |  WITH n
        |  WITH 1 AS m
        |  MATCH (n)
        |  RETURN n AS x
        |}
        |RETURN *
        |""".stripMargin
    )
  }

  test("moves WITH only while variables from MATCH do not exist in previous scope") {
    assertRewrite(
      """WITH 1 AS n
        |WITH n AS m
        |WITH m AS t
        |WITH t AS u
        |MATCH (n)
        |RETURN *
        |""".stripMargin,
      """WITH 1 AS n
        |WITH n AS m
        |MATCH (n)
        |WITH m AS t, n AS n
        |WITH t AS u, n AS n
        |RETURN *
        |""".stripMargin
    )
    assertRewrite(
      """MATCH (n)
        |CALL {
        |  WITH n
        |  WITH n AS m
        |  WITH m AS t
        |  WITH t AS u
        |  MATCH (n)
        |  RETURN n AS x
        |}
        |RETURN *
        |""".stripMargin,
      """MATCH (n)
        |CALL {
        |  WITH n
        |  WITH n AS m
        |  MATCH (n)
        |  WITH m AS t, n AS n
        |  WITH t AS u, n AS n
        |  RETURN n AS x
        |}
        |RETURN *
        |""".stripMargin
    )
    assertRewrite(
      """WITH 1 AS n
        |WITH 2 AS m
        |WITH 3 AS t
        |WITH 4 AS u
        |MATCH (n)
        |RETURN *
        |""".stripMargin,
      """WITH 1 AS n
        |WITH 2 AS m
        |MATCH (n)
        |WITH 3 AS t, n AS n
        |WITH 4 AS u, n AS n
        |RETURN *
        |""".stripMargin
    )
    assertRewrite(
      """MATCH (n)
        |CALL {
        |  WITH n
        |  WITH 1 AS m
        |  WITH 2 AS t
        |  WITH 3 AS u
        |  MATCH (n)
        |  RETURN n AS x
        |}
        |RETURN *
        |""".stripMargin,
      """MATCH (n)
        |CALL {
        |  WITH n
        |  WITH 1 AS m
        |  MATCH (n)
        |  WITH 2 AS t, n AS n
        |  WITH 3 AS u, n AS n
        |  RETURN n AS x
        |}
        |RETURN *
        |""".stripMargin
    )
  }

  test("Variables inside of FullSubqueryExpressions should not be pulled out into the moved WITH") {
    List("COUNT", "COLLECT", "EXISTS").foreach(subqueryExpression => {
      assertRewrite(
        s"""WITH 1 AS n
           |MATCH ({m:$subqueryExpression { RETURN 0 AS x } })
           |RETURN *
           |""".stripMargin,
        s"""
           |MATCH ({m:$subqueryExpression { RETURN 0 AS x } })
           |WITH 1 AS n
           |RETURN *
           |""".stripMargin
      )
    })
  }

  test("Variables inside List Comprehension should not be moved past WITH") {
    assertRewrite(
      """WITH 0 AS n0
        |MATCH (n {n1:[x IN [] | 0]})
        |RETURN 0
        |""".stripMargin,
      """
        |MATCH (n {n1:[x IN [] | 0]})
        |WITH 0 AS n0, n AS n
        |RETURN 0
        |""".stripMargin
    )
  }

  test("Variables inside List Comprehension inside functions should not be moved past WITH") {
    assertRewrite(
      """WITH 0 AS n0
        |MATCH ()-[t:T2{k:size([x IN [NULL]|NULL])}]-()
        |RETURN 1 AS a2
        |""".stripMargin,
      """
        |MATCH ()-[t:T2 {k: size([x IN [NULL] | NULL])}]-()
        |WITH 0 AS n0, t AS t
        |RETURN 1 AS a2
        |""".stripMargin
    )
  }

  test("Variables inside Reduce function should not be moved past WITH") {
    assertRewrite(
      """WITH 0 AS n0
        |MATCH (n { n1 : reduce(x = 0, a IN [1, 2] | x + a)})
        |RETURN 1 AS a2
        |""".stripMargin,
      """
        |MATCH (n { n1 : reduce(x = 0, a IN [1, 2] | x + a)})
        |WITH 0 AS n0, n AS n
        |RETURN 1 AS a2
        |""".stripMargin
    )
  }

  test("Variables inside scoped expressions should not be moved past WITH") {
    List("all", "any", "none", "single").foreach(scopedExpression => {
      assertRewrite(
        s"""WITH 0 AS n0
           |MATCH (n { n1 : $scopedExpression(x IN [1] WHERE x < 60)} )
           |RETURN 1 AS a2
           |""".stripMargin,
        s"""
           |MATCH (n { n1 : $scopedExpression(x IN [1] WHERE x < 60)} )
           |WITH 0 AS n0, n AS n
           |RETURN 1 AS a2
           |""".stripMargin
      )
    })
  }
}
