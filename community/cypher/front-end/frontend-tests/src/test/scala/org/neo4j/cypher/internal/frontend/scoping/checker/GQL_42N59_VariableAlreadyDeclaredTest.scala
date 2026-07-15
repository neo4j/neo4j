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
package org.neo4j.cypher.internal.frontend.scoping.checker

import org.neo4j.cypher.internal.frontend.scoping.E42N59
import org.neo4j.cypher.internal.frontend.scoping.Outcome
import org.neo4j.cypher.internal.frontend.scoping.Passes
import org.neo4j.cypher.internal.frontend.scoping.Versioned.ignoreBeforeCypher25
import org.neo4j.cypher.internal.frontend.scoping.checker.CompositionRestriction.NoLocalCallableBody

/**
 * Test for 42N59 - Variable Already Declared
 */
class GQL_42N59_VariableAlreadyDeclaredTest extends VariableCheckingWithLocalCallablesTestSuite {
  VariableCheckingWithLocalCallablesTestSuite.register(() => testCases())

  override def testCases(): Seq[TestQuery] = Seq(
    // Negative tests
    TestQuery(
      """LET a = 10
        |LET a = [1, a, 3]
        |RETURN a""".stripMargin,
      ignoreBeforeCypher25(E42N59("a")),
      Seq("a")
    ),
    TestQuery(
      """LET a = 10
        |UNWIND [1, a, 3] AS a
        |RETURN a""".stripMargin,
      ignoreBeforeCypher25(E42N59("a")),
      Seq("a")
    ),
    TestQuery(
      """RETURN 1 AS a
        |
        |NEXT
        |
        |LET a = 2
        |RETURN a + 1 AS b""".stripMargin,
      ignoreBeforeCypher25(E42N59("a")),
      Seq("b")
    ),
    TestQuery(
      """LET a = 10
        |CALL {
        |  WITH a
        |  LET a = a
        |  RETURN a AS y
        |}
        |RETURN *""".stripMargin,
      ignoreBeforeCypher25(E42N59("a")),
      Seq("a", "y")
    ),
    TestQuery(
      """LET a = 10
        |CALL {
        |  WITH a
        |  LET a = a + 0
        |  RETURN a AS y
        |}
        |RETURN *""".stripMargin,
      ignoreBeforeCypher25(E42N59("a")),
      Seq("a", "y")
    ),
    TestQuery(
      """LET a = 10
        |UNWIND [1, 2, 3] AS b
        |CALL (a) {
        |  UNWIND [1, 2, 3] AS x
        |  RETURN a * x AS x
        |} IN TRANSACTIONS REPORT STATUS AS b
        |RETURN a, x""".stripMargin,
      ignoreBeforeCypher25(E42N59("b")),
      Seq("a", "x")
    ),
    TestQuery(
      """LET a = 10
        |UNWIND [1, 2, 3] AS b
        |CALL {
        |  WITH a
        |  UNWIND [1, 2, 3] AS x
        |  RETURN a * x AS x
        |} IN TRANSACTIONS REPORT STATUS AS b
        |RETURN a, x""".stripMargin,
      ignoreBeforeCypher25(E42N59("b")),
      Seq("a", "x")
    ),
    TestQuery(
      """CALL test.my.proc(null) YIELD a, b AS a
        |RETURN a""".stripMargin,
      E42N59("a"),
      Seq("n")
    ),
    TestQuery(
      """CALL test.my.proc(null) YIELD a AS c, b AS c
        |RETURN c""".stripMargin,
      E42N59("c"),
      Seq("n")
    ),
    TestQuery(
      """LET query = "bob"
        |CALL db.index.fulltext.queryNodes("myIndex", query) YIELD node AS n, score
        |LET n = 1
        |RETURN n ORDER BY score ASCENDING LIMIT 3""".stripMargin,
      ignoreBeforeCypher25(E42N59("n")),
      Seq("n")
    ),
    TestQuery(
      """MATCH (p)-[]-(x)
        |MATCH p = (x)-[]-(y), x = (p)
        |RETURN p, x""".stripMargin,
      E42N59("p"),
      Seq("p", "x")
    ),
    TestQuery(
      """MATCH p = (p)--() RETURN p""".stripMargin,
      E42N59("p"),
      Seq("p")
    ),
    TestQuery(
      """MATCH ((a)-[r]->(b))+, (b)-[c]->(d)
        |RETURN *""".stripMargin,
      E42N59("b"),
      Seq("a", "r", "b", "c", "d")
    ),
    TestQuery(
      """MATCH ((a)--(b))+, (a WHERE size([1,2]) = 2)
        |RETURN *""".stripMargin,
      E42N59("a"),
      Seq("a", "b")
    ),
    TestQuery(
      """MATCH ((a)--(b))+, (a) WHERE size([1,2]) = 2
        |RETURN *""".stripMargin,
      E42N59("a"),
      Seq("a", "b")
    ),
    TestQuery(
      """MATCH ((a)--(b))+, (a:$(a[0].name) {prop:size(a)} WHERE size(a) = 1)
        |RETURN *""".stripMargin,
      E42N59("a"),
      Seq("a", "b")
    ),
    TestQuery(
      """MATCH ((a)-[r]-(b))+, ()-[r:$(r[0].name) {prop:size(r)} WHERE size(r) = 1]->()
        |RETURN *""".stripMargin,
      E42N59("r"),
      Seq("a", "r", "b")
    ),
    TestQuery(
      """MATCH ((a)-[b]->(c))*(d), (f)-[e]->(a)
        |RETURN *""".stripMargin,
      E42N59("a"),
      Seq("a", "b", "c", "d", "f", "e")
    ),
    TestQuery(
      """MATCH ((a)-[b]->(c))* (d)-[e]->() ((a)-[f]->(g)){2,}
        |RETURN *""".stripMargin,
      E42N59("a"),
      Seq("a", "b", "c", "d", "e", "f", "g")
    ),
    TestQuery(
      """MATCH (a)-->(b)
        |MATCH (x)--(y)((a)-->(t)){1,5}()-->(z)
        |RETURN *""".stripMargin,
      E42N59("a"),
      Seq("a", "b", "x", "y", "t", "z")
    ),
    TestQuery(
      """CREATE (n), (n)""".stripMargin,
      E42N59("n"),
      Seq.empty
    ),
    TestQuery(
      """CREATE ()-[r]->(), ()-[r]->()""".stripMargin,
      E42N59("r"),
      Seq.empty
    ),
    TestQuery(
      """MATCH ()-[r]->()
        |CREATE ()-[r]->()""".stripMargin,
      E42N59("r"),
      Seq.empty
    ),
    TestQuery(
      """MATCH ()-[r:R]->()
        |CREATE ()-[:R]->()-[r]->()""".stripMargin,
      E42N59("r"),
      Seq.empty
    ),
    TestQuery(
      """MATCH (n)
        |CREATE (n)""".stripMargin,
      E42N59("n"),
      Seq.empty
    ),
    TestQuery(
      """MATCH (n)
        |MERGE (n {p:n.p})""".stripMargin,
      E42N59("n"),
      Seq.empty
    ),
    TestQuery(
      """WITH 0.5 AS score
        |MATCH (movie:Movie)
        |  SEARCH movie IN (
        |    VECTOR INDEX moviePlots
        |    FOR [1, 2, 3]
        |    LIMIT 5
        |  ) SCORE AS score
        |RETURN movie.title AS title, score""".stripMargin,
      ignoreBeforeCypher25(E42N59("score")),
      Seq("title", "score")
    ),
    TestQuery(
      """SHOW TRANSACTION
        |TERMINATE TRANSACTION 'neo4j-transaction-3'
        |YIELD transactionId
        |RETURN transactionId""".stripMargin,
      ignoreBeforeCypher25(E42N59("transactionId")),
      Seq("transactionId"),
      compositionRestriction = NoLocalCallableBody
    )
  ) ++ (
    // Positive tests
    for {
      (first, outcomeMod) <- Seq[(String, Outcome => Outcome)](
        "LET a = 10" -> ignoreBeforeCypher25,
        "WITH 10 AS a" -> identity
      )
      (call, callIsImporting) <- Seq("CALL (a)" -> true, "CALL (*)" -> true, "CALL " -> false)
      importingWith <- Seq("// no importing WITH", "WITH a", "WITH a AS a", "WITH *")
      if callIsImporting || !importingWith.startsWith("//")
    } yield {
      TestQuery(
        s"""$first
           |$call {
           |  $importingWith
           |  RETURN a AS y
           |}
           |RETURN *""".stripMargin,
        outcomeMod(Passes),
        Seq("a", "y")
      )
    }
  ) ++ Seq(
    TestQuery(
      """MATCH (a:A)
        |CALL (a) {
        |  MATCH (a)-->(b:B)
        |  FILTER a.x = b.x
        |  RETURN COUNT(b) AS cntB
        |}
        |RETURN a, cntB""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("a", "cntB")
    ),
    TestQuery(
      """MATCH (a:A)-[r:REL]->(b:B)
        |WITH a AS b, b AS tmp, r AS r
        |WITH b AS a, r
        |LIMIT 1
        |MATCH (a)-[r]->(b)
        |RETURN a, r, b""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("a", "r", "b")
    ),
    TestQuery(
      """LET n = 1
        |LET x = COUNT {
        |  LET a = 1
        |  CALL (a) {
        |    LET n = 3
        |    RETURN a + n AS result
        |  }
        |  RETURN result
        |}
        |RETURN *""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("n", "x", "result")
    ),
    TestQuery(
      """LET query = "bob"
        |CALL db.index.fulltext.queryNodes("myIndex", query) YIELD node AS n, score
        |LET node = 1
        |RETURN node ORDER BY score ASCENDING LIMIT 3""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("node")
    ),
    TestQuery(
      """UNWIND range(1, 10) AS i
        |CALL {
        |  WITH i
        |  UNWIND [1, 2] AS j
        |  CREATE (n:N {i: i, j: j})
        |} IN TRANSACTIONS
        |  OF 10 ROWS
        |  ON ERROR FAIL""".stripMargin,
      Passes,
      Seq.empty
    ),
    TestQuery(
      """MATCH ((a)-[e]->(b)-[f]->(a))+(p)-[g]->(r)-[q]->(s)
        |RETURN a, b, p, r, s""".stripMargin,
      Passes,
      Seq("a", "b", "p", "r", "s")
    ),
    TestQuery(
      """MATCH (a:A) MATCH p = ( ()--() WHERE EXISTS { (a) } )+
        |RETURN length(p) AS l""".stripMargin,
      Passes,
      Seq("l")
    ),
    TestQuery(
      """MATCH (a)-->(b)
        |MATCH (x)--(y)((s)-->(t WHERE a.p = 1)){1,5}()-->(z)
        |RETURN *""".stripMargin,
      Passes,
      Seq("a", "b", "x", "y", "s", "t", "z")
    ),
    TestQuery(
      """MATCH (a)-[* {propA: a.prop, propB: b.prop}]-(b) RETURN a, b""".stripMargin,
      Passes,
      Seq("a", "b")
    ),
    TestQuery(
      """MATCH (a)-[*..4 {propA: a.prop, propB: b.prop}]-(b) RETURN a, b""".stripMargin,
      Passes,
      Seq("a", "b")
    ),
    TestQuery(
      """MATCH ((a)--(b))+, (x WHERE size(a) = 2)
        |RETURN *""".stripMargin,
      Passes,
      Seq("a", "b", "x")
    ),
    TestQuery(
      """MATCH ((a)--(b))+, (x) WHERE size(a) = 2
        |RETURN *""".stripMargin,
      Passes,
      Seq("a", "b", "x")
    ),
    TestQuery(
      """MATCH ((a)--(b))+, (n:$(a[0].name))
        |RETURN *""".stripMargin,
      Passes,
      Seq("a", "b", "n")
    ),
    TestQuery(
      """MATCH ((a)--(b))+, (n {prop:size(a)})
        |RETURN *""".stripMargin,
      Passes,
      Seq("a", "b", "n")
    ),
    TestQuery(
      """MATCH ((a)--(b))+, (n:$(a[0].name) {prop:size(a)} WHERE size(a) = 1)
        |RETURN *""".stripMargin,
      Passes,
      Seq("a", "b", "n")
    ),
    TestQuery(
      """MATCH ((a)-[r]-(b))+, ()-[x:$(r[0].name) {prop:size(r)} WHERE size(r) = 1]->()
        |RETURN *""".stripMargin,
      Passes,
      Seq("a", "r", "b", "x")
    ),
    TestQuery(
      """RETURN 7 AS x
        |
        |NEXT
        |
        |WHEN true THEN  {
        |  {
        |    RETURN 5 AS y
        |
        |    NEXT
        |
        |    RETURN x + y + 1 AS x
        |  }
        |  UNION
        |  RETURN x + 2 AS x
        |}""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("x")
    ),
    TestQuery(
      """RETURN 7 AS x
        |
        |NEXT
        |
        |WHEN true THEN  {
        |  RETURN x + 2 AS x
        |  UNION
        |  {
        |    RETURN 5 AS y
        |
        |    NEXT
        |
        |    RETURN x + y + 1 AS x
        |  }
        |}""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("x")
    ),
    TestQuery(
      """MATCH (n), (n)
        |RETURN n""".stripMargin,
      Passes,
      Seq("n")
    ),
    TestQuery(
      """CREATE ()-[r:R]->()""".stripMargin,
      Passes,
      Seq.empty
    ),
    TestQuery(
      """CREATE (n)-[r:R]->(n)""".stripMargin,
      Passes,
      Seq.empty
    ),
    TestQuery(
      """CREATE (n), (n)-[:R]->(n)""".stripMargin,
      Passes,
      Seq.empty
    ),
    TestQuery(
      """MATCH (n)
        |CREATE (n)-[r:R]->(n)""".stripMargin,
      Passes,
      Seq.empty
    ),
    TestQuery(
      """MATCH (n)
        |CREATE ({p:n.p})""".stripMargin,
      Passes,
      Seq.empty
    ),
    TestQuery(
      """MATCH (n)
        |MERGE ({p:n.p})""".stripMargin,
      Passes,
      Seq.empty
    ),
    TestQuery(
      """FOREACH (x IN [1] | FOREACH(x IN [1] | CREATE() ))""".stripMargin,
      Passes,
      Seq.empty
    )
  )
}
